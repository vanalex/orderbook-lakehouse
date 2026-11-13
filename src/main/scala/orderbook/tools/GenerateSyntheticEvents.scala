package orderbook.tools

import orderbook.common.OrderBookSchema

import org.apache.spark.sql.Row

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** Synthetic demo-data source for `orderbook.bronze.raw_events`
  * (data_pipeline_plan.md, Phase 3 "source for demo data").
  *
  * Simulates a small matching engine per instrument, so the feed is internally
  * consistent and the book gold rebuilds from it is never crossed:
  *
  *  - `add`: a limit order priced around a random-walking mid. If it crosses
  *    the opposite side (best ask for a buy, best bid for a sell) it first
  *    trades against those resting orders, best price first. Each fill is a
  *    `trade` event at the resting order's side and price. Only the unfilled
  *    remainder rests, and it's emitted as the `add`.
  *  - `trade`: a market order hitting the best bid or best ask.
  *  - `cancel`: removes a random resting order in full.
  *  - `snapshot`: a marker row with no side, price or qty.
  *
  * The generator doesn't emit `modify`. Without an `order_id` gold can't
  * apply a modify's new absolute qty (see `OrderBookSchema.topOfBookSnapshots`),
  * so a modify followed by a cancel would leave "ghost" qty at that level.
  *
  * Events are `intervalMs` apart (default 1s), so 5000 events cover about
  * 80 minutes: enough one-minute bars for every analytics query.
  *
  * This job is a producer, not a Spark job: it POSTs the events as
  * newline-delimited JSON to Redpanda Connect (`infra/redpanda-connect/landing.yaml`),
  * which lands them as files under `s3://orderbook-warehouse/landing/raw_events/`.
  * `IngestRawEvents` then appends those files to `orderbook.bronze.raw_events`.
  *
  * The feed is one continuous, reproducible simulation (fixed seed). A run
  * sends a slice of it: `count` events starting after the first `from`.
  * Consecutive slices continue the same `seq_no` sequence, timeline and
  * order book, so sending the next slice adds genuinely new events that
  * line up with gold's carried-over `book_state`, and re-sending a slice
  * sends exact duplicates (`make generate-next` / `make generate-replay`).
  *
  * Run with: sbt "runMain orderbook.tools.GenerateSyntheticEvents [count] [instruments] [url] [intervalMs] [from]"
  * e.g.:     sbt "runMain orderbook.tools.GenerateSyntheticEvents 5000 BTC-USD,ETH-USD http://localhost:4195/events 1000 5000"
  *
  * `url` defaults to the `STREAM_URL` env var, else `http://localhost:4195/events`.
  */
object GenerateSyntheticEvents {

  /** Events per POST. Matches the landing pipeline's batch size, so each
    * request becomes one landed file and returns as soon as it's written.
    */
  val RequestSize: Int = 5000

  private case class RestingOrder(side: String, price: Double, var qty: Double)

  /** Per-instrument book: resting orders on each side plus the mid price. */
  private final class Book(var mid: Double) {
    val bids: ArrayBuffer[RestingOrder] = ArrayBuffer.empty
    val asks: ArrayBuffer[RestingOrder] = ArrayBuffer.empty

    def side(s: String): ArrayBuffer[RestingOrder] = if (s == "buy") bids else asks

    def bestBid: Option[RestingOrder] = bids.maxByOption(_.price)
    def bestAsk: Option[RestingOrder] = asks.minByOption(_.price)

    def size: Int = bids.length + asks.length
  }

  private def round(p: Double): Double = math.round(p * 100) / 100.0

  /** Pure event generation, no Spark/IO — kept separate so it's unit-testable.
    *
    * Returns events `from + 1` to `from + count` of the continuous feed for
    * `seed`: the simulation runs from the start and the first `from` events
    * are dropped, so a slice always continues exactly where the previous one
    * stopped (same book, `seq_no` and timestamps).
    */
  def generate(
      count: Int,
      instruments: Seq[String],
      seed: Long = 42L,
      intervalMs: Long = 1000L,
      from: Int = 0
  ): Seq[Row] = {
    require(from >= 0, s"from must be >= 0, got $from")
    val rnd = new Random(seed)
    // Fixed epoch anchor (not wall-clock) so runs are reproducible for a given seed.
    val startMillis = 1700000000000L
    val books       = instruments.map(_ -> new Book(100.0)).toMap

    val out   = ArrayBuffer.empty[Row]
    var seqNo     = 0L
    var step      = 0L
    var stepStart = 0

    // Events produced by the same step (an aggressive add and its fills) get
    // consecutive milliseconds so their order is unambiguous.
    def emit(eventType: String, instrument: String, side: String, price: java.lang.Double, qty: java.lang.Double): Unit = {
      val offsetMs = out.length - stepStart
      seqNo += 1
      out += Row(eventType, instrument, new Timestamp(startMillis + step * intervalMs + offsetMs), side, price, qty, seqNo)
    }

    /** Fills `qty` of an incoming `side` order against the opposite side,
      * best price first, while `crosses(restingPrice)` holds. Returns the
      * unfilled remainder.
      */
    def matchAgainst(instrument: String, book: Book, side: String, qty: Double, crosses: Double => Boolean): Double = {
      val opposite = book.side(if (side == "buy") "sell" else "buy")
      var left     = qty
      var best     = if (side == "buy") book.bestAsk else book.bestBid
      while (left > 0 && best.exists(o => crosses(o.price))) {
        val o      = best.get
        val filled = math.min(left, o.qty)
        emit(OrderBookSchema.EventType.Trade, instrument, o.side, o.price, filled)
        o.qty -= filled
        left -= filled
        if (o.qty <= 0) opposite -= o
        best = if (side == "buy") book.bestAsk else book.bestBid
      }
      left
    }

    while (out.length < from + count) {
      stepStart = out.length
      val instrument = instruments(rnd.nextInt(instruments.length))
      val book       = books(instrument)
      book.mid = round(book.mid + rnd.nextGaussian() * 0.05)

      val roll = rnd.nextDouble()
      if (book.size < 10 || roll < 0.55) {
        // Limit order. ~15% are priced aggressively through the mid.
        val side       = if (rnd.nextBoolean()) "buy" else "sell"
        val dir        = if (side == "buy") -1.0 else 1.0
        val aggressive = rnd.nextDouble() < 0.15
        val offset     = if (aggressive) -(0.05 + rnd.nextDouble() * 0.3) else 0.01 + rnd.nextDouble() * 0.5
        val price      = round(book.mid + dir * offset)
        val qty        = (1 + rnd.nextInt(100)).toDouble

        val crosses: Double => Boolean = if (side == "buy") _ <= price else _ >= price
        val left = matchAgainst(instrument, book, side, qty, crosses)
        if (left > 0) {
          book.side(side) += RestingOrder(side, price, left)
          emit(OrderBookSchema.EventType.Add, instrument, side, price, left)
        }
      } else if (roll < 0.85) {
        val all   = book.bids ++ book.asks
        val order = all(rnd.nextInt(all.length))
        book.side(order.side) -= order
        emit(OrderBookSchema.EventType.Cancel, instrument, order.side, order.price, order.qty)
      } else if (roll < 0.97) {
        // Market order: hits the best bid (sell) or lifts the best ask (buy).
        val side = if (rnd.nextBoolean()) "buy" else "sell"
        val qty  = (1 + rnd.nextInt(60)).toDouble
        matchAgainst(instrument, book, side, qty, _ => true)
      } else {
        emit(OrderBookSchema.EventType.Snapshot, instrument, null, null, null)
      }

      // Keep the mid inside the spread so new passive orders stay sensible.
      for (b <- book.bestBid; a <- book.bestAsk) book.mid = round((b.price + a.price) / 2)

      step += 1
    }

    out.slice(from, from + count).toSeq
  }

  /** One event as a JSON line, in the raw-feed shape `IngestRawEvents`
    * reads. Null fields (a snapshot's side/price/qty) are left out, and the
    * timestamp is ISO-8601 UTC, so the feed doesn't depend on the producer's
    * timezone.
    */
  def toJsonLine(row: Row): String = {
    val fields = OrderBookSchema.bronzeRawEvents.fieldNames.toSeq.zipWithIndex.collect {
      case (name, i) if !row.isNullAt(i) =>
        name -> (row.get(i) match {
          case ts: Timestamp => ujson.Str(ts.toInstant.toString)
          case s: String     => ujson.Str(s)
          case d: Double     => ujson.Num(d)
          case l: Long       => ujson.Num(l.toDouble)
          case other         => ujson.Str(other.toString)
        }): (String, ujson.Value)
    }
    ujson.write(ujson.Obj.from(fields))
  }

  def main(args: Array[String]): Unit = {
    val count       = args.headOption.map(_.toInt).getOrElse(1000)
    val instruments = args.lift(1).map(_.split(",").toIndexedSeq).getOrElse(Seq("BTC-USD", "ETH-USD"))
    val url         = args.lift(2).getOrElse(sys.env.getOrElse("STREAM_URL", "http://localhost:4195/events"))
    val intervalMs  = args.lift(3).map(_.toLong).getOrElse(1000L)
    val from        = args.lift(4).map(_.toInt).getOrElse(0)

    val rows = generate(count, instruments, intervalMs = intervalMs, from = from)

    rows.grouped(RequestSize).zipWithIndex.foreach { case (chunk, i) =>
      val body = chunk.map(toJsonLine).mkString("", "\n", "\n")
      val resp =
        try requests.post(url, data = body, readTimeout = 60000, check = false)
        catch {
          case e: java.net.ConnectException =>
            sys.error(s"Can't reach Redpanda Connect at $url (is the stack up? docker compose up -d): ${e.getMessage}")
        }
      if (!resp.is2xx) sys.error(s"POST $url failed with ${resp.statusCode}: ${resp.text()}")
      println(s"Request ${i + 1}: sent ${chunk.length} events")
    }

    println(
      s"Sent ${rows.length} synthetic events to $url " +
        s"(seq_no ${rows.head.getLong(6)}-${rows.last.getLong(6)}, ${rows.head.get(2)} to ${rows.last.get(2)})"
    )
  }
}
