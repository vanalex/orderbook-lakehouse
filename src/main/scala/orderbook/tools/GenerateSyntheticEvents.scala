package example

import org.apache.spark.sql.Row

import java.sql.Timestamp
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/** Synthetic demo-data source for `orderbook.bronze.raw_events`
  * (data_pipeline_plan.md, Phase 3 "source for demo data").
  *
  * Simulates a toy order book per instrument: `add` events push onto a
  * resting-orders list, `cancel`/`modify`/`trade` events pick a resting order
  * off that list, so the generated feed has internally consistent references
  * (a `cancel` always cancels a real prior `add`) instead of pure noise.
  *
  * This job only writes files to a landing path — it doesn't touch the
  * Iceberg table itself. `IngestRawEvents` is what reads that path and
  * appends to `orderbook.bronze.raw_events`, the same way it would for a
  * historical replay dump.
  *
  * Run with: sbt "runMain example.GenerateSyntheticEvents [count] [instruments] [path] [format]"
  * e.g.:     sbt "runMain example.GenerateSyntheticEvents 5000 BTC-USD,ETH-USD data/raw_events json"
  *
  * `path`/`format` default to `data/raw_events`/`json`, matching
  * `IngestRawEvents`' own defaults, and can also come from the
  * `SOURCE_PATH`/`SOURCE_FORMAT` env vars.
  */
object GenerateSyntheticEvents {

  private case class RestingOrder(side: String, price: Double, var qty: Double)

  /** Pure event generation, no Spark/IO — kept separate so it's unit-testable. */
  def generate(count: Int, instruments: Seq[String], seed: Long = 42L): Seq[Row] = {
    val rnd = new Random(seed)
    // Fixed epoch anchor (not wall-clock) so runs are reproducible for a given seed.
    val startMillis = 1700000000000L

    val resting  = instruments.map(_ -> ArrayBuffer.empty[RestingOrder]).toMap
    val midPrice = scala.collection.mutable.Map(instruments.map(_ -> 100.0): _*)

    (0 until count).map { i =>
      val instrument = instruments(rnd.nextInt(instruments.length))
      val book       = resting(instrument)
      val ts         = new Timestamp(startMillis + i * 10L)
      val seqNo      = (i + 1).toLong

      val roll = rnd.nextDouble()
      val eventType =
        if (book.isEmpty || roll < 0.6) OrderBookSchema.EventType.Add
        else if (roll < 0.75) OrderBookSchema.EventType.Cancel
        else if (roll < 0.9) OrderBookSchema.EventType.Modify
        else if (roll < 0.97) OrderBookSchema.EventType.Trade
        else OrderBookSchema.EventType.Snapshot

      eventType match {
        case OrderBookSchema.EventType.Add =>
          val side = if (rnd.nextBoolean()) "buy" else "sell"
          midPrice(instrument) += (rnd.nextDouble() - 0.5)
          val price = midPrice(instrument) + (if (side == "buy") -rnd.nextDouble() else rnd.nextDouble())
          val qty   = 1 + rnd.nextInt(100)
          book += RestingOrder(side, price, qty.toDouble)
          Row(eventType, instrument, ts, side, price, qty.toDouble, seqNo)

        case OrderBookSchema.EventType.Cancel =>
          val order = book.remove(rnd.nextInt(book.length))
          Row(eventType, instrument, ts, order.side, order.price, order.qty, seqNo)

        case OrderBookSchema.EventType.Modify =>
          val order = book(rnd.nextInt(book.length))
          order.qty = math.max(1.0, order.qty + (rnd.nextInt(21) - 10))
          Row(eventType, instrument, ts, order.side, order.price, order.qty, seqNo)

        case OrderBookSchema.EventType.Trade =>
          val idx        = rnd.nextInt(book.length)
          val order      = book(idx)
          val tradedQty  = math.min(order.qty, 1 + rnd.nextInt(order.qty.toInt)).toDouble
          order.qty -= tradedQty
          if (order.qty <= 0) book.remove(idx)
          Row(eventType, instrument, ts, order.side, order.price, tradedQty, seqNo)

        case OrderBookSchema.EventType.Snapshot =>
          Row(eventType, instrument, ts, null, null, null, seqNo)
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val count       = args.headOption.map(_.toInt).getOrElse(1000)
    val instruments = args.lift(1).map(_.split(",").toIndexedSeq).getOrElse(Seq("BTC-USD", "ETH-USD"))
    val path        = args.lift(2).getOrElse(sys.env.getOrElse("SOURCE_PATH", "data/raw_events"))
    val format      = args.lift(3).getOrElse(sys.env.getOrElse("SOURCE_FORMAT", "json"))

    val spark = PolarisSpark.session("orderbook-generate-synthetic-events")

    val rows = generate(count, instruments)
    val df   = spark.createDataFrame(spark.sparkContext.parallelize(rows), OrderBookSchema.bronzeRawEvents)
    // append: each run adds another batch of files, so re-running
    // IngestRawEvents against a growing `path` picks up more data over time.
    df.write.format(format).mode("append").save(path)

    println(s"Wrote ${rows.length} synthetic events to $path ($format)")

    spark.stop()
  }
}
