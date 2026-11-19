package orderbook.testkit

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/** Helpers for writing test rows with plain doubles against the pipeline's
  * decimal price/qty columns.
  */
object Rows {

  def dec(d: Double): java.math.BigDecimal = new java.math.BigDecimal(d.toString)

  def dec(d: java.lang.Double): java.math.BigDecimal = if (d == null) null else dec(d.doubleValue)

  implicit class RowOps(private val r: Row) extends AnyVal {

    /** A decimal column's value as a double, for assertEquals against literals. */
    def dbl(name: String): Double = r.getAs[java.math.BigDecimal](name).doubleValue
  }

  /** The feed's seven columns with price/qty as doubles: the shape tests
    * write rows in before aligning them to a pipeline schema.
    */
  val feedWithDoubles: StructType = StructType(
    Seq(
      StructField("event_type", StringType),
      StructField("instrument", StringType),
      StructField("timestamp", TimestampType),
      StructField("side", StringType),
      StructField("price", DoubleType),
      StructField("qty", DoubleType),
      StructField("seq_no", LongType)
    )
  )
}
