import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
//import vegas.sparkExt._

object Main {
  def main(args: Array[String]): Unit = {

    /* ------- SPARK CONFIGS --------- */

    val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
      .set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("BI_project")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")



    /* ------- READING INPUT DATAFRAME --------- */

    val trans_df = spark.read.option("header", "true")
        .option("inferSchema", "True")
        .csv("src/resources/input.csv")

    val user_df = spark.read.option("header", "true")
      .option("inferSchema", "True")
      .option("delimiter", ";")
      .csv("src/resources/users.csv")

    user_df.show()
    user_df.printSchema()


    /* ------- PROCESSING DATA --------- */
    val processed_df = preprocess(trans_df, user_df, spark, sc)
    processed_df.show()



    spark.stop()
  }


  private def preprocess(transaction_df: DataFrame, user_df: DataFrame, spark: SparkSession, sc: SparkContext): DataFrame = {


    val percentiles = transaction_df.stat
      .approxQuantile("TransactionAmount", Array(0.25, 0.5, 0.75), 0.0)


    val distribution_stats = transaction_df.select(
      max("TransactionAmount").alias("Max"),
      min("TransactionAmount").alias("Min"),
      mean(col("TransactionAmount")).alias("Mean"),
      stddev(col("TransactionAmount")).alias("Standard Deviation")
    )
      .withColumn("0.25%", lit(percentiles(0)))
      .withColumn("0.50%", lit(percentiles(1)))
      .withColumn("0.75%", lit(percentiles(2)))


    print("\nTRANSACTION AMOUNT STATISTICAL PARAMETERS:\n")
    distribution_stats.show()

    //Preparing for outlier clipping and normalization (IQR approach):
    val iqr = percentiles(0) - percentiles(2)
    val lowerBound = percentiles(0) - 1.5 * iqr
    val upperBound = percentiles(2) + 1.5 * iqr

    val filteredTrans_df = transaction_df.filter(col("TransactionAmount") < lowerBound || col("TransactionAmount") > upperBound)


    //Preparing for normalization:
    val meanVal_filtered = filteredTrans_df.select(mean(col("TransactionAmount"))).head.getDouble(0)
    val stdDevVal_filtered = filteredTrans_df.select(stddev(col("TransactionAmount"))).head.getDouble(0)

    //Applying rest of the transformation:
    val output_df = filteredTrans_df
      .orderBy(col("Timestamp"))
      .withColumn("Category",
              when(col("TransactionAmount") <= percentiles(0), "Low")
              .when(col("TransactionAmount") <= percentiles(1), "Medium")
              .otherwise("High"))
      .withColumn("TransactionAmount_normalized", (col("TransactionAmount") - meanVal_filtered) / stdDevVal_filtered)
      .join(user_df, "UserID")


    println("Params after normalization")
    output_df.select(
      round(mean(col("TransactionAmount_normalized")), 2).alias("Mean"),
      round(stddev(col("TransactionAmount_normalized")), 2).alias("Standard Deviation")
    ).show()


    return output_df
  }
}