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

//    trans_df.printSchema()
//    println("Trans rows: ", trans_df.count())
//    user_df.printSchema()
//    println("User rows: ", user_df.count())


    /* ------- PROCESSING DATA --------- */
    val startTime_1 = System.nanoTime()
    val processed_df1 = preprocess(trans_df, user_df)
    val endTime_1 = System.nanoTime()
    println("Basic processing method execution time: ", (endTime_1 - startTime_1)/1e9)

    val startTime_2 = System.nanoTime()
    val processed_df2 = preprocess_optimized_m1(trans_df, user_df)
    val endTime_2 = System.nanoTime()
    println("Optimal processing method 1 (caching) execution time: ", (endTime_2 - startTime_2)/1e9)

    val startTime_3 = System.nanoTime()
    val processed_df3 = preprocess_optimized_m2(trans_df, user_df)
    val endTime_3 = System.nanoTime()
    println("Optimal processing method 2 (broadcast join) execution time: ", (endTime_3 - startTime_3) / 1e9)

    val startTime_4 = System.nanoTime()
    val processed_df4 = preprocess_optimized_m3(trans_df, user_df)
    val endTime_4 = System.nanoTime()
    println("Optimal processing method 3 (partitioning) execution time: ", (endTime_4 - startTime_4) / 1e9)

    val startTime_5 = System.nanoTime()
    val processed_df5 = preprocess_optimal(trans_df, user_df)
    val endTime_5 = System.nanoTime()
    println("Optimal processing method 4 (combined) execution time: ", (endTime_5 - startTime_5) / 1e9)

    spark.stop()
  }


  private def preprocess(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    val percentiles = transaction_df.stat
      .approxQuantile("TransactionAmount", Array(0.25, 0.5, 0.75), 0.0)

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


    return output_df
  }

  private def preprocess_optimized_m1(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    val percentiles = transaction_df.stat
      .approxQuantile("TransactionAmount", Array(0.25, 0.5, 0.75), 0.0)

    //Preparing for outlier clipping (IQR approach):
    val iqr = percentiles(0) - percentiles(2)
    val lowerBound = percentiles(0) - 1.5 * iqr
    val upperBound = percentiles(2) + 1.5 * iqr

    val filteredTrans_df = transaction_df.filter(col("TransactionAmount") < lowerBound || col("TransactionAmount") > upperBound).cache()

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

    return output_df
  }

  private def preprocess_optimized_m2(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    val percentiles = transaction_df.stat
      .approxQuantile("TransactionAmount", Array(0.25, 0.5, 0.75), 0.0)

    //Preparing for outlier clipping (IQR approach):
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
      .join(broadcast(user_df), "UserID")

    return output_df
  }

  private def preprocess_optimized_m3(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    val percentiles = transaction_df.stat
      .approxQuantile("TransactionAmount", Array(0.25, 0.5, 0.75), 0.0)

    //Preparing for outlier clipping (IQR approach):
    val iqr = percentiles(0) - percentiles(2)
    val lowerBound = percentiles(0) - 1.5 * iqr
    val upperBound = percentiles(2) + 1.5 * iqr

    val partitionedDF = transaction_df.repartitionByRange(3, col("TransactionAmount"))

    val filteredTrans_df = partitionedDF.filter(col("TransactionAmount") < lowerBound || col("TransactionAmount") > upperBound)

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

    return output_df
  }

  private def preprocess_optimal(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    val percentiles = transaction_df.stat
      .approxQuantile("TransactionAmount", Array(0.25, 0.5, 0.75), 0.0)


    //    val distribution_stats = transaction_df.select(
    //        max("TransactionAmount").alias("Max"),
    //        min("TransactionAmount").alias("Min"),
    //        mean(col("TransactionAmount")).alias("Mean"),
    //        stddev(col("TransactionAmount")).alias("Standard Deviation")
    //      )
    //      .withColumn("0.25%", lit(percentiles(0)))
    //      .withColumn("0.50%", lit(percentiles(1)))
    //      .withColumn("0.75%", lit(percentiles(2)))


    //    print("\nTRANSACTION AMOUNT STATISTICAL PARAMETERS:\n")
    //    distribution_stats.show()

    //Preparing for outlier clipping (IQR approach):
    val iqr = percentiles(0) - percentiles(2)
    val lowerBound = percentiles(0) - 1.5 * iqr
    val upperBound = percentiles(2) + 1.5 * iqr


    val partitionedDF = transaction_df.repartitionByRange(3, col("TransactionAmount"))

    val filteredTrans_df = partitionedDF.filter(col("TransactionAmount") < lowerBound || col("TransactionAmount") > upperBound).cache()

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
      .join(broadcast(user_df), "UserID")


    //    println("Params after normalization")
    //    output_df.select(
    //      round(mean(col("TransactionAmount_normalized")), 2).alias("Mean"),
    //      round(stddev(col("TransactionAmount_normalized")), 2).alias("Standard Deviation")
    //    ).show()

    return output_df
  }

}