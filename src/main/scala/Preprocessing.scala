import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
object Preprocessing {

  def preprocess(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    println("Preprocessing method: Basic")
    val startTime = System.nanoTime()

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

    val endTime = System.nanoTime()
    println(f"Method execution time: ${(endTime - startTime)/1e9} sec")

    return output_df
  }

  def preprocess_optimized_m1(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    println("Preprocessing method: Optimized (caching)")
    val startTime = System.nanoTime()

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

    val endTime = System.nanoTime()
    println(f"Method execution time: ${(endTime - startTime) / 1e9} sec")

    return output_df
  }

  def preprocess_optimized_m2(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    println("Preprocessing method: Optimized (broadcast join)")
    val startTime = System.nanoTime()

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

    val endTime = System.nanoTime()
    println(f"Method execution time: ${(endTime - startTime) / 1e9} sec")

    return output_df
  }

  def preprocess_optimized_m3(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    println("Preprocessing method: Optimized (partitioning)")
    val startTime = System.nanoTime()

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

    val endTime = System.nanoTime()
    println(f"Method execution time: ${(endTime - startTime) / 1e9} sec")

    return output_df
  }

  def preprocess_optimal(transaction_df: DataFrame, user_df: DataFrame): DataFrame = {

    println("Preprocessing method: Optimized (combined)")
    val startTime = System.nanoTime()

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

    val endTime = System.nanoTime()
    println(f"Method execution time: ${(endTime - startTime) / 1e9} sec")

    return output_df
  }

}
