import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.{col, countDistinct}

object Main {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setAppName("name").setMaster("local")
      .set("spark.testing.memory", "2147480000")

    val sc: SparkContext = new SparkContext(conf)

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("BI_project")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val trans_df = spark.read.option("header", "true")
        .option("inferSchema", "True")
        .csv("src/resources/input.csv")

    println("Input data: ")
    trans_df.show()

    println("\nSchema: ")
    trans_df.printSchema()



    val (processed_trans_df, processed_user_df) = preprocess(trans_df, 940, spark)
    println("\nOutput data (transactions): ")
    processed_trans_df.show()

    println("\nOutput data (users): ")
    processed_user_df.show()

    spark.stop()
  }


  def preprocess(transaction_df: DataFrame, threshold: Double, spark: SparkSession): (DataFrame, DataFrame) = {

    val user_df = spark.read.option("header", "true")
      .option("inferSchema", "True")
      .csv("src/resources/users.csv")

    val output_transaction__df = transaction_df
      .filter(transaction_df("TransactionAmount") > threshold)
      .orderBy(transaction_df("Timestamp"))

    val output_user_df = user_df

    return (output_transaction__df, output_user_df)
  }
}