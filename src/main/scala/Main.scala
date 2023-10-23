import Duplicates._
import Preprocessing._
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


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


    println("="*30)
    print("INPUT DATA'S STRUCTURE:\n")
    trans_df.printSchema()
    println(f"Transaction data row count: ${trans_df.count()}")
    user_df.printSchema()
    println(f"User data row count: ${user_df.count()}")



    /* --------- PROCESSING DATA --------- */
    println("=" * 30)
    print("TESTING OPTIMALITY OF DIFFERENT PREPROCESSING METHODS:\n")
    val processed_df1 = preprocess(trans_df, user_df)
    val processed_df2 = preprocess_optimized_m1(trans_df, user_df)
    val processed_df3 = preprocess_optimized_m2(trans_df, user_df)
    val processed_df4 = preprocess_optimized_m3(trans_df, user_df)
    val processed_df5 = preprocess_optimal(trans_df, user_df)


    /*----------------- DUPLICATE DETECTION ---------------*/
    println("=" * 30)
    print("DEDUPLICATION TASK:\n")
    val clean = deduplicate(processed_df5)

    /*--------- (Optional) SAVING DATA TO CSV ------------*/
    //clean.write.mode(SaveMode.Overwrite).format("csv").save("../resources/out.csv")

    println("=" * 30)
    println("END ON TASK EXECUTION")
    spark.stop()
  }
}