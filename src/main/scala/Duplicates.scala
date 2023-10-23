import Monitoring.createLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Duplicates {

  def deduplicate(df: DataFrame): DataFrame ={

//    val obv_dupes = df.where(col("TransactionID").contains("DUP"))
//    println(f"Obvious duplicate row count: ${obv_dupes.count()}")

    //Considering duplicates as rows with identical UserID, Transaction amount and Timestamp
    //No aggregation needed, since other attributed would be equal.

    val windowSpec = Window.partitionBy("UserID", "TransactionAmount", "Timestamp").orderBy(desc("TransactionID"))

    val clean =  df.withColumn("Dupe_count", rank().over(windowSpec))
      .filter(col("Dupe_count")===1)
      .drop("Dupe_count")

    println(f"Records left in dataset after duplicate cleaning: ${clean.count()}")

    //logging
    val message = f"""{"removedDupes": ${df.count()-clean.count()}}"""
    createLog(message, "DUPLICATE_REMOVAL", "Dupe_test")

    return clean
  }

}
