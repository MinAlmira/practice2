import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSQL {

 def main(args: Array[String]) {


  val jsonFile = "C:\\Users\\almira2\\IdeaProjects\\practice2\\JSON.json";

  //Initialize SparkSession
  val sparkSession = SparkSession
    .builder()
    .appName("spark-sql-tweets")
    .master("local[*]")
    .getOrCreate()

  //Read json file to DF
  val tweetsDF = sparkSession.read.json(jsonFile)

  //Show the first 100 rows
  tweetsDF.show(100);
 }
}