import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ReadCSVExample {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val inputFile = "C:\\Users\\almira2\\IdeaProjects\\practice2\\titanic.csv"

    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-read-csv")
      .master("local[*]")
      .getOrCreate();

    //Read CSV file to DF and define scheme on the fly
    val passengers = sparkSession.read
      .option("header", "true")
      .option("delimiter", "\t")
      .option("nullValue", "")
      .option("treatEmptyValuesAsNulls", "true")
      .option("inferSchema", "true")
      .csv(inputFile)



    passengers.show(100)
    passengers.printSchema()
    passengers.createOrReplaceTempView("almira")
    sparkSession.sql(
      " SELECT Name, Sex" +
        " FROM almira" +
        "")
      .show(100)
  }
}
