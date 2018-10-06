

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger
import org.apache.log4j.Level

object SparkSessionExample {


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    val jsonFile = "C:\\Users\\almira2\\IdeaProjects\\practice2\\JSON.json";
    //Initialize SparkSession
    val sparkSession = SparkSession
      .builder()
      .appName("spark-sql-basic")
      .master("local[*]")
      .getOrCreate()

    //Read json file to DF
    val employeeDF = sparkSession.read.json(jsonFile)

    //Show the first 100 rows
    employeeDF.show(100)

    //Show thw scheme of DF
    employeeDF.printSchema()

    //Select only the "name" column
    employeeDF.select("name").show()

    //Select people older than 21
    employeeDF.where("age > 21").show()

    // Count people by age
    employeeDF.groupBy("age").count().show()

    // Register the DataFrame as a SQL temporary view
    employeeDF.createOrReplaceTempView("employee")

    sparkSession.sql("select * from employee where age > 21").show()
  }
}