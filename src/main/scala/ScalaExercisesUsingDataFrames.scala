//Import Statements
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object ScalaExercisesUsingDataFrames {
  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("airports").setMaster("local")
  val sc = new SparkContext(conf)

  val realEstate = sc.textFile("data/RealEstate.csv")

  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()
  import spark.implicits._

  val rddData = spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .load("/home/ayoobm/Desktop/Training/data/RealEstate.csv")
    .toDF()

  def main(args: Array[String]): Unit = {
    rddData.createOrReplaceTempView("estate")

    //All houses in Santa Maria
    //val query = spark.sql("SELECT * from estate where Location == 'Santa Maria-Orcutt'")

    //houses location and price over 500 000
    //val query = spark.sql("SELECT Location, Price from estate where Price > 500000")

    //Houses with 3 bedroom and for short sale
    //val query = spark.sql("SELECT * from estate where Bedrooms == 3 and Status == 'Short Sale'")

    //Find the highest priced house in Cayucos
    //val query = spark.sql("SELECT MAX(Price) from estate where Location == 'Cayucos'")

    //Find the averaged price house all the cities
    val query = spark.sql("SELECT Location,AVG(Price) from estate group by Location")

    for (line <- query)println(line)
  }
}
