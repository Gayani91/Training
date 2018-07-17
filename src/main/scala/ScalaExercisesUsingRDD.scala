//Import Statements
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}
import spark.implicits._

object ScalaExercisesUsingRDD {
  //Main
  def main(args: Array[String]): Unit = {
    //Variables
    val conf = new SparkConf().setAppName("airports").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val realEstate = sc.textFile("data/RealEstate.csv")

    val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

    val rddData = spark.read
      .format("csv")
      .option("delimiter", ",")
      .option("header", "true")
      .load("/home/ayoobm/Desktop/Training/data/RealEstate.csv")
      .rdd
      .persist(StorageLevel MEMORY_ONLY)

    //All the houses in Santa Maria Orcutt
    val locCount = rddData.filter(r=>r.getAs[String]("Location")=="Santa Maria-Orcutt").count()
    println(locCount)

    //All houses location and Price of houses above 500 000
    val priceOv500000 = rddData.filter(line => line.getAs[String]("Price").toFloat > 500000.00)
    for (line <- priceOv500000)println(line(1)+" : $ "+line(2))


    //Houses with 3 bedrooms and for short sale
    val housesWith3Bed = rddData.filter(line => (line.getAs[String]("Bedrooms").toInt == 3) && (line.getAs[String]("Status") == "Short Sale"))
    for (line <- housesWith3Bed)println(line)

    //Find the highest priced house in Cayucos
    val highestInCay = rddData.filter(line => line.getAs[String]("Location") == "Cayucos")
    println(highestInCay)

    //Find the average price of houses in each city
    val avgPriceInCity = realEstate.map(line => line.split(",")).groupBy(line => line(1)).map(word => {
      var countAvg = 0.0
      for (list <- word._2){
        countAvg += list(2).toDouble
      }
      print("Average Price for "+word._1+" : "+countAvg/word._2.toList.length)
      countAvg = 0.0
    })
    for(line <- avgPriceInCity)println()


    val avgPricePerCity = rddData.groupBy(r=>r.getAs[String]("Location")).aggregate()
    avgPricePerCity

  }
}
