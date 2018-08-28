import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{Level, Logger}

object TestSpark {
  //Trigering Spark sessions
  Logger.getLogger("org").setLevel(Level.OFF)
  val conf = new SparkConf().setAppName("test").setMaster("local")
  val sc = new SparkContext(conf)
  val spark = SparkSession.builder().appName("test").master("local").getOrCreate()

  //The DataFrames
  val dfProdDetails = spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .load("data/gold_atg_sku.csv")
    .toDF()

  val dfUserRatings = spark.read
    .format("csv")
    .option("delimiter", ",")
    .option("header", "true")
    .load("data/raw_ratings_reviews_new.csv")
    .toDF()


  //Main method The program starts here
  def main(args: Array[String]): Unit = {
    //Formatting the tables data
    val nameOfTable = "UserRatings"
    dfUserRatings.createOrReplaceTempView(nameOfTable)
    val rawRatings = RawRatings.filterTheNull(dfUserRatings)

    val nameOfTableP = "ProdDetails"
    dfProdDetails.createOrReplaceTempView(nameOfTableP)
    val prodDetails = GoldAtgSku.filterTheNull(dfProdDetails)

    //Putting the two DFs as parameters to the
    val top50 = Top50.getTopFifty(prodDetails,rawRatings).select("PRODUCT_ID").collect().map(_.getString(0)).mkString(" ")
    val mappy = Map("top_rated" -> top50)
    println(mappy)
  }
}