import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Top50 (dataframe1: DataFrame, dataFrame2: DataFrame) extends Serializable {
  def getTop50(): DataFrame = {
    val joinedTables = dataframe1.join(dataFrame2,usingColumn = "PRODUCT_ID").dropDuplicates("PRODUCT_ID")
    val addedLog = joinedTables.withColumn("LOG_NUMR",log(joinedTables.col("NUM_REVIEW")))

    //Getting min and max value for LOG NUMR col
    val min_max_numR = addedLog.agg(min("LOG_NUMR"), max("LOG_NUMR")).head()
    val min_max_Avg = addedLog.agg(min("AVG_RATING"),max("AVG_RATING")).head()

    val normalization: Double => Double = normalizationFunc(_,min_max_numR.getDouble(0),min_max_numR.getDouble(1))
    val normalizationAvg: Double => Double = normalizationFunc(_,min_max_Avg(0).toString.toDouble,min_max_Avg(1).toString().toDouble)

    val normalizationUDF = udf(normalization)
    val normalizationAvgUDF = udf(normalizationAvg)

    val normaLog = addedLog.withColumn("NORM_LOG_NUMR",normalizationUDF(addedLog.col("LOG_NUMR")))

    //Norm Func AvgOverall Rating
    val normaAvgRate = normaLog.withColumn("NORM_AVG_RATING",normalizationAvgUDF(normaLog.col("AVG_RATING")))

    def scoreFunction= udf((normLogReview: Double, normRating: Double) => {
      (normLogReview+1)*(normRating+1)
    })

    val scoreFunc = normaAvgRate.withColumn("SCORE",scoreFunction(normaAvgRate.col("NORM_LOG_NUMR"),normaAvgRate.col("NORM_AVG_RATING")))

    return scoreFunc.sort(desc("SCORE")).select("PRODUCT_ID")
  }
  //Normalization Function
  def normalizationFunc(value: Double, min: Double, max: Double): Double ={
    return (value - min)/(max - min)
  }
}



object Top50 {
  def getTopFifty(dataframe1: DataFrame, dataFrame2: DataFrame): DataFrame ={
    val top50 = new Top50(dataframe1,dataFrame2).getTop50()
    return top50
  }
}
