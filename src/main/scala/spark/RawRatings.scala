//Import Statements
import org.apache.spark.sql.{DataFrame, SparkSession}
import TestSpark.spark
import javax.management.Query
import org.apache.spark.sql.functions._


class RawRatings (dataFrame: DataFrame) {

  def filterTheNullValues(): DataFrame ={
    val query = spark.sql("SELECT `raw_ratings_reviews.product_id` as PRODUCT_ID," +
      "`raw_ratings_reviews.feed_date` as FEED_DATE," +
      "`raw_ratings_reviews.numreview` as NUM_REVIEW," +
      "averageoverallrating as AVG_RATING from UserRatings")
      .toDF()
      .filter(!col("PRODUCT_ID").contains("NULL")
        && !col("FEED_DATE").contains("NULL")
        && !col("NUM_REVIEW").contains("NULL")
        && !col("AVG_RATING").contains("NULL")
        && !col("AVG_RATING").isNaN)
    return query.toDF()
  }
}

object RawRatings {

  private def apply(df:DataFrame): RawRatings = {
    new RawRatings(df)
  }

  //Object we use to write methods and handle redundancy etc.
  def filterTheNull(dataFrame: DataFrame): DataFrame ={
    return apply(dataFrame).filterTheNullValues()
  }
}

