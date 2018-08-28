//Import Statements
import org.apache.spark.sql.{DataFrame, SparkSession}
import TestSpark.spark
import org.apache.spark.sql.functions._


class GoldAtgSku (dataFrame: DataFrame) {

  def filterTheNullValues(): DataFrame ={
    val query = spark.sql("SELECT `gold_atg_sku.sku_id` as SKU_ID," +
      "`gold_atg_sku.parent_product_id` as PRODUCT_ID," +
      "`gold_atg_sku.product_primary_type_value` as PRIMARY_TYPE_VALUE," +
      "`gold_atg_sku.product_type_value` as TYPE_VALUE," +
      "`gold_atg_sku.product_sub_type_value` as SUB_TYPE_VALUE from ProdDetails")
      .toDF()
      .filter(col("SKU_ID").isNotNull
        && !col("PRODUCT_ID").contains("NULL")
        && !col("PRIMARY_TYPE_VALUE").contains("NULL")
        && !col("TYPE_VALUE").contains("NULL")
        && !col("SUB_TYPE_VALUE").contains("NULL"))
    return query.toDF()
  }
}

object GoldAtgSku {

  private def apply(df:DataFrame): GoldAtgSku = {
    new GoldAtgSku(df)
  }

  //Object we use to write methods and handle redundancy etc.
  def filterTheNull(dataFrame: DataFrame): DataFrame ={
    return apply(dataFrame).filterTheNullValues()
  }
}
