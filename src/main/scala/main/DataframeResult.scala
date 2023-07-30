import org.apache.spark.sql.{DataFrame, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object DataframeResult {
  def addDateColumn(dataframe: DataFrame): DataFrame = {
    dataframe.withColumn("date", lit("01/06/2022"))
  }

  def addYearColumn(dataframe: DataFrame): DataFrame = {
    dataframe.withColumn("year", year(to_date(col("date"), "dd/MM/yyyy")))
  }

  def addCountryNameColumn(dataframe: DataFrame, countryCodesDF: DataFrame): DataFrame = {
    dataframe.join(
      countryCodesDF,
      dataframe.col("country_code") === countryCodesDF.col("country_code"),
      "left_outer"
    ).drop(countryCodesDF.col("country_code"))
      .withColumnRenamed("country_name", "nom_Pays")
  }

  def addServiceDetailsColumn(dataframe: DataFrame, service: String): DataFrame = {
    dataframe.withColumn("détails service", when(col("service") === service, col("service")).otherwise("Autre"))
  }

  def addGoodDetailsColumn(dataframe: DataFrame, good: String): DataFrame = {
    dataframe.withColumn("détails Good", when(col("goods") === good, col("goods")).otherwise("Autre"))
  }

  
}
