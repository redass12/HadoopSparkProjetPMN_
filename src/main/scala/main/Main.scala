import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import utils.Utils
import common.{ApplicationProperties, Constants}
import UserDefinedFunction._

object Main {
  def main(args: Array[String]): Unit = {
    
    val spark = Utils.createSparkSession("HadoopSparkProjetPMN")

   
    val dataframe = Utils.readCSV(spark, ApplicationProperties.InputFilePath)

   

    // Transformation 1 : 
    val dataframeWithDate = dataframe.withColumn(Constants.ColumnDate, convertTimeRefToDate(col(Constants.ColumnTimeRef)))

    // Transformation 2 : 
    val dataframeWithYear = dataframeWithDate.withColumn(Constants.ColumnYear, year(col(Constants.ColumnDate)))

    // Transformation 3 : 
    val dataframeWithCountryName = dataframeWithYear.withColumn(Constants.ColumnCountryName, lit(""))

    // Sauvegarde en CSV
    Utils.writeCSV(dataframeWithCountryName, ApplicationProperties.OutputCSVFilePath)

    // Sauvegarde en Parquet
    Utils.writeParquet(dataframeWithCountryName, ApplicationProperties.OutputParquetFilePath)

    // Arrêter le SparkSession
    spark.stop()
  }
}
