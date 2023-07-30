import org.apache.spark.sql.{SparkSession, DataFrame}

object Utils {
  // Crée un SparkSession et retourne l'instance
  def createSparkSession(appName: String): SparkSession = {
    SparkSession
      .builder()
      .appName(appName)
      .getOrCreate()
  }

  // Lit un fichier CSV et retourne un DataFrame
  def readCSV(spark: SparkSession, filePath: String): DataFrame = {
    spark.read
      .option("header", "true") // Si le fichier CSV a une ligne d'en-tête
      .option("inferSchema", "true") // Inférer le schéma des colonnes
      .csv(filePath)
  }

 def writeCSV(dataframe: DataFrame, outputPath: String): Unit = {
   dataframe.write
     .option("header", "true")
     .csv(outputPath)
 }

 def writeParquet(dataframe: DataFrame, outputPath: String): Unit = {
   dataframe.write
     .parquet(outputPath)
 }


}

