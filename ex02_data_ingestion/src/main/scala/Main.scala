import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {

    // 1. Configuration (Reprise de votre config fonctionnelle)
    val spark = SparkSession.builder()
      .appName("Exercice2_Cleaning")
      .master("local")
      .config("fs.s3a.access.key", "minio")
      .config("fs.s3a.secret.key", "minio123")
      .config("fs.s3a.endpoint", "http://localhost:9000/")
      .config("fs.s3a.path.style.access", "true")
      .config("fs.s3a.connection.ssl.enable", "false")
      .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("--- Démarrage Exercice 2 : Nettoyage (Branche 1) ---")

    // 2. Lecture depuis le Data Lake (Fichier Brut déposé à l'Ex 1)
    val inputPath = "s3a://nyc-raw/yellow_tripdata_2023-01.parquet"
    println(s"Lecture de : $inputPath")

    try {
      val rawDf = spark.read.parquet(inputPath)

      // 3. Nettoyage des données (Data Cleaning)
      // On applique des règles métier basiques pour avoir une donnée de qualité
      val cleanedDf = rawDf
        // Règle 1 : Pas de distance négative ou nulle
        .filter(col("trip_distance") > 0)
        // Règle 2 : Pas de montant payé négatif
        .filter(col("total_amount") >= 0)
        // Règle 3 : On s'assure que le nombre de passagers est cohérent (optionnel mais recommandé)
        .filter(col("passenger_count") > 0)

      // Affichage pour contrôle
      val countRaw = rawDf.count()
      val countClean = cleanedDf.count()
      println(s"Lignes avant nettoyage : $countRaw")
      println(s"Lignes après nettoyage : $countClean")
      println(s"Lignes supprimées : ${countRaw - countClean}")

      // 4. Écriture du résultat propre dans Minio (Dossier 'processed' ou 'clean')
      // C'est la "Branche 1" destinée aux Data Scientists
      val outputPath = "s3a://nyc-raw/processed/yellow_tripdata_2023-01_clean.parquet"

      cleanedDf.write
        .mode("overwrite")
        .parquet(outputPath)

      println(s"Données nettoyées sauvegardées dans : $outputPath")

    } catch {
      case e: Exception =>
        println("Erreur : " + e.getMessage)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
