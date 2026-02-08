import org.apache.spark.sql.SparkSession
import java.nio.file.Paths

/**
 * =================================================================================
 * Projet      : NYC Taxi Big Data Architecture
 * Exercice    : 1 - Collecte et Ingestion (Data Lake)
 * Description : Ce script réalise l'ingestion des données brutes.
 * Il lit les fichiers Parquet stockés localement (data/raw)
 * et les transfère vers le Data Lake (MinIO).
 * =================================================================================
 */

object Main {
  def main(args: Array[String]): Unit = {

    // --- 1. CONFIGURATION DE L'ENVIRONNEMENT ---

    // Configuration spécifique pour Windows (Hadoop binaries / winutils.exe)
    // Nécessaire pour éviter les erreurs "Failed to locate the winutils binary" sur Windows.
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"))

    // Initialisation de la session Spark
    val spark = SparkSession.builder()
      .appName("Ex01_Local_to_Minio")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    // Réduction du niveau de log pour ne voir que les avertissements et erreurs
    spark.sparkContext.setLogLevel("WARN")

    // --- 2. DÉFINITION DES DONNÉES À TRAITER ---

    // Liste des fichiers Parquet (Q1 2023) à ingérer
    val filesToProcess = Seq(
      "yellow_tripdata_2023-01.parquet",
      "yellow_tripdata_2023-02.parquet",
      "yellow_tripdata_2023-03.parquet"
    )

    // Construction du chemin absolu vers le dossier local 'data/raw' du projet
    // System.getProperty("user.dir") récupère la racine du projet dynamiquement
    val projectDir = System.getProperty("user.dir")
    val dataRawDir = Paths.get(projectDir, "data", "raw")

    println(s"--> Lecture depuis : $dataRawDir")

    // --- 3. TRAITEMENT ET INGESTION ---

    filesToProcess.foreach { fileName =>
      // Construction des chemins source (Local) et destination (MinIO)
      val localPath = dataRawDir.resolve(fileName).toString
      val minioPath = s"s3a://nyc-raw/$fileName"

      println(s"--> Traitement de $fileName")

      try {
        // ÉTAPE A : Lecture du fichier local via Spark
        // Spark vérifie ici l'intégrité du Parquet
        val df = spark.read.parquet(localPath)

        // 2. On l'écrit dans Minio
        df.write.mode("overwrite").parquet(minioPath)

        println(s" Envoyé vers Minio : $minioPath")

      } catch {
        case e: Exception =>
          println(s" Erreur (Vérifie que le fichier est bien dans data/raw) : ${e.getMessage}")
      }
    }

    spark.stop()
  }
}