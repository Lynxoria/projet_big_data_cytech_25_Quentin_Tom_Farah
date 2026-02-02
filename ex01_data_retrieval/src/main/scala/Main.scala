import org.apache.spark.sql.SparkSession
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

object Main {
  def main(args: Array[String]): Unit = {

    // --- FIX WINDOWS OBLIGATOIRE ---
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    // LA LIGNE QUI MANQUAIT POUR L'ERREUR "RELATIVE PATH":
    System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"))

    // 1. Initialisation de la session Spark
    val spark = SparkSession.builder()
      .appName("Ex01_Data_Retrieval")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      // Identifiants Docker (minioadmin)
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      // Force l'utilisation d'un buffer disque valide sur Windows
      .config("spark.hadoop.fs.s3a.buffer.dir", System.getProperty("java.io.tmpdir"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    val fileUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    // Chemin local sécurisé
    val tempDir = System.getProperty("java.io.tmpdir")
    val localPath = Paths.get(tempDir, "temp_taxi_data.parquet").toString
    // Chemin Minio
    val minioPath = "s3a://nyc-raw/yellow_tripdata_2023-01.parquet"

    println(s"--> Téléchargement de $fileUrl ...")

    try {
      // 2. Téléchargement
      val in = new URL(fileUrl).openStream()
      Files.copy(in, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING)
      println("--> Téléchargement local terminé.")

      // 3. Lecture et Écriture
      println("--> Lecture et écriture vers Minio...")
      val df = spark.read.parquet(localPath)

      df.write
        .mode("overwrite")
        .parquet(minioPath)

      println(s"--> Succès ! Données stockées dans $minioPath")

    } catch {
      case e: Exception =>
        println("ERREUR : " + e.getMessage)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}