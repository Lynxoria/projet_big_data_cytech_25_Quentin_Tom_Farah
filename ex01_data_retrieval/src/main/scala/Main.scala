import org.apache.spark.sql.SparkSession
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

object Main {
  def main(args: Array[String]): Unit = {

    // 1. Initialisation de la session Spark avec configuration Minio (S3)
    val spark = SparkSession.builder()
      .appName("SparkApp")
      .master("local")
      .config("fs.s3a.access.key", "minio")
      .config("fs.s3a.secret.key", "minio123")
      .config("fs.s3a.endpoint", "http://localhost:9000/") // A changer lors du déploiement
      .config("fs.s3a.path.style.access", "true")
      .config("fs.s3a.connection.ssl.enable", "false")
      .config("fs.s3a.attempts.maximum", "1")
      .config("fs.s3a.connection.establish.timeout", "6000")
      .config("fs.s3a.connection.timeout", "5000")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    // URL du fichier Parquet (Exemple: Janvier 2023 - Yellow Taxi)
    // Astuce: Vous pouvez rendre cette URL dynamique via les arguments (args)
    val fileUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
    val localPath = System.getProperty("java.io.tmpdir") + "temp_taxi_data.parquet"
    val minioPath = "s3a://nyc-raw/yellow_tripdata_2023-01.parquet"

    println(s"--> Téléchargement de $fileUrl ...")

    // 2. Téléchargement du fichier en local (méthode simple via Java NIO)
    // Spark ne télécharge pas nativement via HTTP vers S3 sans complexité,
    // le plus simple est de passer par un temp local.
    try {
      val in = new URL(fileUrl).openStream()
      Files.copy(in, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING)
      println("--> Téléchargement local terminé.")

      // 3. Lecture et Écriture vers Minio
      println("--> Lecture du fichier et écriture vers Minio...")

      val df = spark.read.parquet(localPath)

      // On écrit directement dans le Data Lake
      df.write
        .mode("overwrite")
        .parquet(minioPath)

      println(s"--> Succès ! Données stockées dans $minioPath")

    } catch {
      case e: Exception =>
        println("Erreur lors du traitement : " + e.getMessage)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}