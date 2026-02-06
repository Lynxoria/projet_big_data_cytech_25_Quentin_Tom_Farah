import org.apache.spark.sql.SparkSession
import java.nio.file.Paths

object Main {
  def main(args: Array[String]): Unit = {

    // --- CONFIG WINDOWS/HADOOP ---
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"))

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

    spark.sparkContext.setLogLevel("WARN")

    val filesToProcess = Seq(
      "yellow_tripdata_2023-01.parquet",
      "yellow_tripdata_2023-02.parquet",
      "yellow_tripdata_2023-03.parquet"
    )

    // On pointe directement sur ton dossier data/raw du projet
    val projectDir = System.getProperty("user.dir")
    val dataRawDir = Paths.get(projectDir, "data", "raw")

    println(s"--> Lecture depuis : $dataRawDir")

    filesToProcess.foreach { fileName =>
      val localPath = dataRawDir.resolve(fileName).toString
      val minioPath = s"s3a://nyc-raw/$fileName"

      println(s"--> Traitement de $fileName")

      try {
        // 1. On lit le fichier qui est DÉJÀ sur ton PC
        val df = spark.read.parquet(localPath)

        // 2. On l'écrit dans Minio (C'est ça l'ingestion)
        df.write.mode("overwrite").parquet(minioPath)

        println(s"✅ Envoyé vers Minio : $minioPath")

      } catch {
        case e: Exception =>
          println(s"❌ Erreur (Vérifie que le fichier est bien dans data/raw) : ${e.getMessage}")
      }
    }

    spark.stop()
  }
}