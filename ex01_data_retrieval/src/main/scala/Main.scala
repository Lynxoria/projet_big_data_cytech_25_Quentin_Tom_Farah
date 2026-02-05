import org.apache.spark.sql.SparkSession
import java.net.URL
import java.nio.file.{Files, Paths, StandardCopyOption}

object Main {
  def main(args: Array[String]): Unit = {

    // --- FIX WINDOWS OBLIGATOIRE ---
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    System.setProperty("hadoop.tmp.dir", System.getProperty("java.io.tmpdir"))

    // 1. Initialisation de la session Spark (Une seule fois au début)
    val spark = SparkSession.builder()
      .appName("Ex01_Data_Retrieval_MultiFiles")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
      .config("spark.hadoop.fs.s3a.buffer.dir", System.getProperty("java.io.tmpdir"))
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    // --- LISTE DES FICHIERS À TRAITER (Janvier, Février, Mars) ---
    val filesToProcess = Seq(
      "yellow_tripdata_2023-01.parquet",
      "yellow_tripdata_2023-02.parquet",
      "yellow_tripdata_2023-03.parquet"
    )

    val baseUrl = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    val tempDir = System.getProperty("java.io.tmpdir")

    println(s"--> Démarrage du traitement pour ${filesToProcess.length} fichiers...")

    try {
      // BOUCLE SUR CHAQUE FICHIER
      filesToProcess.foreach { fileName =>
        val fileUrl = baseUrl + fileName
        val localPath = Paths.get(tempDir, fileName).toString
        val minioPath = s"s3a://nyc-raw/$fileName"

        println(s"\n------------------------------------------------")
        println(s"--> Traitement de : $fileName")
        println(s"------------------------------------------------")

        try {
          // 2. Téléchargement
          println(s"--> Téléchargement depuis $fileUrl ...")
          val in = new URL(fileUrl).openStream()
          Files.copy(in, Paths.get(localPath), StandardCopyOption.REPLACE_EXISTING)
          in.close() // Bonne pratique : fermer le stream
          println("--> Téléchargement local terminé.")

          // 3. Lecture et Écriture vers Minio
          println(s"--> Envoi vers Minio ($minioPath)...")
          val df = spark.read.parquet(localPath)

          df.write
            .mode("overwrite")
            .parquet(minioPath)

          println(s"✅ Succès pour $fileName")

          // Optionnel : Supprimer le fichier local pour libérer de l'espace
          // Files.deleteIfExists(Paths.get(localPath))

        } catch {
          case e: Exception =>
            println(s"❌ ERREUR sur le fichier $fileName : " + e.getMessage)
            e.printStackTrace()
        }
      }

    } finally {
      println("\n--> Arrêt de la session Spark.")
      spark.stop()
    }
  }
}