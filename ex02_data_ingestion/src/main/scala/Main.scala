import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {
  def main(args: Array[String]): Unit = {

    // --- 1. CONFIGURATION SPARK (Minio + Postgres) ---
    val spark = SparkSession.builder()
      .appName("Ex02_SingleJob_Branching")
      .master("local[*]")
      // Config Minio
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio") // ou minio
      .config("spark.hadoop.fs.s3a.secret.key", "minio123") // ou minio123
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("--- Démarrage du Job Multi-Branche ---")

    // --- 2. LECTURE (Depuis Minio Raw) ---
    // On reprend le fichier brut téléchargé à l'exercice 1
    val rawInputPath = "s3a://nyc-raw/yellow_tripdata_2023-01.parquet"
    println(s"1. Lecture des données brutes : $rawInputPath")

    try {
      val rawDf = spark.read.parquet(rawInputPath)

      // --- 3. TRANSFORMATION (Nettoyage en mémoire) ---
      // Source 54: "Apply transformations in memory"
      println("2. Nettoyage des données en cours...")
      val cleanedDf = rawDf
        .filter(col("trip_distance") > 0)
        .filter(col("total_amount") >= 0)
        .filter(col("passenger_count") > 0)
      // On peut ajouter ici une conversion de types si nécessaire pour matcher Postgres
      // ex: cast des colonnes float/double

      // On met le DataFrame en cache car on va l'utiliser deux fois (Branching)
      cleanedDf.cache()

      println(s"   -> ${rawDf.count()} lignes brutes")
      println(s"   -> ${cleanedDf.count()} lignes propres conservées")

      // --- 4. BRANCHE 1 : Sauvegarde sur Minio (Pour le ML) ---
      val minioOutputPath = "s3a://nyc-raw/processed/yellow_tripdata_2023-01_clean.parquet"
      println(s"3. [Branche 1] Sauvegarde vers Minio : $minioOutputPath")

      cleanedDf.write
        .mode("overwrite")
        .parquet(minioOutputPath)

      println("   -> Sauvegarde Minio terminée.")

      // --- 5. BRANCHE 2 : Ingestion dans Postgres (Pour la BI) ---
      println("4. [Branche 2] Ingestion vers PostgreSQL...")

      val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
      val dbProperties = new Properties()
      dbProperties.put("user", "postgres")
      dbProperties.put("password", "postgres")
      dbProperties.put("driver", "org.postgresql.Driver")

      // --- CORRECTION : Renommage des colonnes pour matcher Postgres ---
      val finalDf = cleanedDf
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("PULocationID", "pulocationid")
        .withColumnRenamed("DOLocationID", "dolocationid")
        .withColumnRenamed("payment_type", "payment_type_id") // Attention ici parfois c'est payment_type tout court dans le parquet

      // Vérifiez les colonnes restantes avec un printSchema si ça replante
      // finalDf.printSchema()

      finalDf.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "fact_trips", dbProperties)

      println("   -> Ingestion Postgres terminée.")
    } catch {
      case e: Exception =>
        println("!!! ERREUR !!!")
        println(e.getMessage)
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}