import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

object Main {

  def main(args: Array[String]): Unit = {

    // --- 1. CONFIGURATION SPARK ---
    val spark = SparkSession.builder()
      .appName("Ex02_Robust_Q1_Ingestion")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .config("spark.sql.caseSensitive", "false") // Gère airport_fee vs Airport_fee
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    println("--- Démarrage de l'ingestion Robuste (Union Q1 2023) ---")

    // Liste des fichiers à traiter manuellement
    val files = Seq(
      "s3a://nyc-raw/yellow_tripdata_2023-01.parquet",
      "s3a://nyc-raw/yellow_tripdata_2023-02.parquet",
      "s3a://nyc-raw/yellow_tripdata_2023-03.parquet"
    )

    try {
      // --- 2. LECTURE & STANDARDISATION INDIVIDUELLE ---
      // On lit chaque fichier séparément pour laisser Spark déduire le schéma correct par fichier
      // Puis on cast tout vers un format standard
      val dataframes = files.map { filePath =>
        println(s"--> Lecture de $filePath ...")
        val df = spark.read.parquet(filePath)
        standardizeSchema(df)
      }

      // --- 3. FUSION (UNION) ---
      println("--> Fusion des 3 mois...")
      // reduce(_ unionByName _) fusionne tous les DataFrames de la liste en un seul
      val rawDf = dataframes.reduce(_ unionByName _)

      // --- 4. NETTOYAGE ---
      println("--> Nettoyage des données...")
      val cleanedDf = rawDf
        .filter(col("trip_distance") > 0)
        .filter(col("total_amount") >= 0)
        .na.fill(1, Seq("passenger_count")) // Remplace null par 1
        .filter(col("passenger_count") > 0)

      cleanedDf.cache()
      println(s"   -> Total lignes propres : ${cleanedDf.count()}")

      // --- 5. BRANCHE 1 : Minio (ML) ---
      val minioOutputPath = "s3a://nyc-raw/processed/yellow_tripdata_2023_Q1_clean.parquet"
      println(s"--> Sauvegarde Minio : $minioOutputPath")
      cleanedDf.write.mode("overwrite").parquet(minioOutputPath)

      // --- 6. BRANCHE 2 : Postgres (BI) ---
      println("--> Ingestion PostgreSQL...")
      val jdbcUrl = "jdbc:postgresql://localhost:5432/postgres"
      val dbProperties = new Properties()
      dbProperties.put("user", "postgres")
      dbProperties.put("password", "postgres")
      dbProperties.put("driver", "org.postgresql.Driver")

      // Le schéma est déjà standardisé, juste besoin de renommer pour Postgres
      val finalDf = cleanedDf
        .withColumnRenamed("VendorID", "vendor_id")
        .withColumnRenamed("RatecodeID", "rate_code_id")
        .withColumnRenamed("PULocationID", "pulocationid")
        .withColumnRenamed("DOLocationID", "dolocationid")
        .withColumnRenamed("payment_type", "payment_type_id")

      finalDf.write
        .mode(SaveMode.Append)
        .jdbc(jdbcUrl, "fact_trips", dbProperties)

      println("✅ Terminé avec succès !")

    } catch {
      case e: Exception =>
        println("!!! ERREUR CRITIQUE !!!")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * Fonction qui force un DataFrame à avoir un schéma précis via des CASTs.
   * C'est la méthode la plus sûre pour gérer l'évolution de schéma.
   */
  def standardizeSchema(df: DataFrame): DataFrame = {
    // On liste les colonnes qu'on veut absolument et leur type cible
    // Si une colonne n'existe pas dans le fichier, lit null (via un try/catch ou vérification optionnelle)
    // Ici on suppose que les colonnes existent mais ont le mauvais type.

    var tempDf = df

    // Liste des colonnes critiques et leur cast cible
    // On cast tout ce qui est ID en Long, tout ce qui est Prix/Distance en Double
    val casts = Map(
      "VendorID" -> "long",
      "passenger_count" -> "double", // On passe par double pour accepter 1.0 et 1
      "trip_distance" -> "double",
      "RatecodeID" -> "double",
      "PULocationID" -> "long",
      "DOLocationID" -> "long",
      "payment_type" -> "long",
      "fare_amount" -> "double",
      "extra" -> "double",
      "mta_tax" -> "double",
      "tip_amount" -> "double",
      "tolls_amount" -> "double",
      "improvement_surcharge" -> "double",
      "total_amount" -> "double",
      "congestion_surcharge" -> "double"
      // airport_fee est géré dynamiquement car il change de casse
    )

    casts.foreach { case (colName, typeName) =>
      if (tempDf.columns.contains(colName)) {
        tempDf = tempDf.withColumn(colName, col(colName).cast(typeName))
      }
    }

    // Gestion spéciale pour airport_fee (Casse variable)
    if (tempDf.columns.contains("Airport_fee")) {
      tempDf = tempDf.withColumnRenamed("Airport_fee", "airport_fee")
    }
    if (tempDf.columns.contains("airport_fee")) {
      tempDf = tempDf.withColumn("airport_fee", col("airport_fee").cast("double"))
    } else {
      // Si la colonne n'existe pas (ex: Janvier), on la crée avec 0.0 pour pouvoir fusionner
      tempDf = tempDf.withColumn("airport_fee", lit(0.0))
    }

    tempDf
  }
}