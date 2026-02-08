import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import java.util.Properties

/**
 * =================================================================================
 * Projet      : NYC Taxi Big Data Architecture
 * Exercice    : 2 - Nettoyage et Ingestion Multi-branches
 * Description : Ce script réalise le pipeline ETL principal.
 * 1. Extraction : Lecture des fichiers Parquet hétérogènes (Q1 2023).
 * 2. Transformation : Standardisation des schémas (gestion des colonnes manquantes)
 * et nettoyage des données (valeurs négatives, nulls).
 * 3. Chargement (Multi-branche) :
 * - Branche 1 : Stockage Data Lake (MinIO) pour le Machine Learning.
 * - Branche 2 : Stockage Data Warehouse (PostgreSQL) pour la BI.
 * =================================================================================
 */

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
      // Important : permet de gérer "airport_fee" et "Airport_fee" sans erreur immédiate
      .config("spark.sql.caseSensitive", "false")
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
      /*
       * Stratégie : "Divide & Conquer"
       * Au lieu de lire tous les fichiers d'un coup (ce qui ferait planter Spark si les types changent),
       * on lit chaque fichier individuellement, on force son schéma, puis on fusionne.
       */
      val dataframes = files.map { filePath =>
        println(s"--> Lecture de $filePath ...")
        val df = spark.read.parquet(filePath)
        standardizeSchema(df)
      }

      // --- 3. FUSION (UNION) ---
      println("--> Fusion des 3 mois...")
      // reduce(_ unionByName _) fusionne tous les DataFrames de la liste en un seul
      // 'unionByName' permet de fusionner même si l'ordre des colonnes diffère.
      val rawDf = dataframes.reduce(_ unionByName _)

      // --- 4. NETTOYAGE ---
      println("--> Nettoyage des données...")
      val cleanedDf = rawDf
        // Règle 1 : Cohérence physique (pas de distance négative)
        .filter(col("trip_distance") > 0)
        // Règle 1 : Cohérence physique (pas de distance négative)
        .filter(col("total_amount") >= 0)
        // Règle 1 : Cohérence physique (pas de distance négative)
        .na.fill(1, Seq("passenger_count")) // Remplace null par 1
        .filter(col("passenger_count") > 0)

      // Mise en cache car ce DataFrame va être utilisé deux fois (Branche 1 & 2)
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

      println("Terminé avec succès !")

    } catch {
      case e: Exception =>
        println("!!! ERREUR CRITIQUE !!!")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }

  /**
   * Harmonise le schéma d'un DataFrame entrant.
   * Gère les changements de types (Int vs Double) et les changements de noms (Casse).
   * * @param df Le DataFrame brut lu depuis un fichier Parquet
   * @return Un DataFrame avec un schéma strict et unifié
   */
  def standardizeSchema(df: DataFrame): DataFrame = {

    var tempDf = df

    // 1. Définition des types cibles (Target Types)
    // On utilise Double pour les montants/quantités pour éviter les erreurs de précisions
    // On utilise Long pour les Identifiants
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
      // airport_fee est géré dynamiquement car il change de case
    )
    // Application des casts si la colonne existe
    casts.foreach { case (colName, typeName) =>
      if (tempDf.columns.contains(colName)) {
        tempDf = tempDf.withColumn(colName, col(colName).cast(typeName))
      }
    }

    // 2. Gestion spécifique de 'airport_fee' (Colonne instable)
    // Cas 1 : La colonne existe sous le nom "Airport_fee" (Majuscule) -> On renomme
    if (tempDf.columns.contains("Airport_fee")) {
      tempDf = tempDf.withColumnRenamed("Airport_fee", "airport_fee")
    }
    // Cas 2 : La colonne existe (en minuscule) -> On cast en double
    if (tempDf.columns.contains("airport_fee")) {
      tempDf = tempDf.withColumn("airport_fee", col("airport_fee").cast("double"))
    } else {
      // Cas 3 : La colonne n'existe pas (ex: fichiers avant 2023) -> On crée une colonne par défaut
      // Cela permet l'unionByName sans erreur.
      tempDf = tempDf.withColumn("airport_fee", lit(0.0))
    }

    tempDf
  }
}