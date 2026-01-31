from pyspark.sql import SparkSession
from delta import *

# URL del Master
master_url = "spark://spark-master:7077"

builder = SparkSession.builder \
    .appName("Lab_SECOP_Bronze") \
    .master(master_url) \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executor.memory", "1g")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 📥 LECTURA CSV RAW
print("Leyendo CSV crudo...")

df_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/app/data/SECOP_II_Contratos_Electronicos_Q1_2025.csv")


# 🥉 ESCRITURA BRONZE (Delta Lake)
print("Escribiendo en capa Bronce...")

output_path = "/app/data/lakehouse/bronze/secop"

df_raw.write \
    .format("delta") \
    .mode("overwrite") \
    .save(output_path)

print(f"Ingesta completada. Registros procesados: {df_raw.count()}")

