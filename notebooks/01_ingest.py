from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# -------------------------------------------------
# CONFIGURACIÓN SPARK + DELTA
# -------------------------------------------------
master_url = "spark://spark-master:7077"

builder = (
    SparkSession.builder
    .appName("Lab_SECOP_Bronze")
    .master(master_url)
    .config("spark.executor.memory", "1g")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("🔥 Spark iniciado correctamente con Delta Lake")

# -------------------------------------------------
# RUTAS
# -------------------------------------------------
input_path = "/app/data/SECOP_II_Contratos_Electronicos_Q1_2025.csv"
output_path = "/app/data/lakehouse/bronze/secop"

# -------------------------------------------------
# 📥 LECTURA CSV RAW (BRONZE SIN TIPADO)
# -------------------------------------------------
print("📥 Leyendo CSV crudo...")

df_raw = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "false")  # 🔑 todo como STRING
    .load(input_path)
)

print(f"✅ Registros leídos: {df_raw.count()}")

# -------------------------------------------------
# 🥉 ESCRITURA BRONZE (DELTA)
# -------------------------------------------------
print("📦 Escribiendo capa Bronze (Delta Lake)...")

(
    df_raw.write
    .format("delta")
    .mode("overwrite")
    .save(output_path)
)

print(f"✅ Ingesta Bronze completada. Registros procesados: {df_raw.count()}")

spark.stop()
