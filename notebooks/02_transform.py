# =========================
# LAB SECOP - SILVER
# =========================

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, year, quarter
from delta import configure_spark_with_delta_pip

# =========================
# 🔧 CONFIGURACIÓN SPARK + DELTA
# =========================
master_url = "spark://spark-master:7077"

builder = SparkSession.builder \
    .appName("Lab_SECOP_Silver_QualityGate") \
    .master(master_url) \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.executor.memory", "1g")

# Crear sesión Spark con Delta
spark = configure_spark_with_delta_pip(builder).getOrCreate()
print("✅ Sesión Spark con Delta configurada")

# =========================
# 📥 LEER BRONZE COMO PARQUET
# =========================
bronze_path = "/app/data/lakehouse/bronze/secop"

df_bronze = spark.read.parquet(bronze_path)
print("✅ Bronze leído como Parquet")
print(f"Registros en Bronze: {df_bronze.count()}")

# =========================
# CONVERTIR A DELTA LOCAL (opcional pero recomendado)
# =========================
bronze_delta_path = "/app/data/lakehouse/bronze/secop_delta"

df_bronze.write.format("delta").mode("overwrite").save(bronze_delta_path)
df_bronze = spark.read.format("delta").load(bronze_delta_path)
print("✅ Bronze convertido a Delta y cargado")

# =========================
# 🚦 QUALITY GATE
# =========================
df_quality = df_bronze.withColumn(
    "motivo_rechazo",
    when(col("valor_del_contrato") <= 0, "VALOR_CONTRATO_INVALIDO")
    .when(col("fecha_de_firma").isNull(), "FECHA_FIRMA_NULA")
)

# -------------------------
# ❌ Registros inválidos (cuarentena)
# -------------------------
quarantine_path = "/app/data/lakehouse/quarantine/secop_errors"
df_quarantine = df_quality.filter(col("motivo_rechazo").isNotNull())

df_quarantine.write \
    .format("delta") \
    .mode("overwrite") \
    .save(quarantine_path)

print(f"⚠️ Registros enviados a cuarentena: {df_quarantine.count()}")

# -------------------------
# ✅ Registros válidos (Silver)
# -------------------------
silver_path = "/app/data/lakehouse/silver/secop"

df_silver = (
    df_quality
    .filter(col("motivo_rechazo").isNull())
    .withColumn("anio_firma", year(col("fecha_de_firma")))
    .withColumn("trimestre_firma", quarter(col("fecha_de_firma")))
    .select(
        "nombre_entidad",
        "departamento",
        "ciudad",
        "valor_del_contrato",
        "fecha_de_firma",
        "anio_firma",
        "trimestre_firma"
    )
)

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .save(silver_path)

print("✅ Capa Silver generada correctamente")
print(f"✔️ Registros válidos: {df_silver.count()}")
