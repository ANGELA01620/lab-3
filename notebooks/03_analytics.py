# =========================
# LAB SECOP - ORO
# =========================
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc
from delta import configure_spark_with_delta_pip

# =========================
# 🔧 CONFIGURACIÓN SPARK + DELTA
# =========================
builder = SparkSession.builder \
    .appName("Lab_SECOP_Gold") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print("✅ Sesión Spark con Delta configurada")

# =========================
# 📥 LEER SILVER
# =========================
df_silver = spark.read.format("delta").load("/app/data/lakehouse/silver/secop")
print(f"✅ Silver leído: {df_silver.count()} registros")

# =========================
# AGREGACIÓN (TOP 10 DEPARTAMENTOS POR CONTRATO)
# =========================
df_gold = df_silver \
    .groupBy("departamento") \
    .agg(sum("valor_del_contrato").alias("total_contratado")) \
    .orderBy(desc("total_contratado")) \
    .limit(10)

# =========================
# PERSISTIR ORO
# =========================
df_gold.write.format("delta").mode("overwrite").save("/app/data/lakehouse/gold/top_deptos")
print("✅ Capa Oro generada correctamente")

# =========================
# VISUALIZAR
# =========================
print("Top 10 Departamentos por contratación:")
df_pandas = df_gold.toPandas()
print(df_pandas)
