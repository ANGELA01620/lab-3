from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, desc

# ===============================
# CONFIGURACIÓN SPARK (SIN DELTA)
# ===============================
spark = (
    SparkSession.builder
    .appName("SECOP Analytics - Gold")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")
print("✅ Spark iniciado correctamente")

# ===============================
# RUTA SILVER (PARQUET)
# ===============================
silver_path = "/app/data/lakehouse/silver/secop"

# ===============================
# LECTURA SILVER
# ===============================
df_silver = spark.read.parquet(silver_path)
print("📊 Registros leídos:", df_silver.count())

# ===============================
# 🥇 CAPA GOLD
# TOP 10 ENTIDADES POR MONTO CONTRATADO
# ===============================
df_gold = (
    df_silver
        .groupBy("nombre_entidad")
        .agg(sum("valor_del_contrato").alias("total_contratado"))
        .orderBy(desc("total_contratado"))
        .limit(10)
)

# ===============================
# MOSTRAR RESULTADO
# ===============================
print("🏆 Top 10 Entidades por contratación:")
df_gold.show(truncate=False)

# ===============================
# CIERRE
# ===============================
spark.stop()
