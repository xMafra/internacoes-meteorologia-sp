from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(
    "/home/jovyan/work/data/silver/ibge/2022"
)

print("===== TESTE ENCODING IBGE =====")

df.filter(df.NM_MUN == "Marília") \
    .select(
        "CD_MUN",
        "CD_MUN_6",
        "NM_MUN",
        "SIGLA_UF"
    ) \
    .show(truncate=False)

spark.stop()