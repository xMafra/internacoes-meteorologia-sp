from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    radians,
    sin,
    cos,
    asin,
    sqrt,
    lit,
    row_number
)
from pyspark.sql.window import Window


# ============================================================
# CONFIGURAÇÕES
# ============================================================

CAMINHO_SILVER_IBGE = (
    "/home/jovyan/work/data/silver/ibge/2022"
)

CAMINHO_SILVER_INMET = (
    "/home/jovyan/work/data/silver/inmet"
)

CAMINHO_GOLD = (
    "/home/jovyan/work/data/gold/municipio_estacao"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("TCC-Gold-Municipio-Estacao")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("INICIANDO GOLD MUNICÍPIO → ESTAÇÃO")
print("=" * 70)


# ============================================================
# LEITURA SILVER IBGE
# ============================================================

print("\nLendo Silver IBGE...")

df_ibge = spark.read.parquet(
    CAMINHO_SILVER_IBGE
)

print(
    f"Municípios IBGE: {df_ibge.count()}"
)


# ============================================================
# LEITURA SILVER INMET
# ============================================================

print("\nLendo Silver INMET...")

df_inmet_2023 = (
    spark.read
    .parquet(
        f"{CAMINHO_SILVER_INMET}/2023"
    )
    .select(
        "codigo_estacao",
        "estacao",
        "latitude",
        "longitude"
    )
)

df_inmet_2024 = (
    spark.read
    .parquet(
        f"{CAMINHO_SILVER_INMET}/2024"
    )
    .select(
        "codigo_estacao",
        "estacao",
        "latitude",
        "longitude"
    )
)

df_inmet_2025 = (
    spark.read
    .parquet(
        f"{CAMINHO_SILVER_INMET}/2025"
    )
    .select(
        "codigo_estacao",
        "estacao",
        "latitude",
        "longitude"
    )
)

df_inmet = (
    df_inmet_2023
    .unionByName(df_inmet_2024)
    .unionByName(df_inmet_2025)
)


# ============================================================
# ESTAÇÕES INMET
# ============================================================

df_estacoes = (
    df_inmet
    .where(
        col("codigo_estacao").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
    )
    .dropDuplicates(["codigo_estacao"])
    .select(
        "codigo_estacao",
        "estacao",
        col("latitude").alias("latitude_estacao"),
        col("longitude").alias("longitude_estacao")
    )
)

print(
    f"Estações INMET: {df_estacoes.count()}"
)


# ============================================================
# MUNICÍPIOS IBGE
# ============================================================

df_municipios = (
    df_ibge
    .select(
        "CD_MUN",
        "NM_MUN",
        "SIGLA_UF",
        "AREA_KM2",
        col("latitude").alias("latitude_municipio"),
        col("longitude").alias("longitude_municipio")
    )
    .where(
        col("latitude_municipio").isNotNull()
        & col("longitude_municipio").isNotNull()
    )
)


# ============================================================
# CROSS JOIN
# ============================================================

print(
    "\nCalculando distância entre municípios e estações..."
)

df_combinacoes = (
    df_municipios
    .crossJoin(df_estacoes)
)


# ============================================================
# CÁLCULO HAVERSINE
# ============================================================

RAIO_TERRA_KM = 6371.0


delta_lat = radians(
    col("latitude_estacao")
    - col("latitude_municipio")
)

delta_lon = radians(
    col("longitude_estacao")
    - col("longitude_municipio")
)

lat_municipio = radians(
    col("latitude_municipio")
)

lat_estacao = radians(
    col("latitude_estacao")
)


a = (
    sin(delta_lat / 2) ** 2
    +
    cos(lat_municipio)
    * cos(lat_estacao)
    * sin(delta_lon / 2) ** 2
)


distancia = (
    2
    * lit(RAIO_TERRA_KM)
    * asin(sqrt(a))
)


df_distancias = (
    df_combinacoes
    .withColumn(
        "distancia_km",
        distancia
    )
)


# ============================================================
# SELECIONAR ESTAÇÃO MAIS PRÓXIMA
# ============================================================

window = (
    Window
    .partitionBy("CD_MUN")
    .orderBy(
        col("distancia_km").asc()
    )
)


df_gold = (
    df_distancias
    .withColumn(
        "ranking",
        row_number().over(window)
    )
    .filter(
        col("ranking") == 1
    )
    .drop("ranking")
)


# ============================================================
# SCHEMA FINAL GOLD
# ============================================================

df_gold = df_gold.select(
    "CD_MUN",
    "NM_MUN",
    "SIGLA_UF",
    "AREA_KM2",
    "latitude_municipio",
    "longitude_municipio",
    "codigo_estacao",
    "estacao",
    "latitude_estacao",
    "longitude_estacao",
    "distancia_km"
)


# ============================================================
# GRAVAÇÃO
# ============================================================

print("\nGravando Gold...")

(
    df_gold
    .write
    .mode("overwrite")
    .parquet(CAMINHO_GOLD)
)


# ============================================================
# RESUMO
# ============================================================

print("\n" + "=" * 70)
print("RESUMO GOLD MUNICÍPIO → ESTAÇÃO")
print("=" * 70)

print(
    f"Municípios: {df_gold.count()}"
)

print(
    f"Colunas: {df_gold.columns}"
)

print("\nAmostra:")

(
    df_gold
    .orderBy("CD_MUN")
    .show(
        10,
        truncate=False
    )
)

print(
    f"\nSaída: {CAMINHO_GOLD}"
)

print("=" * 70)


# ============================================================
# FINALIZAÇÃO
# ============================================================

spark.stop()