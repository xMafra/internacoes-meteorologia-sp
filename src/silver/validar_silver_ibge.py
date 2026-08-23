from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct


# ============================================================
# CONFIGURAÇÃO SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("Teste Silver IBGE")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# CAMINHO DA SILVER
# ============================================================

CAMINHO_SILVER = "/home/jovyan/work/data/silver/ibge/2022"


# ============================================================
# LEITURA
# ============================================================

df = spark.read.parquet(CAMINHO_SILVER)


# ============================================================
# VALIDAÇÕES
# ============================================================

print("=" * 70)
print("TESTE SILVER IBGE")
print("=" * 70)

print(f"Registros: {df.count()}")
print(f"Municípios distintos: {df.select('CD_MUN').distinct().count()}")
print(f"Colunas: {df.columns}")

print("\n" + "=" * 70)
print("SCHEMA")
print("=" * 70)

df.printSchema()


# ============================================================
# VALIDAÇÃO DE LATITUDE E LONGITUDE
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO LATITUDE/LONGITUDE")
print("=" * 70)

print(
    "Latitude nula:",
    df.filter(col("latitude").isNull()).count()
)

print(
    "Longitude nula:",
    df.filter(col("longitude").isNull()).count()
)

print(
    "Latitude distinta:",
    df.select("latitude").distinct().count()
)

print(
    "Longitude distinta:",
    df.select("longitude").distinct().count()
)


# ============================================================
# VALIDAÇÃO DOS MUNICÍPIOS
# ============================================================

print("\n" + "=" * 70)
print("AMOSTRA DOS MUNICÍPIOS")
print("=" * 70)

(
    df.select(
        "CD_MUN",
        "CD_MUN_6",
        "NM_MUN",
        "SIGLA_UF",
        "latitude",
        "longitude",
        "AREA_KM2"
    )
    .orderBy("CD_MUN")
    .show(10, truncate=False)
)


# ============================================================
# TESTE ESPECÍFICO: MARÍLIA
# ============================================================

print("\n" + "=" * 70)
print("TESTE MARÍLIA")
print("=" * 70)

(
    df.filter(col("NM_MUN").contains("Mar"))
    .select(
        "CD_MUN",
        "NM_MUN",
        "SIGLA_UF",
        "latitude",
        "longitude"
    )
    .show(20, truncate=False)
)


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

total = df.count()
municipios = df.select("CD_MUN").distinct().count()
lat_nulas = df.filter(col("latitude").isNull()).count()
lon_nulas = df.filter(col("longitude").isNull()).count()

print("\n" + "=" * 70)
print("RESULTADO")
print("=" * 70)

if (
    total == 645
    and municipios == 645
    and lat_nulas == 0
    and lon_nulas == 0
):
    print("SILVER IBGE VALIDADA COM SUCESSO")
    print("645 municípios presentes")
    print("Latitude preenchida")
    print("Longitude preenchida")
else:
    print("!!! PROBLEMAS ENCONTRADOS !!!")

    if total != 645:
        print(f"- Quantidade de registros: {total}")

    if municipios != 645:
        print(f"- Municípios distintos: {municipios}")

    if lat_nulas > 0:
        print(f"- Latitude nula em {lat_nulas} registros")

    if lon_nulas > 0:
        print(f"- Longitude nula em {lon_nulas} registros")


# ============================================================
# ENCERRAMENTO
# ============================================================

spark.stop()