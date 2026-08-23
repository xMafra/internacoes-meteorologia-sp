from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min,
    max,
    avg,
    sum,
    when
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

CAMINHO_GOLD = (
    "/home/jovyan/work/data/gold/municipio_estacao"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("TCC-Teste-Gold-Municipio-Estacao")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# LEITURA
# ============================================================

df = spark.read.parquet(
    CAMINHO_GOLD
)


# ============================================================
# TESTE GERAL
# ============================================================

print("=" * 70)
print("TESTE GOLD MUNICÍPIO → ESTAÇÃO")
print("=" * 70)

total_registros = df.count()

municipios_distintos = (
    df.select("CD_MUN")
    .distinct()
    .count()
)

estacoes_distintas = (
    df.select("codigo_estacao")
    .distinct()
    .count()
)

print(f"Registros: {total_registros}")
print(f"Municípios distintos: {municipios_distintos}")
print(f"Estações utilizadas: {estacoes_distintas}")


# ============================================================
# SCHEMA
# ============================================================

print("\n" + "=" * 70)
print("SCHEMA")
print("=" * 70)

df.printSchema()


# ============================================================
# VALIDAÇÃO DE NULOS
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DE NULOS")
print("=" * 70)

colunas_validar = [
    "CD_MUN",
    "NM_MUN",
    "SIGLA_UF",
    "latitude_municipio",
    "longitude_municipio",
    "codigo_estacao",
    "estacao",
    "latitude_estacao",
    "longitude_estacao",
    "distancia_km"
]

for coluna in colunas_validar:

    quantidade_nulos = (
        df.filter(
            col(coluna).isNull()
        ).count()
    )

    print(
        f"{coluna}: {quantidade_nulos} nulos"
    )


# ============================================================
# VALIDAÇÃO DA DISTÂNCIA
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DA DISTÂNCIA")
print("=" * 70)

distancias_invalidas = (
    df.filter(
        (col("distancia_km").isNull())
        | (col("distancia_km") < 0)
    )
    .count()
)

print(
    f"Distâncias inválidas: {distancias_invalidas}"
)


# ============================================================
# ESTATÍSTICAS DE DISTÂNCIA
# ============================================================

print("\n" + "=" * 70)
print("ESTATÍSTICAS DE DISTÂNCIA")
print("=" * 70)

estatisticas = (
    df.select(
        min("distancia_km").alias("minima_km"),
        max("distancia_km").alias("maxima_km"),
        avg("distancia_km").alias("media_km")
    )
    .collect()[0]
)

print(
    f"Distância mínima: {estatisticas['minima_km']:.2f} km"
)

print(
    f"Distância máxima: {estatisticas['maxima_km']:.2f} km"
)

print(
    f"Distância média: {estatisticas['media_km']:.2f} km"
)


# ============================================================
# ESTAÇÕES MAIS UTILIZADAS
# ============================================================

print("\n" + "=" * 70)
print("ESTAÇÕES MAIS UTILIZADAS")
print("=" * 70)

(
    df.groupBy(
        "codigo_estacao",
        "estacao"
    )
    .count()
    .orderBy(
        col("count").desc()
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# MAIORES DISTÂNCIAS
# ============================================================

print("\n" + "=" * 70)
print("10 MAIORES DISTÂNCIAS")
print("=" * 70)

(
    df.select(
        "CD_MUN",
        "NM_MUN",
        "codigo_estacao",
        "estacao",
        "distancia_km"
    )
    .orderBy(
        col("distancia_km").desc()
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# MENORES DISTÂNCIAS
# ============================================================

print("\n" + "=" * 70)
print("10 MENORES DISTÂNCIAS")
print("=" * 70)

(
    df.select(
        "CD_MUN",
        "NM_MUN",
        "codigo_estacao",
        "estacao",
        "distancia_km"
    )
    .orderBy(
        col("distancia_km").asc()
    )
    .show(
        10,
        truncate=False
    )
)


# ============================================================
# FINAL
# ============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO CONCLUÍDA")
print("=" * 70)

spark.stop()