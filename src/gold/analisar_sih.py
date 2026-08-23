from pyspark.sql import SparkSession
from pyspark.sql import functions as F


# ============================================================
# CONFIGURAÇÕES
# ============================================================

CAMINHO_SILVER = (
    "/home/jovyan/work/data/silver/sih"
)


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("TCC-Analise-SIH")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("=" * 70)
print("ANÁLISE SIH PARA MODELAGEM GOLD")
print("=" * 70)


# ============================================================
# LEITURA
# ============================================================

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(CAMINHO_SILVER)
)

total_registros = df.count()

print(f"\nTotal de registros: {total_registros}")
print(f"Total de colunas: {len(df.columns)}")


# ============================================================
# COLUNAS DISPONÍVEIS
# ============================================================

print("\n" + "=" * 70)
print("COLUNAS DISPONÍVEIS")
print("=" * 70)

for coluna in df.columns:
    print(f" - {coluna}")


# ============================================================
# MUNICÍPIOS
# ============================================================

print("\n" + "=" * 70)
print("MUNICÍPIOS")
print("=" * 70)

for coluna in ["MUNIC_RES", "CD_MUN", "NM_MUN", "SIGLA_UF"]:

    if coluna in df.columns:

        print(f"\n{coluna}")

        df.select(coluna).show(
            10,
            truncate=False
        )

        print(
            f"Valores distintos: "
            f"{df.select(coluna).distinct().count()}"
        )


# ============================================================
# DISTRIBUIÇÃO TEMPORAL
# ============================================================

print("\n" + "=" * 70)
print("DISTRIBUIÇÃO TEMPORAL")
print("=" * 70)


if "DT_INTER" in df.columns:

    df_temporal = (
        df
        .filter(F.col("DT_INTER").isNotNull())
        .withColumn(
            "ANO",
            F.year("DT_INTER")
        )
        .withColumn(
            "MES",
            F.month("DT_INTER")
        )
    )

    print("\nInternações por ano:")

    (
        df_temporal
        .groupBy("ANO")
        .count()
        .orderBy("ANO")
        .show(
            20,
            truncate=False
        )
    )

    print("\nInternações por ano e mês:")

    (
        df_temporal
        .groupBy("ANO", "MES")
        .count()
        .orderBy("ANO", "MES")
        .show(
            100,
            truncate=False
        )
    )

    print("\nPeríodo:")

    periodo = (
        df.select(
            F.min("DT_INTER").alias("data_minima"),
            F.max("DT_INTER").alias("data_maxima")
        )
        .collect()[0]
    )

    print(
        f"Data mínima: {periodo['data_minima']}"
    )

    print(
        f"Data máxima: {periodo['data_maxima']}"
    )


# ============================================================
# INTERNAÇÕES POR MUNICÍPIO
# ============================================================

print("\n" + "=" * 70)
print("INTERNAÇÕES POR MUNICÍPIO")
print("=" * 70)


if "MUNIC_RES" in df.columns:

    print("\n10 municípios com mais registros:")

    (
        df
        .groupBy("MUNIC_RES")
        .count()
        .orderBy(
            F.desc("count")
        )
        .show(
            10,
            truncate=False
        )
    )


# ============================================================
# DIAGNÓSTICOS
# ============================================================

print("\n" + "=" * 70)
print("DIAGNÓSTICOS")
print("=" * 70)


if "DIAG_PRINC" in df.columns:

    diagnosticos_distintos = (
        df
        .filter(F.col("DIAG_PRINC").isNotNull())
        .select("DIAG_PRINC")
        .distinct()
        .count()
    )

    print(
        f"Diagnósticos principais distintos: "
        f"{diagnosticos_distintos}"
    )

    print("\n10 diagnósticos mais frequentes:")

    (
        df
        .filter(F.col("DIAG_PRINC").isNotNull())
        .groupBy("DIAG_PRINC")
        .count()
        .orderBy(
            F.desc("count")
        )
        .show(
            10,
            truncate=False
        )
    )


# ============================================================
# CAMPOS RELACIONADOS A ÓBITO
# ============================================================

print("\n" + "=" * 70)
print("CAMPOS RELACIONADOS A ÓBITO")
print("=" * 70)

palavras_obito = [
    "OBITO",
    "ÓBITO",
    "MORTE",
    "MORT"
]

colunas_obito = [
    coluna
    for coluna in df.columns
    if any(
        palavra in coluna.upper()
        for palavra in palavras_obito
    )
]

if colunas_obito:

    print("Colunas encontradas:")

    for coluna in colunas_obito:
        print(f" - {coluna}")

        (
            df
            .select(coluna)
            .groupBy(coluna)
            .count()
            .orderBy(
                F.desc("count")
            )
            .show(
                10,
                truncate=False
            ))

else:

    print(
        "Nenhuma coluna explicitamente relacionada "
        "a óbito foi encontrada."
    )


# ============================================================
# CAMPOS POTENCIALMENTE ÚTEIS PARA AGREGAÇÃO
# ============================================================

print("\n" + "=" * 70)
print("CAMPOS POTENCIALMENTE ÚTEIS")
print("=" * 70)

palavras_chave = [
    "DIAG",
    "CID",
    "PROC",
    "PROCED",
    "VAL",
    "IDADE",
    "SEXO",
    "MORTE",
    "OBITO",
    "ÓBITO",
    "MUNIC",
    "DT_"
]

for coluna in df.columns:

    if any(
        palavra in coluna.upper()
        for palavra in palavras_chave
    ):

        print(
            f" - {coluna}"
        )


# ============================================================
# NULOS DOS CAMPOS MAIS IMPORTANTES
# ============================================================

print("\n" + "=" * 70)
print("NULOS — CAMPOS IMPORTANTES")
print("=" * 70)

campos_importantes = [
    "MUNIC_RES",
    "CD_MUN",
    "NM_MUN",
    "SIGLA_UF",
    "DT_INTER",
    "DT_SAIDA",
    "DIAG_PRINC",
    "IDADE",
    "SEXO"
]

for coluna in campos_importantes:

    if coluna in df.columns:

        nulos = (
            df
            .filter(F.col(coluna).isNull())
            .count()
        )

        percentual = (
            nulos / total_registros * 100
            if total_registros > 0
            else 0
        )

        print(
            f"{coluna}: "
            f"{nulos} nulos "
            f"({percentual:.2f}%)"
        )


# ============================================================
# FINAL
# ============================================================

print("\n" + "=" * 70)
print("ANÁLISE CONCLUÍDA")
print("=" * 70)

spark.stop()
