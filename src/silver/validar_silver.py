from pyspark.sql import SparkSession
from pyspark.sql import functions as F


CAMINHO_SILVER = (
    "/home/jovyan/work/data/silver/sih"
)


def criar_spark() -> SparkSession:
    """Cria uma sessão Spark para validação da Silver."""

    return (
        SparkSession.builder
        .appName("TCC-Validacao-Silver-SIH")
        .master("local[2]")
        .getOrCreate()
    )


def validar_silver(
    spark: SparkSession,
) -> None:
    """
    Executa as principais validações da camada Silver do SIH.

    Validações realizadas:

    - quantidade de períodos;
    - quantidade total de registros;
    - quantidade de registros por ano;
    - quantidade de registros por período;
    - schema;
    - quantidade de colunas;
    - intervalo das datas;
    - municípios distintos;
    - registros sem município;
    - registros sem UF;
    - registros sem diagnóstico principal;
    - registros com data de internação inválida.
    """

    print()
    print("=" * 70)
    print("VALIDAÇÃO DA SILVER - SIH 2023-2025")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Leitura de toda a Silver
    # ------------------------------------------------------------------

    df = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(CAMINHO_SILVER)
    )

    # ------------------------------------------------------------------
    # 2. Quantidade total de registros
    # ------------------------------------------------------------------

    total_registros = df.count()

    print()
    print("Total de registros:", total_registros)

    # ------------------------------------------------------------------
    # 3. Quantidade de colunas
    # ------------------------------------------------------------------

    total_colunas = len(df.columns)

    print(
        "Total de colunas:",
        total_colunas,
    )

    if total_colunas != 116:
        raise RuntimeError(
            f"Quantidade inesperada de colunas: "
            f"{total_colunas}. Esperado: 116."
        )

    # ------------------------------------------------------------------
    # 4. Extração de ano/mês a partir do caminho do arquivo
    # ------------------------------------------------------------------

    df = df.withColumn(
        "_arquivo",
        F.input_file_name(),
    )

    df = (
        df
        .withColumn(
            "_ano",
            F.regexp_extract(
                F.col("_arquivo"),
                r"/sih/(\d{4})/",
                1,
            ).cast("int"),
        )
        .withColumn(
            "_mes",
            F.regexp_extract(
                F.col("_arquivo"),
                r"/(\d{2})/[^/]+$",
                1,
            ).cast("int"),
        )
    )

    # ------------------------------------------------------------------
    # 5. Períodos encontrados
    # ------------------------------------------------------------------

    periodos = (
        df
        .select(
            "_ano",
            "_mes",
        )
        .distinct()
        .orderBy(
            "_ano",
            "_mes",
        )
    )

    total_periodos = periodos.count()

    print()
    print(
        "Períodos encontrados:",
        total_periodos,
    )

    if total_periodos != 36:
        raise RuntimeError(
            f"Quantidade inesperada de períodos: "
            f"{total_periodos}. Esperado: 36."
        )

    print()
    print("Períodos:")

    periodos.show(
        36,
        truncate=False,
    )

    # ------------------------------------------------------------------
    # 6. Registros por ano
    # ------------------------------------------------------------------

    print()
    print("Registros por ano:")

    (
        df
        .groupBy("_ano")
        .count()
        .orderBy("_ano")
        .show(
            10,
            truncate=False,
        )
    )

    # ------------------------------------------------------------------
    # 7. Registros por mês
    # ------------------------------------------------------------------

    print()
    print("Registros por período:")

    (
        df
        .groupBy(
            "_ano",
            "_mes",
        )
        .count()
        .orderBy(
            "_ano",
            "_mes",
        )
        .show(
            40,
            truncate=False,
        )
    )

    # ------------------------------------------------------------------
    # 8. Registros sem município
    # ------------------------------------------------------------------

    sem_municipio = (
        df
        .filter(
            F.col("NM_MUN").isNull()
        )
        .count()
    )

    print()
    print(
        "Registros sem município:",
        sem_municipio,
    )

    if sem_municipio != 0:
        raise RuntimeError(
            "Existem registros sem município "
            "na Silver."
        )

    # ------------------------------------------------------------------
    # 9. Registros sem UF
    # ------------------------------------------------------------------

    sem_uf = (
        df
        .filter(
            F.col("SIGLA_UF").isNull()
        )
        .count()
    )

    print(
        "Registros sem UF:",
        sem_uf,
    )

    if sem_uf != 0:
        raise RuntimeError(
            "Existem registros sem UF na Silver."
        )

    # ------------------------------------------------------------------
    # 10. UF encontrada
    # ------------------------------------------------------------------

    print()
    print("Distribuição por UF:")

    (
        df
        .groupBy("SIGLA_UF")
        .count()
        .orderBy(
            F.desc("count")
        )
        .show(
            20,
            truncate=False,
        )
    )

    # ------------------------------------------------------------------
    # 11. Municípios distintos
    # ------------------------------------------------------------------

    municipios_distintos = (
        df
        .select("MUNIC_RES")
        .distinct()
        .count()
    )

    print()
    print(
        "Municípios distintos:",
        municipios_distintos,
    )

    if municipios_distintos > 645:
        raise RuntimeError(
            "A Silver possui mais de 645 municípios. "
            "Verificar dados."
        )

    # ------------------------------------------------------------------
    # 12. Diagnóstico principal
    # ------------------------------------------------------------------

    sem_diagnostico = (
        df
        .filter(
            F.col("DIAG_PRINC").isNull()
        )
        .count()
    )

    print()
    print(
        "Registros sem diagnóstico principal:",
        sem_diagnostico,
    )

    # ------------------------------------------------------------------
    # 13. Datas
    # ------------------------------------------------------------------

    print()
    print("Tipos das colunas de data:")

    df.select(
        "DT_INTER",
        "DT_SAIDA",
        "NASC",
    ).printSchema()

    # ------------------------------------------------------------------
    # 14. Intervalo de internação
    # ------------------------------------------------------------------

    datas = (
        df
        .select(
            F.min("DT_INTER").alias(
                "data_minima"
            ),
            F.max("DT_INTER").alias(
                "data_maxima"
            ),
        )
        .collect()[0]
    )

    print(
        "Data mínima de internação:",
        datas["data_minima"],
    )

    print(
        "Data máxima de internação:",
        datas["data_maxima"],
    )

    # ------------------------------------------------------------------
    # 15. Datas de internação inválidas
    # ------------------------------------------------------------------

    datas_invalidas = (
        df
        .filter(
            F.col("DT_INTER").isNull()
        )
        .count()
    )

    print(
        "Registros sem data de internação:",
        datas_invalidas,
    )

    # ------------------------------------------------------------------
    # 16. Valores de UF
    # ------------------------------------------------------------------

    ufs = [
        linha["SIGLA_UF"]
        for linha in (
            df
            .select("SIGLA_UF")
            .distinct()
            .collect()
        )
    ]

    print()
    print(
        "UFs encontradas:",
        sorted(ufs),
    )

    if ufs != ["SP"]:
        raise RuntimeError(
            f"UFs inesperadas na Silver: {ufs}"
        )

    # ------------------------------------------------------------------
    # 17. Exemplo dos dados
    # ------------------------------------------------------------------

    print()
    print("Exemplos da Silver:")

    df.select(
        "MUNIC_RES",
        "NM_MUN",
        "SIGLA_UF",
        "AREA_KM2",
        "DT_INTER",
        "DT_SAIDA",
        "NASC",
        "DIAG_PRINC",
    ).show(
        10,
        truncate=False,
    )

    # ------------------------------------------------------------------
    # 18. Resumo final
    # ------------------------------------------------------------------

    print()
    print("=" * 70)
    print("RESUMO DA VALIDAÇÃO")
    print("=" * 70)

    print(
        "Períodos:",
        total_periodos,
    )

    print(
        "Registros:",
        total_registros,
    )

    print(
        "Colunas:",
        total_colunas,
    )

    print(
        "Municípios distintos:",
        municipios_distintos,
    )

    print(
        "Registros sem município:",
        sem_municipio,
    )

    print(
        "Registros sem UF:",
        sem_uf,
    )

    print(
        "Registros sem data de internação:",
        datas_invalidas,
    )

    print(
        "UFs:",
        sorted(ufs),
    )

    print("=" * 70)
    print("SILVER SIH VALIDADA COM SUCESSO")
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    spark = criar_spark()

    try:
        validar_silver(spark)

    finally:
        # O stop também é executado dentro de validar_silver.
        # Esta chamada é protegida para garantir encerramento.
        try:
            spark.stop()
        except Exception:
            pass