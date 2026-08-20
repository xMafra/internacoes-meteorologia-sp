from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    countDistinct,
    min,
    max
)
from calendar import isleap


# ============================================================
# CONFIGURAÇÃO
# ============================================================

BASE_PATH = "/home/jovyan/work/data/silver/inmet"

ANOS = [2023, 2024, 2025]

ESTACOES_ESPERADAS = 40
UF_ESPERADA = "SP"


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("ValidacaoSilverINMET")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# VALIDAÇÃO
# ============================================================

erros_gerais = []

for ano in ANOS:

    print("\n")
    print("=" * 70)
    print(f"VALIDAÇÃO SILVER INMET {ano}")
    print("=" * 70)

    caminho = f"{BASE_PATH}/{ano}"

    try:
        df = spark.read.parquet(caminho)
    except Exception as e:
        print(f"ERRO ao ler {caminho}")
        print(e)
        erros_gerais.append(f"{ano}: erro ao ler arquivos")
        continue


    # --------------------------------------------------------
    # INFORMAÇÕES BÁSICAS
    # --------------------------------------------------------

    total = df.count()

    colunas = len(df.columns)

    estacoes = (
        df.select("codigo_estacao")
        .distinct()
        .count()
    )

    ufs = [
        row["uf"]
        for row in (
            df.select("uf")
            .distinct()
            .orderBy("uf")
            .collect()
        )
    ]

    data_min = (
        df.select(min("data_hora_utc"))
        .first()[0]
    )

    data_max = (
        df.select(max("data_hora_utc"))
        .first()[0]
    )


    # --------------------------------------------------------
    # NULOS
    # --------------------------------------------------------

    sem_data = (
        df.filter(col("data_hora_utc").isNull())
        .count()
    )

    sem_estacao = (
        df.filter(col("codigo_estacao").isNull())
        .count()
    )

    sem_latitude = (
        df.filter(col("latitude").isNull())
        .count()
    )

    sem_longitude = (
        df.filter(col("longitude").isNull())
        .count()
    )


    # --------------------------------------------------------
    # DUPLICIDADES
    # --------------------------------------------------------

    duplicidades = (
        df.groupBy(
            "codigo_estacao",
            "data_hora_utc"
        )
        .count()
        .filter(col("count") > 1)
        .count()
    )


    # --------------------------------------------------------
    # QUANTIDADE ESPERADA
    #
    # 2024 possui 366 dias.
    # 2023 e 2025 possuem 365.
    # --------------------------------------------------------

    dias_ano = 366 if isleap(ano) else 365

    registros_por_estacao_esperados = dias_ano * 24

    registros_esperados = (
        registros_por_estacao_esperados
        * ESTACOES_ESPERADAS
    )


    # --------------------------------------------------------
    # RESULTADO PRINCIPAL
    # --------------------------------------------------------

    print(f"Registros: {total}")
    print(f"Registros esperados: {registros_esperados}")

    print(f"Colunas: {colunas}")

    print(f"Estações distintas: {estacoes}")
    print(f"Estações esperadas: {ESTACOES_ESPERADAS}")

    print(f"UFs: {ufs}")

    print(f"Data mínima: {data_min}")
    print(f"Data máxima: {data_max}")

    print(f"Sem data/hora: {sem_data}")
    print(f"Sem estação: {sem_estacao}")
    print(f"Sem latitude: {sem_latitude}")
    print(f"Sem longitude: {sem_longitude}")

    print(f"Duplicidades estação/data: {duplicidades}")


    # --------------------------------------------------------
    # REGISTROS POR ESTAÇÃO
    # --------------------------------------------------------

    print("\nRegistros por estação:")

    registros_estacao = (
        df.groupBy("codigo_estacao")
        .count()
        .orderBy("codigo_estacao")
    )

    registros_estacao.show(
        ESTACOES_ESPERADAS,
        False
    )


    # --------------------------------------------------------
    # VERIFICAÇÃO DA QUANTIDADE POR ESTAÇÃO
    # --------------------------------------------------------

    estacoes_incorretas = (
        registros_estacao
        .filter(
            col("count") != registros_por_estacao_esperados
        )
        .count()
    )

    print(
        f"Estações com quantidade inesperada: "
        f"{estacoes_incorretas}"
    )


    # --------------------------------------------------------
    # SCHEMA
    # --------------------------------------------------------

    print("\nSchema:")

    df.printSchema()


    # --------------------------------------------------------
    # AMOSTRA
    # --------------------------------------------------------

    print("\nAmostra:")

    (
        df.select(
            "ano",
            "data_hora_utc",
            "codigo_estacao",
            "estacao",
            "latitude",
            "longitude",
            "precipitacao_mm",
            "temperatura_c",
            "umidade_pct",
            "vento_velocidade_ms"
        )
        .orderBy(
            "codigo_estacao",
            "data_hora_utc"
        )
        .show(10, False)
    )


    # ========================================================
    # VALIDAÇÕES
    # ========================================================

    erros_ano = []


    # Quantidade total
    if total != registros_esperados:
        erros_ano.append(
            f"Quantidade de registros inesperada: "
            f"{total} "
            f"(esperado: {registros_esperados})"
        )


    # Quantidade de estações
    if estacoes != ESTACOES_ESPERADAS:
        erros_ano.append(
            f"Quantidade de estações inesperada: "
            f"{estacoes} "
            f"(esperado: {ESTACOES_ESPERADAS})"
        )


    # UF
    if ufs != [UF_ESPERADA]:
        erros_ano.append(
            f"UF inesperada: {ufs} "
            f"(esperado: ['{UF_ESPERADA}'])"
        )


    # Datas nulas
    if sem_data != 0:
        erros_ano.append(
            f"Existem {sem_data} registros "
            f"sem data/hora"
        )


    # Estações nulas
    if sem_estacao != 0:
        erros_ano.append(
            f"Existem {sem_estacao} registros "
            f"sem código da estação"
        )


    # Latitude nula
    if sem_latitude != 0:
        erros_ano.append(
            f"Existem {sem_latitude} registros "
            f"sem latitude"
        )


    # Longitude nula
    if sem_longitude != 0:
        erros_ano.append(
            f"Existem {sem_longitude} registros "
            f"sem longitude"
        )


    # Duplicidades
    if duplicidades != 0:
        erros_ano.append(
            f"Existem {duplicidades} "
            f"duplicidades estação/data"
        )


    # Quantidade por estação
    if estacoes_incorretas != 0:
        erros_ano.append(
            f"Existem {estacoes_incorretas} "
            f"estações com quantidade de registros inesperada"
        )


    # --------------------------------------------------------
    # RESULTADO DO ANO
    # --------------------------------------------------------

    if len(erros_ano) == 0:

        print("\n")
        print("=" * 70)
        print(f"SILVER INMET {ano} VALIDADA COM SUCESSO")
        print("=" * 70)

    else:

        print("\n")
        print("!!! PROBLEMAS ENCONTRADOS !!!")

        for erro in erros_ano:
            print(f"- {erro}")

        erros_gerais.extend(
            [f"{ano}: {erro}" for erro in erros_ano]
        )


# ============================================================
# RESULTADO FINAL
# ============================================================

print("\n")
print("=" * 70)
print("RESULTADO FINAL DA VALIDAÇÃO")
print("=" * 70)

if len(erros_gerais) == 0:

    print("TODOS OS ANOS FORAM VALIDADOS COM SUCESSO")
    print(f"Anos validados: {ANOS}")

else:

    print("FORAM ENCONTRADOS PROBLEMAS:")
    print()

    for erro in erros_gerais:
        print(f"- {erro}")


# ============================================================
# ENCERRAMENTO
# ============================================================

spark.stop()