# ==============================================================
# SILVER INMET DIÁRIO
# ==============================================================
#
# Granularidade:
#   1 registro = 1 estação meteorológica por dia
#
# Entrada:
#   /home/jovyan/work/data/silver/inmet
#
# Saída:
#   /home/jovyan/work/data/silver/inmet_diario
#
# Período:
#   2023–2025
# ==============================================================

from pyspark.sql import SparkSession
from pyspark.sql.window import Window

from pyspark.sql.functions import (
    col,
    to_date,
    count,
    sum as spark_sum,
    avg,
    min as spark_min,
    max as spark_max,
    round as spark_round,
    row_number,
    trim,
)


# ==============================================================
# CONFIGURAÇÕES
# ==============================================================

BASE_PATH = "/home/jovyan/work/data"

SILVER_INMET = (
    f"{BASE_PATH}/silver/inmet"
)

SILVER_INMET_DIARIO = (
    f"{BASE_PATH}/silver/inmet_diario"
)


# ==============================================================
# SPARK
# ==============================================================

spark = (
    SparkSession.builder
    .appName("Silver_INMET_Diario")
    .config("spark.sql.shuffle.partitions", "40")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

# Reduz drasticamente os logs do Spark
spark.sparkContext.setLogLevel("ERROR")


# ==============================================================
# CABEÇALHO
# ==============================================================

print("=" * 60)
print("INICIANDO SILVER INMET DIÁRIO")
print("=" * 60)


# ==============================================================
# LEITURA
# ==============================================================

print("\nLendo Silver INMET...")

# A Silver horaria possui uma raiz independente por ano e cada uma
# delas e particionada por codigo_estacao. O recursiveFileLookup le os
# arquivos, mas desativa a descoberta das colunas de particao no Spark;
# por isso codigo_estacao desaparecia do schema. A leitura separada
# preserva a coluna presente nos diretorios codigo_estacao=A701, por
# exemplo, e a uniao recompõe o periodo completo.
dfs_inmet_por_ano = [
    (
        spark.read
        .option("mergeSchema", "true")
        .parquet(f"{SILVER_INMET}/{ano}")
    )
    for ano in range(2023, 2026)
]

df_inmet = dfs_inmet_por_ano[0]

for df_ano in dfs_inmet_por_ano[1:]:
    df_inmet = df_inmet.unionByName(
        df_ano,
        allowMissingColumns=True,
    )

total_horario = df_inmet.count()

print(
    f"Registros horários encontrados: "
    f"{total_horario}"
)


# ==============================================================
# VALIDAÇÃO DO SCHEMA
# ==============================================================

colunas_obrigatorias = [
    "data_hora_utc",
    "codigo_estacao",
    "estacao",
    "latitude",
    "longitude",
    "precipitacao_mm",
    "temperatura_c",
    "umidade_pct",
    "vento_velocidade_ms",
]

colunas_faltantes = [
    coluna
    for coluna in colunas_obrigatorias
    if coluna not in df_inmet.columns
]

if colunas_faltantes:

    print("\nERRO: colunas obrigatórias ausentes:")

    for coluna in colunas_faltantes:
        print(f"- {coluna}")

    print("\nSchema encontrado:")
    df_inmet.printSchema()

    raise ValueError(
        "A Silver INMET não possui todas "
        "as colunas necessárias."
    )

print("✓ Schema validado.")


# ==============================================================
# PREPARAÇÃO
# ==============================================================

print("\nPreparando dados...")

df_inmet = (
    df_inmet

    # ----------------------------------------------------------
    # Código oficial da estação
    # ----------------------------------------------------------

    .withColumn(
        "codigo_estacao",
        trim(
            col("codigo_estacao")
            .cast("string")
        )
    )

    # ----------------------------------------------------------
    # Nome da estação
    # ----------------------------------------------------------

    .withColumn(
        "estacao",
        trim(
            col("estacao")
            .cast("string")
        )
    )

    # ----------------------------------------------------------
    # Coordenadas
    # ----------------------------------------------------------

    .withColumn(
        "latitude",
        col("latitude").cast("double")
    )

    .withColumn(
        "longitude",
        col("longitude").cast("double")
    )

    # ----------------------------------------------------------
    # Data
    # ----------------------------------------------------------

    .withColumn(
        "data",
        to_date(
            col("data_hora_utc")
        )
    )
)


# ==============================================================
# VALIDAÇÃO DE DATAS
# ==============================================================

nulos_data = (
    df_inmet
    .filter(
        col("data").isNull()
    )
    .count()
)

if nulos_data > 0:

    raise ValueError(
        f"Existem {nulos_data} registros "
        "sem data válida."
    )


# ==============================================================
# FILTRO 2023–2025
# ==============================================================

print("Filtrando período 2023–2025...")

df_inmet = (
    df_inmet
    .filter(
        (col("data") >= "2023-01-01")
        &
        (col("data") <= "2025-12-31")
    )
)

total_periodo = df_inmet.count()

print(
    f"Registros no período: "
    f"{total_periodo}"
)


# ==============================================================
# VALIDAÇÃO DOS CÓDIGOS DE ESTAÇÃO
# ==============================================================

print("\nValidando códigos de estação...")

estacoes_invalidas = (
    df_inmet
    .filter(
        col("codigo_estacao").isNull()
        |
        (col("codigo_estacao") == "")
    )
    .count()
)

if estacoes_invalidas > 0:

    print(
        f"ERRO: {estacoes_invalidas} "
        "registros sem código de estação."
    )

    raise ValueError(
        "Existem registros sem "
        "codigo_estacao."
    )

print("✓ Todos os registros possuem código de estação.")


# ==============================================================
# ESTAÇÕES
# ==============================================================

total_estacoes = (
    df_inmet
    .select(
        "codigo_estacao"
    )
    .distinct()
    .count()
)

print(
    f"Estações meteorológicas: "
    f"{total_estacoes}"
)


# ==============================================================
# VALIDAÇÃO DE IDENTIDADE DAS ESTAÇÕES
# ==============================================================

print(
    "\nValidando identificação das estações..."
)

estacoes_sem_nome = (
    df_inmet
    .filter(
        col("estacao").isNull()
        |
        (col("estacao") == "")
    )
    .count()
)

if estacoes_sem_nome > 0:

    raise ValueError(
        f"Existem {estacoes_sem_nome} "
        "registros sem nome da estação."
    )


# ==============================================================
# VALIDAÇÃO DAS COORDENADAS
# ==============================================================

estacoes_sem_latitude = (
    df_inmet
    .filter(
        col("latitude").isNull()
    )
    .count()
)

estacoes_sem_longitude = (
    df_inmet
    .filter(
        col("longitude").isNull()
    )
    .count()
)

print(
    f"Registros sem latitude: "
    f"{estacoes_sem_latitude}"
)

print(
    f"Registros sem longitude: "
    f"{estacoes_sem_longitude}"
)

if (
    estacoes_sem_latitude > 0
    or
    estacoes_sem_longitude > 0
):

    raise ValueError(
        "Existem registros sem "
        "coordenadas da estação."
    )

print("✓ Coordenadas validadas.")


# ==============================================================
# CANONIZACAO DOS METADADOS DAS ESTACOES
# ==============================================================
#
# O INMET pode revisar o nome ou a precisao das coordenadas de uma
# estacao entre arquivos anuais. Como codigo_estacao e a identidade
# oficial e permanece estavel, adotamos os metadados do registro mais
# recente para todo o periodo.
# ==============================================================

janela_metadados_estacao = (
    Window
    .partitionBy("codigo_estacao")
    .orderBy(col("data_hora_utc").desc())
)

metadados_estacao = (
    df_inmet
    .withColumn(
        "ordem_metadados",
        row_number().over(janela_metadados_estacao)
    )
    .filter(col("ordem_metadados") == 1)
    .select(
        "codigo_estacao",
        col("estacao").alias("estacao_canonica"),
        col("latitude").alias("latitude_canonica"),
        col("longitude").alias("longitude_canonica"),
    )
)

df_inmet = (
    df_inmet
    .drop("estacao", "latitude", "longitude")
    .join(
        metadados_estacao,
        on="codigo_estacao",
        how="left",
    )
    .withColumnRenamed("estacao_canonica", "estacao")
    .withColumnRenamed("latitude_canonica", "latitude")
    .withColumnRenamed("longitude_canonica", "longitude")
)


# ==============================================================
# VALIDAÇÃO DE CONSISTÊNCIA
# ==============================================================
#
# Um mesmo codigo_estacao deve representar
# uma única estação e possuir coordenadas
# consistentes.
# ==============================================================

print(
    "\nValidando consistência das estações..."
)

detalhes_inconsistencias_estacao = (
    df_inmet
    .groupBy(
        "codigo_estacao"
    )
    .agg(
        count(
            "estacao"
        )
        .alias(
            "qtd_registros"
        ),

        spark_max(
            "estacao"
        )
        .alias(
            "estacao_max"
        ),

        spark_min(
            "estacao"
        )
        .alias(
            "estacao_min"
        ),

        spark_max(
            "latitude"
        )
        .alias(
            "latitude_max"
        ),

        spark_min(
            "latitude"
        )
        .alias(
            "latitude_min"
        ),

        spark_max(
            "longitude"
        )
        .alias(
            "longitude_max"
        ),

        spark_min(
            "longitude"
        )
        .alias(
            "longitude_min"
        ),
    )
    .filter(
        (
            col("latitude_max")
            !=
            col("latitude_min")
        )
        |
        (
            col("longitude_max")
            !=
            col("longitude_min")
        )
        |
        (
            col("estacao_max")
            !=
            col("estacao_min")
        )
    )
)

inconsistencias_estacao = (
    detalhes_inconsistencias_estacao.count()
)

print(
    f"Estações com inconsistência: "
    f"{inconsistencias_estacao}"
)

if inconsistencias_estacao > 0:

    detalhes_inconsistencias_estacao.show(
        truncate=False
    )

    raise ValueError(
        "Existem estações com "
        "informações inconsistentes."
    )

print(
    "✓ Identificação das estações consistente."
)


# ==============================================================
# AGREGAÇÃO DIÁRIA
# ==============================================================

print(
    "\nAgregando por estação e dia..."
)

#
# IMPORTANTE:
#
# Aqui NÃO fazemos JOIN com a referência
# municipio_estacao.
#
# A Silver INMET já possui:
#
#   codigo_estacao
#   estacao
#   latitude
#   longitude
#
# Portanto usamos diretamente essas colunas.
#
# Isso evita o problema:
#
#   latitude_estacao cannot be resolved
#
# porque latitude_estacao só passa a existir
# DEPOIS desta agregação.
#

df_diario = (
    df_inmet

    .groupBy(
        "codigo_estacao",
        "data",
    )

    .agg(

        # ------------------------------------------------------
        # Identificação
        # ------------------------------------------------------

        spark_max(
            "estacao"
        )
        .alias(
            "estacao"
        ),

        spark_max(
            "latitude"
        )
        .alias(
            "latitude_estacao"
        ),

        spark_max(
            "longitude"
        )
        .alias(
            "longitude_estacao"
        ),

        # ------------------------------------------------------
        # Quantidade de observações
        # ------------------------------------------------------

        count("*")
        .alias(
            "qtd_observacoes"
        ),

        # ------------------------------------------------------
        # Quantidade de observações válidas por variável
        # ------------------------------------------------------
        #
        # count(coluna) ignora valores NULL.
        #
        # Isso permite distinguir:
        #
        #   24 -> cobertura horária completa
        #   1–23 -> cobertura parcial
        #   0 -> nenhuma medição válida no dia
        #
        # Esses campos serão preservados na Silver diária para
        # rastreabilidade e para futuros critérios de qualidade
        # no Power BI e nas análises do TCC.
        # ------------------------------------------------------

        count(
            "precipitacao_mm"
        )
        .alias(
            "qtd_obs_precipitacao"
        ),

        count(
            "temperatura_c"
        )
        .alias(
            "qtd_obs_temperatura"
        ),

        count(
            "umidade_pct"
        )
        .alias(
            "qtd_obs_umidade"
        ),

        count(
            "vento_velocidade_ms"
        )
        .alias(
            "qtd_obs_vento"
        ),

        # ------------------------------------------------------
        # Precipitação
        # ------------------------------------------------------

        spark_sum(
            "precipitacao_mm"
        )
        .alias(
            "precipitacao_dia_mm"
        ),

        # ------------------------------------------------------
        # Temperatura
        # ------------------------------------------------------

        avg(
            "temperatura_c"
        )
        .alias(
            "temperatura_media_dia_c"
        ),

        spark_min(
            "temperatura_c"
        )
        .alias(
            "temperatura_minima_dia_c"
        ),

        spark_max(
            "temperatura_c"
        )
        .alias(
            "temperatura_maxima_dia_c"
        ),

        # ------------------------------------------------------
        # Umidade
        # ------------------------------------------------------

        avg(
            "umidade_pct"
        )
        .alias(
            "umidade_media_dia_pct"
        ),

        spark_min(
            "umidade_pct"
        )
        .alias(
            "umidade_minima_dia_pct"
        ),

        spark_max(
            "umidade_pct"
        )
        .alias(
            "umidade_maxima_dia_pct"
        ),

        # ------------------------------------------------------
        # Vento
        # ------------------------------------------------------

        avg(
            "vento_velocidade_ms"
        )
        .alias(
            "vento_medio_dia_ms"
        ),

        spark_max(
            "vento_velocidade_ms"
        )
        .alias(
            "vento_maximo_dia_ms"
        ),
    )
)


# ==============================================================
# ARREDONDAMENTO
# ==============================================================

df_diario = (

    df_diario

    .withColumn(
        "precipitacao_dia_mm",
        spark_round(
            col(
                "precipitacao_dia_mm"
            ),
            2
        )
    )

    .withColumn(
        "temperatura_media_dia_c",
        spark_round(
            col(
                "temperatura_media_dia_c"
            ),
            2
        )
    )

    .withColumn(
        "temperatura_minima_dia_c",
        spark_round(
            col(
                "temperatura_minima_dia_c"
            ),
            2
        )
    )

    .withColumn(
        "temperatura_maxima_dia_c",
        spark_round(
            col(
                "temperatura_maxima_dia_c"
            ),
            2
        )
    )

    .withColumn(
        "umidade_media_dia_pct",
        spark_round(
            col(
                "umidade_media_dia_pct"
            ),
            2
        )
    )

    .withColumn(
        "umidade_minima_dia_pct",
        spark_round(
            col(
                "umidade_minima_dia_pct"
            ),
            2
        )
    )

    .withColumn(
        "umidade_maxima_dia_pct",
        spark_round(
            col(
                "umidade_maxima_dia_pct"
            ),
            2
        )
    )

    .withColumn(
        "vento_medio_dia_ms",
        spark_round(
            col(
                "vento_medio_dia_ms"
            ),
            2
        )
    )

    .withColumn(
        "vento_maximo_dia_ms",
        spark_round(
            col(
                "vento_maximo_dia_ms"
            ),
            2
        )
    )
)


# ==============================================================
# ORDEM DAS COLUNAS
# ==============================================================

df_diario = (
    df_diario
    .select(

        "data",

        "codigo_estacao",

        "estacao",

        "latitude_estacao",

        "longitude_estacao",

        "qtd_observacoes",

        "qtd_obs_precipitacao",

        "qtd_obs_temperatura",

        "qtd_obs_umidade",

        "qtd_obs_vento",

        "precipitacao_dia_mm",

        "temperatura_media_dia_c",

        "temperatura_minima_dia_c",

        "temperatura_maxima_dia_c",

        "umidade_media_dia_pct",

        "umidade_minima_dia_pct",

        "umidade_maxima_dia_pct",

        "vento_medio_dia_ms",

        "vento_maximo_dia_ms",
    )
)


# ==============================================================
# RESULTADOS
# ==============================================================

total_diario = (
    df_diario.count()
)

print("\n" + "=" * 60)
print("RESUMO")
print("=" * 60)

print(
    f"Registros horários: "
    f"{total_horario}"
)

print(
    f"Registros horários 2023–2025: "
    f"{total_periodo}"
)

print(
    f"Registros diários: "
    f"{total_diario}"
)

print(
    f"Estações: "
    f"{total_estacoes}"
)


# ==============================================================
# VALIDAÇÃO DA CHAVE
# ==============================================================

print(
    "\nValidando chave estação + data..."
)

total_registros = (
    df_diario.count()
)

total_chaves = (
    df_diario
    .select(
        "codigo_estacao",
        "data"
    )
    .distinct()
    .count()
)

duplicidades = (
    total_registros
    -
    total_chaves
)

print(
    f"Duplicidades: "
    f"{duplicidades}"
)

if duplicidades > 0:

    raise ValueError(
        "Existem duplicidades na chave "
        "codigo_estacao + data."
    )

print(
    "✓ Chave validada."
)


# ==============================================================
# VALIDAÇÃO TEMPORAL
# ==============================================================

datas = (
    df_diario
    .select(

        spark_min(
            "data"
        )
        .alias(
            "data_min"
        ),

        spark_max(
            "data"
        )
        .alias(
            "data_max"
        ),
    )
    .collect()[0]
)

print(
    f"Período: "
    f"{datas['data_min']} "
    f"a "
    f"{datas['data_max']}"
)


# ==============================================================
# VALIDAÇÃO DE COMPLETUDE
# ==============================================================

#
# 2023 = 365 dias
# 2024 = 366 dias
# 2025 = 365 dias
#
# Total = 1096 dias
#

dias_esperados = 1096

total_esperado = (
    total_estacoes
    *
    dias_esperados
)

print(
    f"Registros esperados: "
    f"{total_esperado}"
)

print(
    f"Registros encontrados: "
    f"{total_diario}"
)

if total_diario == total_esperado:

    print(
        "✓ Completeness diária validada."
    )

else:

    print(
        "⚠ Atenção: existem dias ausentes "
        "ou estações com cobertura incompleta."
    )


# ==============================================================
# DISTRIBUIÇÃO POR ANO
# ==============================================================

print(
    "\nRegistros por ano:"
)

(
    df_diario
    .groupBy(
        col("data")
        .substr(
            1,
            4
        )
        .alias(
            "ano"
        )
    )
    .count()
    .orderBy(
        "ano"
    )
    .show(
        truncate=False
    )
)


# ==============================================================
# OBSERVAÇÕES POR DIA
# ==============================================================

estatisticas = (
    df_diario
    .select(

        spark_min(
            "qtd_observacoes"
        )
        .alias(
            "min"
        ),

        spark_max(
            "qtd_observacoes"
        )
        .alias(
            "max"
        ),

        avg(
            "qtd_observacoes"
        )
        .alias(
            "media"
        ),
    )
    .collect()[0]
)

print(
    "Observações por dia — "
    f"mín: {estatisticas['min']}, "
    f"máx: {estatisticas['max']}, "
    f"média: {estatisticas['media']:.2f}"
)


# ==============================================================
# COBERTURA HORÁRIA VÁLIDA POR VARIÁVEL
# ==============================================================
#
# As quatro colunas abaixo registram quantas das 24 observações
# horárias do dia possuem valor válido para cada variável.
#
# A validação garante:
#
#   0 <= qtd_obs_* <= 24
#
# e também verifica a coerência entre:
#
#   qtd_obs_* == 0  <=> indicador diário == NULL
#
# ==============================================================

print(
    "\nValidando cobertura horária "
    "das variáveis meteorológicas..."
)


metricas_cobertura = [
    (
        "qtd_obs_precipitacao",
        "precipitacao_dia_mm",
    ),
    (
        "qtd_obs_temperatura",
        "temperatura_media_dia_c",
    ),
    (
        "qtd_obs_umidade",
        "umidade_media_dia_pct",
    ),
    (
        "qtd_obs_vento",
        "vento_medio_dia_ms",
    ),
]


for campo_qtd, campo_indicador in metricas_cobertura:

    fora_intervalo = (
        df_diario
        .filter(
            (col(campo_qtd) < 0)
            |
            (col(campo_qtd) > 24)
        )
        .count()
    )

    inconsistencias = (
        df_diario
        .filter(
            (
                (col(campo_qtd) == 0)
                &
                col(campo_indicador).isNotNull()
            )
            |
            (
                (col(campo_qtd) > 0)
                &
                col(campo_indicador).isNull()
            )
        )
        .count()
    )

    estatisticas_cobertura = (
        df_diario
        .select(
            spark_min(campo_qtd).alias("min"),
            spark_max(campo_qtd).alias("max"),
            avg(campo_qtd).alias("media"),
        )
        .collect()[0]
    )

    print(
        f"{campo_qtd}: "
        f"mín={estatisticas_cobertura['min']}, "
        f"máx={estatisticas_cobertura['max']}, "
        f"média={estatisticas_cobertura['media']:.2f}, "
        f"inconsistências={inconsistencias}"
    )

    if fora_intervalo > 0:

        raise ValueError(
            f"O campo {campo_qtd} possui "
            "valores fora do intervalo 0–24."
        )

    if inconsistencias > 0:

        raise ValueError(
            f"Existem {inconsistencias} inconsistências "
            f"entre {campo_qtd} e {campo_indicador}."
        )


print(
    "✓ Cobertura horária das variáveis validada."
)


# ==============================================================
# VALIDAÇÃO DOS ATRIBUTOS DIÁRIOS
# ==============================================================

print(
    "\nValidando atributos da Silver diária..."
)

campos_importantes = [
    "codigo_estacao",
    "estacao",
    "latitude_estacao",
    "longitude_estacao",
]

for campo in campos_importantes:

    nulos = (
        df_diario
        .filter(
            col(campo).isNull()
        )
        .count()
    )

    print(
        f"{campo}: "
        f"{nulos} nulos"
    )

    if nulos > 0:

        raise ValueError(
            f"O campo {campo} "
            "possui valores nulos."
        )

print(
    "✓ Atributos da estação validados."
)


# ==============================================================
# VALIDAÇÃO DE VARIÁVEIS METEOROLÓGICAS
# ==============================================================

print(
    "\nValidando variáveis meteorológicas..."
)

campos_meteorologicos = [
    "precipitacao_dia_mm",
    "temperatura_media_dia_c",
    "temperatura_minima_dia_c",
    "temperatura_maxima_dia_c",
    "umidade_media_dia_pct",
    "umidade_minima_dia_pct",
    "umidade_maxima_dia_pct",
    "vento_medio_dia_ms",
    "vento_maximo_dia_ms",
]

for campo in campos_meteorologicos:

    nulos = (
        df_diario
        .filter(
            col(campo).isNull()
        )
        .count()
    )

    print(
        f"{campo}: "
        f"{nulos} nulos"
    )

print(
    "✓ Variáveis meteorológicas verificadas."
)


# ==============================================================
# AMOSTRA
# ==============================================================

print(
    "\nAmostra:"
)

(
    df_diario
    .orderBy(
        "data",
        "codigo_estacao"
    )
    .show(
        10,
        truncate=False
    )
)


# ==============================================================
# GRAVAÇÃO
# ==============================================================

print(
    "\nGravando Silver INMET Diário..."
)

(
    df_diario
    .write
    .mode("overwrite")
    .partitionBy(
        "data"
    )
    .parquet(
        SILVER_INMET_DIARIO
    )
)


# ==============================================================
# VALIDAÇÃO PÓS-GRAVAÇÃO
# ==============================================================

print(
    "Validando gravação..."
)

df_validacao = (
    spark.read
    .option(
        "mergeSchema",
        "true"
    )
    .parquet(
        SILVER_INMET_DIARIO
    )
)

total_gravado = (
    df_validacao.count()
)

if total_gravado != total_diario:

    raise ValueError(
        "Quantidade de registros gravados "
        "diferente da quantidade produzida."
    )

print(
    f"Registros gravados: "
    f"{total_gravado}"
)

print(
    "✓ Gravação validada."
)


# ==============================================================
# VALIDAÇÃO DAS COLUNAS DE COBERTURA APÓS GRAVAÇÃO
# ==============================================================

colunas_cobertura = [
    "qtd_obs_precipitacao",
    "qtd_obs_temperatura",
    "qtd_obs_umidade",
    "qtd_obs_vento",
]


colunas_cobertura_faltantes = [
    campo
    for campo in colunas_cobertura
    if campo not in df_validacao.columns
]


if colunas_cobertura_faltantes:

    print(
        "\nERRO: colunas de cobertura "
        "não foram persistidas:"
    )

    for campo in colunas_cobertura_faltantes:
        print(f"- {campo}")

    raise ValueError(
        "As colunas de cobertura horária "
        "não foram gravadas corretamente."
    )


print(
    "✓ Colunas de cobertura horária "
    "persistidas com sucesso."
)


# ==============================================================
# VALIDAÇÃO FINAL DA CHAVE APÓS GRAVAÇÃO
# ==============================================================

chaves_gravadas = (
    df_validacao
    .select(
        "codigo_estacao",
        "data"
    )
    .distinct()
    .count()
)

if chaves_gravadas != total_gravado:

    raise ValueError(
        "Existem duplicidades na chave "
        "codigo_estacao + data após gravação."
    )

print(
    "✓ Chave validada após gravação."
)


# ==============================================================
# FINAL
# ==============================================================

print(
    "\n" + "=" * 60
)

print(
    "SILVER INMET DIÁRIO CONCLUÍDA"
)

print(
    "=" * 60
)

print(
    f"Saída: "
    f"{SILVER_INMET_DIARIO}"
)

print(
    "Granularidade: "
    "estação + dia"
)

print(
    f"Registros: "
    f"{total_gravado}"
)

print(
    f"Estações: "
    f"{total_estacoes}"
)

print(
    f"Período: "
    f"{datas['data_min']} "
    f"a "
    f"{datas['data_max']}"
)

print(
    "=" * 60
)


# ==============================================================
# ENCERRAMENTO
# ==============================================================

spark.stop()
