# ==============================================================
# GOLD — FATO INTERNAÇÃO + METEOROLOGIA
# ==============================================================
#
# Objetivo:
#   Enriquecer a Gold fato_internacao com os indicadores diários
#   da Silver INMET Diário.
#
# Granularidade final:
#   1 registro = 1 internação SIH
#
# Chave do JOIN:
#   fato_internacao.codigo_estacao = inmet_diario.codigo_estacao
#   fato_internacao.data_internacao = inmet_diario.data
#
# Entradas:
#   /home/jovyan/work/data/gold/fato_internacao
#   /home/jovyan/work/data/silver/inmet_diario
#
# Saída:
#   /home/jovyan/work/data/gold/fato_internacao_meteorologia
#
# Regra crítica:
#   A quantidade de internações NÃO pode mudar.
#
# Esperado:
#   8.409.047 registros antes do JOIN
#   8.409.047 registros depois do JOIN
#
# Observação:
#   A Silver INMET Diário possui 1 linha por codigo_estacao + data
#   e 43.840 registros no período 2023–2025.
#
# ==============================================================

from pyspark.sql import SparkSession

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    to_date,
    count,
    countDistinct,
    sum as spark_sum,
    min as spark_min,
    max as spark_max,
    when,
    round as spark_round,
    broadcast,
    year,
    lit,
)


# ==============================================================
# CONFIGURAÇÕES
# ==============================================================

BASE_PATH = "/home/jovyan/work/data"

GOLD_FATO_INTERNACAO = (
    f"{BASE_PATH}/gold/fato_internacao"
)

SILVER_INMET_DIARIO = (
    f"{BASE_PATH}/silver/inmet_diario"
)

GOLD_FATO_INTERNACAO_METEOROLOGIA = (
    f"{BASE_PATH}/gold/fato_internacao_meteorologia"
)


# Valores esperados do conjunto congelado do TCC
TOTAL_INTERNACOES_ESPERADO = 8_409_047
TOTAL_INMET_DIARIO_ESPERADO = 43_840
TOTAL_ESTACOES_ESPERADO = 40

DATA_INICIO = "2023-01-01"
DATA_FIM = "2025-12-31"


# ==============================================================
# SPARK
# ==============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Fato_Internacao_Meteorologia")
    .config(
        "spark.sql.shuffle.partitions",
        "40"
    )
    .config(
        "spark.ui.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ==============================================================
# FUNÇÃO AUXILIAR — VALIDAÇÃO DE COLUNAS
# ==============================================================

def validar_colunas(
    df,
    colunas_obrigatorias,
    nome_dataset,
):
    colunas_faltantes = [
        coluna
        for coluna in colunas_obrigatorias
        if coluna not in df.columns
    ]

    if colunas_faltantes:

        print(
            f"\nERRO: colunas ausentes em "
            f"{nome_dataset}:"
        )

        for coluna in colunas_faltantes:
            print(
                f"- {coluna}"
            )

        print(
            "\nSchema encontrado:"
        )

        df.printSchema()

        raise ValueError(
            f"{nome_dataset} não possui "
            "todas as colunas necessárias."
        )


# ==============================================================
# CABEÇALHO
# ==============================================================

print(
    "=" * 70
)

print(
    "INICIANDO GOLD — FATO INTERNAÇÃO + METEOROLOGIA"
)

print(
    "=" * 70
)


# ==============================================================
# 1. LEITURA DA GOLD FATO INTERNAÇÃO
# ==============================================================

print(
    "\nLendo Gold fato_internacao..."
)

df_fato = (
    spark.read
    .option(
        "mergeSchema",
        "true"
    )
    .parquet(
        GOLD_FATO_INTERNACAO
    )
)


colunas_fato_obrigatorias = [
    "ano",
    "mes",
    "data_internacao",
    "codigo_estacao",
    "CD_MUN",
    "NM_MUN",
    "codigo_cid",
]

validar_colunas(
    df_fato,
    colunas_fato_obrigatorias,
    "Gold fato_internacao",
)


total_fato = (
    df_fato.count()
)


print(
    f"Registros na fato_internacao: "
    f"{total_fato}"
)


if total_fato != TOTAL_INTERNACOES_ESPERADO:

    raise ValueError(
        "Quantidade inesperada de registros "
        "na Gold fato_internacao. "
        f"Encontrado: {total_fato}. "
        f"Esperado: {TOTAL_INTERNACOES_ESPERADO}."
    )


print(
    "✓ Quantidade da fato_internacao validada."
)


# ==============================================================
# 2. PREPARAÇÃO DA CHAVE DA FATO
# ==============================================================

print(
    "\nPreparando chave da fato_internacao..."
)


df_fato = (
    df_fato

    .withColumn(
        "_codigo_estacao_join",
        trim(
            upper(
                col(
                    "codigo_estacao"
                ).cast("string")
            )
        )
    )

    .withColumn(
        "_data_join",
        to_date(
            col(
                "data_internacao"
            )
        )
    )
)


chaves_fato_invalidas = (
    df_fato
    .filter(
        col(
            "_codigo_estacao_join"
        ).isNull()
        |
        (
            col(
                "_codigo_estacao_join"
            )
            ==
            ""
        )
        |
        col(
            "_data_join"
        ).isNull()
    )
    .count()
)


print(
    f"Internações com chave meteorológica inválida: "
    f"{chaves_fato_invalidas}"
)


if chaves_fato_invalidas > 0:

    raise ValueError(
        "Existem internações sem "
        "codigo_estacao ou data_internacao válida."
    )


print(
    "✓ Chave da fato_internacao validada."
)


# ==============================================================
# 2.1. COBERTURA TEMPORAL DA FATO EM RELAÇÃO AO INMET
# ==============================================================
#
# Os arquivos hospitalares processados pertencem ao recorte do
# projeto, mas data_internacao pode eventualmente estar fora do
# intervalo meteorológico 2023-01-01 a 2025-12-31.
#
# Essas internações NÃO serão removidas.
#
# O objetivo é preservar integralmente a fato_internacao e marcar
# explicitamente os registros para os quais não existe cobertura
# temporal do INMET.
# ==============================================================

print(
    "\nValidando período das datas de internação..."
)


estatisticas_periodo_fato = (
    df_fato

    .agg(

        spark_min(
            "_data_join"
        ).alias(
            "data_min"
        ),

        spark_max(
            "_data_join"
        ).alias(
            "data_max"
        ),

        spark_sum(
            when(
                col("_data_join") < DATA_INICIO,
                1
            ).otherwise(0)
        ).alias(
            "antes_periodo_inmet"
        ),

        spark_sum(
            when(
                (
                    col("_data_join") >= DATA_INICIO
                )
                &
                (
                    col("_data_join") <= DATA_FIM
                ),
                1
            ).otherwise(0)
        ).alias(
            "dentro_periodo_inmet"
        ),

        spark_sum(
            when(
                col("_data_join") > DATA_FIM,
                1
            ).otherwise(0)
        ).alias(
            "depois_periodo_inmet"
        ),
    )

    .collect()[0]
)


fato_antes_periodo = (
    estatisticas_periodo_fato[
        "antes_periodo_inmet"
    ]
)

fato_dentro_periodo = (
    estatisticas_periodo_fato[
        "dentro_periodo_inmet"
    ]
)

fato_depois_periodo = (
    estatisticas_periodo_fato[
        "depois_periodo_inmet"
    ]
)

fato_fora_periodo = (
    fato_antes_periodo
    +
    fato_depois_periodo
)


print(
    f"Período das datas de internação: "
    f"{estatisticas_periodo_fato['data_min']} "
    f"a "
    f"{estatisticas_periodo_fato['data_max']}"
)

print(
    f"Internações antes de {DATA_INICIO}: "
    f"{fato_antes_periodo}"
)

print(
    f"Internações dentro de "
    f"{DATA_INICIO} a {DATA_FIM}: "
    f"{fato_dentro_periodo}"
)

print(
    f"Internações depois de {DATA_FIM}: "
    f"{fato_depois_periodo}"
)

print(
    f"Internações fora da cobertura temporal INMET: "
    f"{fato_fora_periodo}"
)


if fato_fora_periodo > 0:

    print(
        "\nDistribuição das internações fora "
        "do período INMET por ano:"
    )

    (
        df_fato

        .filter(
            (
                col("_data_join") < DATA_INICIO
            )
            |
            (
                col("_data_join") > DATA_FIM
            )
        )

        .groupBy(
            year(
                col("_data_join")
            ).alias(
                "ano_data_internacao"
            )
        )

        .count()

        .orderBy(
            "ano_data_internacao"
        )

        .show(
            50,
            truncate=False
        )
    )


# ==============================================================
# 3. LEITURA DA SILVER INMET DIÁRIO
# ==============================================================

print(
    "\nLendo Silver INMET Diário..."
)


# IMPORTANTE:
# A coluna data foi usada como partição na gravação da Silver.
# Portanto a leitura é feita normalmente, sem recursiveFileLookup,
# para que o Spark preserve a descoberta dessa coluna.

df_inmet = (
    spark.read
    .option(
        "mergeSchema",
        "true"
    )
    .parquet(
        SILVER_INMET_DIARIO
    )
)


colunas_inmet_obrigatorias = [
    "data",
    "codigo_estacao",
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
]

validar_colunas(
    df_inmet,
    colunas_inmet_obrigatorias,
    "Silver INMET Diário",
)


total_inmet = (
    df_inmet.count()
)


total_estacoes_inmet = (
    df_inmet
    .select(
        "codigo_estacao"
    )
    .distinct()
    .count()
)


print(
    f"Registros INMET Diário: "
    f"{total_inmet}"
)

print(
    f"Estações INMET Diário: "
    f"{total_estacoes_inmet}"
)


if total_inmet != TOTAL_INMET_DIARIO_ESPERADO:

    raise ValueError(
        "Quantidade inesperada de registros "
        "na Silver INMET Diário. "
        f"Encontrado: {total_inmet}. "
        f"Esperado: {TOTAL_INMET_DIARIO_ESPERADO}."
    )


if total_estacoes_inmet != TOTAL_ESTACOES_ESPERADO:

    raise ValueError(
        "Quantidade inesperada de estações "
        "na Silver INMET Diário. "
        f"Encontrado: {total_estacoes_inmet}. "
        f"Esperado: {TOTAL_ESTACOES_ESPERADO}."
    )


print(
    "✓ Silver INMET Diário validada."
)


# ==============================================================
# 4. PREPARAÇÃO DA SILVER INMET
# ==============================================================

df_inmet = (
    df_inmet

    .withColumn(
        "_codigo_estacao_join",
        trim(
            upper(
                col(
                    "codigo_estacao"
                ).cast("string")
            )
        )
    )

    .withColumn(
        "_data_join",
        to_date(
            col(
                "data"
            )
        )
    )
)


# ==============================================================
# 5. VALIDAÇÃO DA CHAVE DO INMET
# ==============================================================

print(
    "\nValidando chave codigo_estacao + data do INMET..."
)


chaves_inmet_invalidas = (
    df_inmet
    .filter(
        col(
            "_codigo_estacao_join"
        ).isNull()
        |
        (
            col(
                "_codigo_estacao_join"
            )
            ==
            ""
        )
        |
        col(
            "_data_join"
        ).isNull()
    )
    .count()
)


if chaves_inmet_invalidas > 0:

    raise ValueError(
        f"Existem {chaves_inmet_invalidas} "
        "registros INMET com chave inválida."
    )


duplicidades_inmet = (
    df_inmet

    .groupBy(
        "_codigo_estacao_join",
        "_data_join",
    )

    .count()

    .filter(
        col("count") > 1
    )

    .count()
)


print(
    f"Duplicidades na chave INMET: "
    f"{duplicidades_inmet}"
)


if duplicidades_inmet > 0:

    raise ValueError(
        "A Silver INMET Diário possui "
        "duplicidades em codigo_estacao + data."
    )


datas_inmet = (
    df_inmet

    .select(
        spark_min(
            "_data_join"
        ).alias(
            "data_min"
        ),

        spark_max(
            "_data_join"
        ).alias(
            "data_max"
        ),
    )

    .collect()[0]
)


print(
    f"Período INMET: "
    f"{datas_inmet['data_min']} "
    f"a "
    f"{datas_inmet['data_max']}"
)


if (
    str(
        datas_inmet["data_min"]
    )
    !=
    DATA_INICIO
    or
    str(
        datas_inmet["data_max"]
    )
    !=
    DATA_FIM
):

    raise ValueError(
        "Período da Silver INMET Diário "
        "diferente de 2023–2025."
    )


print(
    "✓ Chave e período do INMET validados."
)


# ==============================================================
# 6. VALIDAÇÃO DOS CÓDIGOS DE ESTAÇÃO
# ==============================================================

print(
    "\nComparando estações da fato com o INMET..."
)


df_codigos_fato = (
    df_fato
    .select(
        col(
            "_codigo_estacao_join"
        ).alias(
            "codigo_estacao"
        )
    )
    .distinct()
)


df_codigos_inmet = (
    df_inmet
    .select(
        col(
            "_codigo_estacao_join"
        ).alias(
            "codigo_estacao"
        )
    )
    .distinct()
)


fato_sem_inmet = (
    df_codigos_fato

    .join(
        df_codigos_inmet,
        on="codigo_estacao",
        how="left_anti",
    )
)


inmet_sem_fato = (
    df_codigos_inmet

    .join(
        df_codigos_fato,
        on="codigo_estacao",
        how="left_anti",
    )
)


qtd_fato_sem_inmet = (
    fato_sem_inmet.count()
)

qtd_inmet_sem_fato = (
    inmet_sem_fato.count()
)


print(
    f"Estações da fato sem INMET: "
    f"{qtd_fato_sem_inmet}"
)

print(
    f"Estações do INMET sem uso na fato: "
    f"{qtd_inmet_sem_fato}"
)


if qtd_fato_sem_inmet > 0:

    print(
        "\nEstações da fato sem INMET:"
    )

    fato_sem_inmet.show(
        50,
        truncate=False
    )

    raise ValueError(
        "Existem estações da fato_internacao "
        "sem correspondência na Silver INMET Diário."
    )


print(
    "✓ Códigos de estação compatíveis."
)


# ==============================================================
# 7. PREPARAÇÃO DO DATAFRAME METEOROLÓGICO PARA O JOIN
# ==============================================================

# Não carregamos novamente:
#   estacao
#   latitude_estacao
#   longitude_estacao
#
# Esses atributos já existem na fato_internacao por causa do
# mapeamento município → estação.
#
# Isso evita colunas duplicadas e mantém a Gold base como fonte
# dos atributos geográficos da estação.

df_inmet_join = (
    df_inmet

    .select(

        "_codigo_estacao_join",

        "_data_join",

        col(
            "qtd_observacoes"
        ).alias(
            "qtd_observacoes_inmet"
        ),

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

    .withColumn(
        "_meteo_match",
        when(
            col(
                "_codigo_estacao_join"
            ).isNotNull(),
            1
        ).otherwise(0)
    )
)


# ==============================================================
# 8. JOIN
# ==============================================================

print(
    "\nIntegrando fato_internacao "
    "com Silver INMET Diário..."
)


# A Silver INMET Diário possui apenas 43.840 registros.
# Broadcast evita shuffle desnecessário da fato com 8,4 milhões
# de internações.

df_integrada = (
    df_fato
    .alias("f")

    .join(
        broadcast(
            df_inmet_join
        ).alias("m"),

        on=[
            "_codigo_estacao_join",
            "_data_join",
        ],

        how="left",
    )
)


# ==============================================================
# 9. VALIDAÇÃO DO JOIN
# ==============================================================

print(
    "\nValidando resultado do JOIN..."
)


estatisticas_join = (
    df_integrada

    .agg(

        count("*")
        .alias(
            "total"
        ),

        spark_sum(
            when(
                col("_meteo_match") == 1,
                1
            ).otherwise(0)
        )
        .alias(
            "com_match"
        ),

        spark_sum(
            when(
                col("_meteo_match").isNull(),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_match"
        ),

        # ------------------------------------------------------
        # Sem match por posição temporal
        # ------------------------------------------------------

        spark_sum(
            when(
                (
                    col("_meteo_match").isNull()
                )
                &
                (
                    col("_data_join") < DATA_INICIO
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_match_antes_periodo"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match").isNull()
                )
                &
                (
                    col("_data_join") > DATA_FIM
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_match_depois_periodo"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match").isNull()
                )
                &
                (
                    col("_data_join") >= DATA_INICIO
                )
                &
                (
                    col("_data_join") <= DATA_FIM
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_match_dentro_periodo"
        ),

        # ------------------------------------------------------
        # Variáveis sem valor, considerando apenas registros
        # que efetivamente encontraram estação + dia no INMET.
        # ------------------------------------------------------

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "temperatura_media_dia_c"
                    ).isNull()
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_temperatura_com_match"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "precipitacao_dia_mm"
                    ).isNull()
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_precipitacao_com_match"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "umidade_media_dia_pct"
                    ).isNull()
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_umidade_com_match"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "vento_medio_dia_ms"
                    ).isNull()
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "sem_vento_com_match"
        ),

        # ------------------------------------------------------
        # Cobertura >= 18 horas válidas
        # ------------------------------------------------------

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "qtd_obs_temperatura"
                    )
                    >=
                    18
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "temp_cobertura_18h"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "qtd_obs_precipitacao"
                    )
                    >=
                    18
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "precip_cobertura_18h"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "qtd_obs_umidade"
                    )
                    >=
                    18
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "umidade_cobertura_18h"
        ),

        spark_sum(
            when(
                (
                    col("_meteo_match") == 1
                )
                &
                (
                    col(
                        "qtd_obs_vento"
                    )
                    >=
                    18
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "vento_cobertura_18h"
        ),
    )

    .collect()[0]
)


total_integrado = (
    estatisticas_join[
        "total"
    ]
)

com_match = (
    estatisticas_join[
        "com_match"
    ]
)

sem_match = (
    estatisticas_join[
        "sem_match"
    ]
)

sem_match_antes = (
    estatisticas_join[
        "sem_match_antes_periodo"
    ]
)

sem_match_depois = (
    estatisticas_join[
        "sem_match_depois_periodo"
    ]
)

sem_match_dentro = (
    estatisticas_join[
        "sem_match_dentro_periodo"
    ]
)


print(
    f"Registros antes do JOIN: "
    f"{total_fato}"
)

print(
    f"Registros depois do JOIN: "
    f"{total_integrado}"
)

print(
    f"Internações com estação + dia encontrado: "
    f"{com_match}"
)

print(
    f"Internações sem estação + dia encontrado: "
    f"{sem_match}"
)

print(
    f"  - antes do período INMET: "
    f"{sem_match_antes}"
)

print(
    f"  - depois do período INMET: "
    f"{sem_match_depois}"
)

print(
    f"  - dentro do período INMET: "
    f"{sem_match_dentro}"
)


# --------------------------------------------------------------
# Regra crítica 1:
# o JOIN nunca pode alterar a quantidade de internações.
# --------------------------------------------------------------

if total_integrado != total_fato:

    raise ValueError(
        "O JOIN alterou a granularidade da fato. "
        f"Antes: {total_fato}. "
        f"Depois: {total_integrado}."
    )


# --------------------------------------------------------------
# Regra crítica 2:
# não pode existir falha de correspondência dentro do intervalo
# em que sabemos que as 40 estações possuem todos os 1.096 dias.
# --------------------------------------------------------------

if sem_match_dentro > 0:

    print(
        "\nERRO: existem internações dentro de "
        "2023–2025 sem correspondência meteorológica."
    )

    (
        df_integrada

        .filter(
            (
                col("_meteo_match").isNull()
            )
            &
            (
                col("_data_join") >= DATA_INICIO
            )
            &
            (
                col("_data_join") <= DATA_FIM
            )
        )

        .select(
            "_data_join",
            "_codigo_estacao_join",
            "CD_MUN",
            "NM_MUN",
        )

        .groupBy(
            "_data_join",
            "_codigo_estacao_join",
        )

        .count()

        .orderBy(
            col("count").desc()
        )

        .show(
            50,
            truncate=False
        )
    )

    raise ValueError(
        "Existem internações dentro do período "
        "INMET sem correspondência estação + dia."
    )


# --------------------------------------------------------------
# Regra crítica 3:
# depois das validações de código e completude do INMET, todos os
# sem-match esperados devem ser explicados exclusivamente por
# datas fora da cobertura temporal do INMET.
# --------------------------------------------------------------

if sem_match != fato_fora_periodo:

    raise ValueError(
        "A quantidade de internações sem INMET "
        "não coincide com a quantidade de internações "
        "fora do período meteorológico. "
        f"Sem match: {sem_match}. "
        f"Fora do período: {fato_fora_periodo}."
    )


print(
    "✓ JOIN preservou todas as internações."
)

print(
    "✓ Não existem falhas de correspondência "
    "dentro do período 2023–2025."
)


if sem_match > 0:

    print(
        "⚠ Existem internações sem meteorologia "
        "porque data_internacao está fora da "
        "cobertura temporal 2023–2025 do INMET."
    )

    print(
        "  Essas internações serão preservadas "
        "na Gold com indicadores meteorológicos NULL."
    )


# 10. VALIDAÇÃO DA COBERTURA METEOROLÓGICA NAS INTERNAÇÕES
# ==============================================================

print(
    "\n" + "=" * 70
)

print(
    "COBERTURA METEOROLÓGICA NAS INTERNAÇÕES"
)

print(
    "=" * 70
)


print(
    f"Total de internações: "
    f"{total_integrado}"
)

print(
    f"Internações dentro da cobertura temporal INMET: "
    f"{com_match}"
)

print(
    f"Internações fora da cobertura temporal INMET: "
    f"{sem_match}"
)


def imprimir_cobertura(
    nome,
    sem_valor_com_match,
    cobertura_18h,
):
    """
    A cobertura da variável é calculada usando como denominador
    apenas as internações que encontraram codigo_estacao + data
    na Silver INMET Diário.

    Assim não confundimos:
      - ausência temporal do INMET; com
      - sensor/variável meteorológica sem medição válida.
    """

    com_valor = (
        com_match
        -
        sem_valor_com_match
    )

    if com_match > 0:

        pct_com_valor_match = (
            com_valor
            /
            com_match
            *
            100
        )

        pct_18h_match = (
            cobertura_18h
            /
            com_match
            *
            100
        )

    else:

        pct_com_valor_match = 0.0
        pct_18h_match = 0.0


    pct_com_valor_total = (
        com_valor
        /
        total_integrado
        *
        100
    )


    print(
        f"{nome:<14} "
        f"com valor={com_valor:<8} "
        f"sem valor no INMET={sem_valor_com_match:<8} "
        f"cobertura entre matches={pct_com_valor_match:6.2f}% "
        f"cobertura total={pct_com_valor_total:6.2f}% "
        f">=18h={cobertura_18h:<8} "
        f"({pct_18h_match:6.2f}% dos matches)"
    )


imprimir_cobertura(
    "Temperatura",
    estatisticas_join[
        "sem_temperatura_com_match"
    ],
    estatisticas_join[
        "temp_cobertura_18h"
    ],
)


imprimir_cobertura(
    "Precipitação",
    estatisticas_join[
        "sem_precipitacao_com_match"
    ],
    estatisticas_join[
        "precip_cobertura_18h"
    ],
)


imprimir_cobertura(
    "Umidade",
    estatisticas_join[
        "sem_umidade_com_match"
    ],
    estatisticas_join[
        "umidade_cobertura_18h"
    ],
)


imprimir_cobertura(
    "Vento",
    estatisticas_join[
        "sem_vento_com_match"
    ],
    estatisticas_join[
        "vento_cobertura_18h"
    ],
)


# 11. VALIDAÇÃO qtd_observacoes_inmet
# ==============================================================

dias_inmet_incompletos = (
    df_integrada
    .filter(
        (
            col("_meteo_match") == 1
        )
        &
        (
            col(
                "qtd_observacoes_inmet"
            )
            !=
            24
        )
    )
    .count()
)


print(
    f"\nInternações associadas a dias "
    f"com qtd_observacoes_inmet != 24: "
    f"{dias_inmet_incompletos}"
)


if dias_inmet_incompletos > 0:

    raise ValueError(
        "Existem internações ligadas a dias "
        "meteorológicos com quantidade de "
        "registros horários diferente de 24."
    )


print(
    "✓ Todos os dias meteorológicos associados "
    "possuem 24 registros horários."
)


# ==============================================================
# 12. REMOÇÃO DAS COLUNAS TÉCNICAS DE JOIN
# ==============================================================

df_integrada = (
    df_integrada

    # ----------------------------------------------------------
    # Flag de disponibilidade meteorológica
    # ----------------------------------------------------------

    .withColumn(
        "meteorologia_disponivel",
        when(
            col("_meteo_match") == 1,
            1
        ).otherwise(0)
    )

    # ----------------------------------------------------------
    # Motivo da ausência meteorológica
    # ----------------------------------------------------------

    .withColumn(
        "motivo_sem_meteorologia",

        when(
            col("_meteo_match") == 1,
            lit(None).cast("string")
        )

        .when(
            col("_data_join") < DATA_INICIO,
            "DATA_ANTERIOR_AO_PERIODO_INMET"
        )

        .when(
            col("_data_join") > DATA_FIM,
            "DATA_POSTERIOR_AO_PERIODO_INMET"
        )

        .otherwise(
            "SEM_CORRESPONDENCIA_INMET"
        )
    )

    .drop(
        "_codigo_estacao_join",
        "_data_join",
        "_meteo_match",
    )
)


# ==============================================================
# 13. VALIDAÇÃO DAS NOVAS COLUNAS
# ==============================================================

colunas_meteorologicas_finais = [
    "meteorologia_disponivel",
    "motivo_sem_meteorologia",
    "qtd_observacoes_inmet",
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
]


validar_colunas(
    df_integrada,
    colunas_meteorologicas_finais,
    "Gold integrada",
)


# ==============================================================
# 14. AMOSTRA
# ==============================================================

print(
    "\nAmostra da Gold integrada:"
)


(
    df_integrada

    .select(
        "data_internacao",
        "CD_MUN",
        "NM_MUN",
        "codigo_estacao",
        "codigo_cid",
        "meteorologia_disponivel",
        "motivo_sem_meteorologia",
        "qtd_observacoes_inmet",
        "qtd_obs_temperatura",
        "temperatura_media_dia_c",
        "qtd_obs_precipitacao",
        "precipitacao_dia_mm",
        "qtd_obs_umidade",
        "umidade_media_dia_pct",
        "qtd_obs_vento",
        "vento_medio_dia_ms",
    )

    .orderBy(
        "data_internacao",
        "CD_MUN",
    )

    .show(
        10,
        truncate=False
    )
)


# ==============================================================
# 15. GRAVAÇÃO
# ==============================================================

print(
    "\nGravando Gold "
    "fato_internacao_meteorologia..."
)


# Mantemos ano e mês como partições físicas.
#
# Não sobrescrevemos a fato_internacao original neste momento.
# Isso preserva a base já validada e permite comparar os dois
# datasets durante esta etapa de construção.

(
    df_integrada

    .repartition(
        "ano",
        "mes",
    )

    .write

    .mode(
        "overwrite"
    )

    .partitionBy(
        "ano",
        "mes",
    )

    .parquet(
        GOLD_FATO_INTERNACAO_METEOROLOGIA
    )
)


# ==============================================================
# 16. VALIDAÇÃO PÓS-GRAVAÇÃO
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
        GOLD_FATO_INTERNACAO_METEOROLOGIA
    )
)


total_gravado = (
    df_validacao.count()
)


print(
    f"Registros gravados: "
    f"{total_gravado}"
)


if total_gravado != TOTAL_INTERNACOES_ESPERADO:

    raise ValueError(
        "Quantidade gravada diferente "
        "da quantidade esperada. "
        f"Encontrado: {total_gravado}. "
        f"Esperado: {TOTAL_INTERNACOES_ESPERADO}."
    )


validar_colunas(
    df_validacao,
    colunas_meteorologicas_finais,
    "Gold gravada",
)


# ==============================================================
# 17. VALIDAÇÃO FINAL PÓS-GRAVAÇÃO
# ==============================================================

validacao_final = (
    df_validacao

    .agg(

        count("*")
        .alias(
            "total"
        ),

        spark_sum(
            when(
                col(
                    "meteorologia_disponivel"
                )
                ==
                1,
                1
            ).otherwise(0)
        )
        .alias(
            "com_meteorologia"
        ),

        spark_sum(
            when(
                col(
                    "meteorologia_disponivel"
                )
                ==
                0,
                1
            ).otherwise(0)
        )
        .alias(
            "sem_meteorologia"
        ),

        spark_sum(
            when(
                (
                    col(
                        "meteorologia_disponivel"
                    )
                    ==
                    1
                )
                &
                (
                    col(
                        "qtd_observacoes_inmet"
                    )
                    ==
                    24
                ),
                1
            ).otherwise(0)
        )
        .alias(
            "matches_com_24h"
        ),

        countDistinct(
            "codigo_estacao"
        )
        .alias(
            "estacoes"
        ),

        spark_min(
            "data_internacao"
        )
        .alias(
            "data_min"
        ),

        spark_max(
            "data_internacao"
        )
        .alias(
            "data_max"
        ),
    )

    .collect()[0]
)


if (
    validacao_final[
        "com_meteorologia"
    ]
    !=
    com_match
):

    raise ValueError(
        "Quantidade de registros com meteorologia "
        "mudou após a gravação."
    )


if (
    validacao_final[
        "sem_meteorologia"
    ]
    !=
    sem_match
):

    raise ValueError(
        "Quantidade de registros sem meteorologia "
        "mudou após a gravação."
    )


if (
    validacao_final[
        "matches_com_24h"
    ]
    !=
    com_match
):

    raise ValueError(
        "Nem todas as internações com meteorologia "
        "estão associadas a um dia INMET "
        "com 24 registros horários."
    )


motivos_invalidos = (
    df_validacao

    .filter(
        (
            col(
                "meteorologia_disponivel"
            )
            ==
            0
        )
        &
        (
            col(
                "motivo_sem_meteorologia"
            ).isNull()
        )
    )

    .count()
)


if motivos_invalidos > 0:

    raise ValueError(
        "Existem registros sem meteorologia "
        "e sem motivo registrado."
    )


print(
    "✓ Gravação validada."
)

print(
    "✓ Colunas meteorológicas persistidas."
)

print(
    "✓ Granularidade da fato preservada."
)

print(
    "✓ Registros fora da cobertura INMET "
    "foram preservados e identificados."
)


# 18. RESUMO FINAL
# ==============================================================

print(
    "\n" + "=" * 70
)

print(
    "GOLD FATO INTERNAÇÃO + METEOROLOGIA CONCLUÍDA"
)

print(
    "=" * 70
)


print(
    f"Entrada fato_internacao: "
    f"{total_fato}"
)

print(
    f"Entrada INMET Diário: "
    f"{total_inmet}"
)

print(
    f"Saída integrada: "
    f"{total_gravado}"
)

print(
    f"Internações com meteorologia: "
    f"{validacao_final['com_meteorologia']}"
)

print(
    f"Internações sem meteorologia "
    f"(fora do período INMET): "
    f"{validacao_final['sem_meteorologia']}"
)

print(
    f"Estações na saída: "
    f"{validacao_final['estacoes']}"
)

print(
    f"Período das internações: "
    f"{validacao_final['data_min']} "
    f"a "
    f"{validacao_final['data_max']}"
)

print(
    f"Saída: "
    f"{GOLD_FATO_INTERNACAO_METEOROLOGIA}"
)

print(
    "Granularidade: "
    "1 registro = 1 internação SIH"
)

print(
    "=" * 70
)


# ==============================================================
# ENCERRAMENTO
# ==============================================================

spark.stop()
