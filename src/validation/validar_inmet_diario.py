from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, countDistinct, sum as spark_sum, avg,
    min as spark_min, max as spark_max, when, year, to_date,
    round as spark_round, trim, upper, input_file_name, regexp_extract,
)

BASE_PATH = "/home/jovyan/work/data"
SILVER_INMET = f"{BASE_PATH}/silver/inmet"
SILVER_INMET_DIARIO = f"{BASE_PATH}/silver/inmet_diario"
GOLD_MUNICIPIO_ESTACAO = f"{BASE_PATH}/gold/municipio_estacao"

DATA_INICIO = "2023-01-01"
DATA_FIM = "2025-12-31"
DIAS_ESPERADOS = 1096
ESTACOES_ESPERADAS = 40
REGISTROS_DIARIOS_ESPERADOS = ESTACOES_ESPERADAS * DIAS_ESPERADOS
REGISTROS_HORARIOS_ESPERADOS = REGISTROS_DIARIOS_ESPERADOS * 24

spark = (
    SparkSession.builder
    .appName("Validacao_Silver_INMET_Diario")
    .config("spark.sql.shuffle.partitions", "40")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def validar_colunas(df, colunas, nome_dataset):
    faltantes = [c for c in colunas if c not in df.columns]
    if faltantes:
        print(f"\nERRO: colunas ausentes em {nome_dataset}:")
        for c in faltantes:
            print(f"- {c}")
        print("\nSchema encontrado:")
        df.printSchema()
        raise ValueError(
            f"{nome_dataset} não possui todas as colunas necessárias."
        )


def ler_inmet_horario():
    """
    Lê a Silver INMET horária ano a ano.

    Motivo:
    o diretório raiz possui subdiretórios 2023, 2024 e 2025 que não
    são partições Hive nomeadas no formato ano=2023. Ao tentar ler
    diretamente a raiz, o Spark detecta estruturas de diretório
    conflitantes.

    A leitura por ano evita esse conflito e preserva a descoberta de
    codigo_estacao quando ela estiver armazenada como partição dentro
    de cada diretório anual.

    Se codigo_estacao ainda não aparecer, o fallback usa
    recursiveFileLookup somente dentro daquele ano e tenta recuperar
    o código Axxx pelo caminho físico do arquivo.
    """

    print("\nLendo Silver INMET horária por ano...")

    dataframes_ano = []

    for ano in [2023, 2024, 2025]:

        caminho_ano = (
            f"{SILVER_INMET}/{ano}"
        )

        print(
            f"  - Lendo {ano}..."
        )

        try:

            df_ano = (
                spark.read
                .option(
                    "mergeSchema",
                    "true"
                )
                .option(
                    "basePath",
                    caminho_ano
                )
                .parquet(
                    caminho_ano
                )
            )

        except Exception:

            print(
                f"    Leitura particionada de {ano} "
                f"não foi possível."
            )

            print(
                "    Aplicando fallback pelo caminho "
                "físico dos arquivos..."
            )

            df_ano = None

        if (
            df_ano is not None
            and
            "codigo_estacao" in df_ano.columns
        ):

            print(
                "    ✓ codigo_estacao encontrada "
                "como coluna/partição."
            )

        else:

            df_ano = (
                spark.read
                .option(
                    "mergeSchema",
                    "true"
                )
                .option(
                    "recursiveFileLookup",
                    "true"
                )
                .parquet(
                    caminho_ano
                )
                .withColumn(
                    "_arquivo_origem",
                    input_file_name()
                )
            )

            df_ano = (
                df_ano
                .withColumn(
                    "codigo_estacao",
                    regexp_extract(
                        col(
                            "_arquivo_origem"
                        ),
                        r"codigo_estacao=([^/\\]+)",
                        1,
                    )
                )
            )

            qtd_hive = (
                df_ano
                .filter(
                    col(
                        "codigo_estacao"
                    )
                    !=
                    ""
                )
                .limit(1)
                .count()
            )

            if qtd_hive == 0:

                df_ano = (
                    df_ano
                    .withColumn(
                        "codigo_estacao",
                        regexp_extract(
                            col(
                                "_arquivo_origem"
                            ),
                            r"(A[0-9]{3})",
                            1,
                        )
                    )
                )

                qtd_path = (
                    df_ano
                    .filter(
                        col(
                            "codigo_estacao"
                        )
                        !=
                        ""
                    )
                    .limit(1)
                    .count()
                )

                if qtd_path == 0:

                    print(
                        f"\nERRO: não foi possível "
                        f"recuperar codigo_estacao "
                        f"para {ano}."
                    )

                    print(
                        "Amostra dos caminhos físicos:"
                    )

                    (
                        df_ano
                        .select(
                            "_arquivo_origem"
                        )
                        .distinct()
                        .show(
                            10,
                            truncate=False
                        )
                    )

                    raise ValueError(
                        f"Não foi possível recuperar "
                        f"codigo_estacao da Silver "
                        f"INMET {ano}."
                    )

                print(
                    "    ✓ codigo_estacao recuperada "
                    "pelo código Axxx no caminho."
                )

            else:

                print(
                    "    ✓ codigo_estacao recuperada "
                    "do caminho Hive da partição."
                )

            df_ano = (
                df_ano
                .drop(
                    "_arquivo_origem"
                )
            )

        df_ano = (
            df_ano
            .withColumn(
                "codigo_estacao",
                trim(
                    upper(
                        col(
                            "codigo_estacao"
                        ).cast("string")
                    )
                )
            )
        )

        registros_ano = (
            df_ano.count()
        )

        estacoes_ano = (
            df_ano
            .select(
                "codigo_estacao"
            )
            .distinct()
            .count()
        )

        print(
            f"    Registros: "
            f"{registros_ano}"
        )

        print(
            f"    Estações: "
            f"{estacoes_ano}"
        )

        dataframes_ano.append(
            df_ano
        )

    df_final = (
        dataframes_ano[0]
        .unionByName(
            dataframes_ano[1],
            allowMissingColumns=True
        )
        .unionByName(
            dataframes_ano[2],
            allowMissingColumns=True
        )
    )

    print(
        "✓ Silver INMET horária carregada "
        "para 2023–2025."
    )

    return df_final


print("=" * 70)
print("VALIDAÇÃO — SILVER INMET DIÁRIO")
print("=" * 70)

# ==============================================================
# 1. SILVER DIÁRIA
# ==============================================================

print("\nLendo Silver INMET Diário...")

df_diario = (
    spark.read
    .option("mergeSchema", "true")
    .parquet(SILVER_INMET_DIARIO)
)

colunas_diario = [
    "data", "codigo_estacao", "estacao", "latitude_estacao",
    "longitude_estacao", "qtd_observacoes", "precipitacao_dia_mm",
    "temperatura_media_dia_c", "temperatura_minima_dia_c",
    "temperatura_maxima_dia_c", "umidade_media_dia_pct",
    "umidade_minima_dia_pct", "umidade_maxima_dia_pct",
    "vento_medio_dia_ms", "vento_maximo_dia_ms",
]

validar_colunas(df_diario, colunas_diario, "Silver INMET Diário")

df_diario = (
    df_diario
    .withColumn("codigo_estacao", trim(upper(col("codigo_estacao"))))
    .withColumn("data", to_date(col("data")))
)

total_diario = df_diario.count()
total_estacoes = df_diario.select("codigo_estacao").distinct().count()

print(f"Registros diários: {total_diario}")
print(f"Estações: {total_estacoes}")

# ==============================================================
# 2. VALIDAÇÃO ESTRUTURAL
# ==============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO ESTRUTURAL")
print("=" * 70)

total_chaves = (
    df_diario
    .select("codigo_estacao", "data")
    .distinct()
    .count()
)
duplicidades = total_diario - total_chaves

datas = (
    df_diario
    .select(
        spark_min("data").alias("data_min"),
        spark_max("data").alias("data_max"),
    )
    .collect()[0]
)

print(f"Duplicidades: {duplicidades}")
print(f"Período: {datas['data_min']} a {datas['data_max']}")
print(f"Registros esperados: {REGISTROS_DIARIOS_ESPERADOS}")
print(f"Registros encontrados: {total_diario}")

if duplicidades != 0:
    raise ValueError("Existem duplicidades em codigo_estacao + data.")

if total_estacoes != ESTACOES_ESPERADAS:
    raise ValueError(
        f"Quantidade inesperada de estações: {total_estacoes}. "
        f"Esperado: {ESTACOES_ESPERADAS}."
    )

if total_diario != REGISTROS_DIARIOS_ESPERADOS:
    raise ValueError(
        f"Quantidade diária inesperada: {total_diario}. "
        f"Esperado: {REGISTROS_DIARIOS_ESPERADOS}."
    )

if str(datas["data_min"]) != DATA_INICIO or str(datas["data_max"]) != DATA_FIM:
    raise ValueError("Período da Silver INMET Diário diferente de 2023–2025.")

qtd_obs_invalidas = (
    df_diario
    .filter(col("qtd_observacoes") != 24)
    .count()
)
print(f"Dias com qtd_observacoes != 24: {qtd_obs_invalidas}")

if qtd_obs_invalidas != 0:
    raise ValueError(
        "Existem estação/dia com quantidade de observações diferente de 24."
    )

print("✓ Estrutura diária validada.")

# ==============================================================
# 3. COBERTURA GERAL
# ==============================================================

variaveis = [
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

print("\n" + "=" * 70)
print("COBERTURA GERAL DAS VARIÁVEIS")
print("=" * 70)

for campo in variaveis:
    nulos = df_diario.filter(col(campo).isNull()).count()
    validos = total_diario - nulos
    cobertura = validos / total_diario * 100
    print(
        f"{campo:<32} "
        f"nulos={nulos:<6} "
        f"cobertura={cobertura:6.2f}%"
    )

# ==============================================================
# 4. NULOS POR ANO
# ==============================================================

print("\n" + "=" * 70)
print("NULOS POR ANO")
print("=" * 70)

df_nulos_ano = (
    df_diario
    .withColumn("ano", year(col("data")))
    .groupBy("ano")
    .agg(
        count("*").alias("dias_estacao"),
        spark_sum(
            when(col("precipitacao_dia_mm").isNull(), 1).otherwise(0)
        ).alias("precipitacao_nula"),
        spark_sum(
            when(col("temperatura_media_dia_c").isNull(), 1).otherwise(0)
        ).alias("temperatura_nula"),
        spark_sum(
            when(col("umidade_media_dia_pct").isNull(), 1).otherwise(0)
        ).alias("umidade_nula"),
        spark_sum(
            when(col("vento_medio_dia_ms").isNull(), 1).otherwise(0)
        ).alias("vento_nulo"),
    )
    .orderBy("ano")
)

df_nulos_ano.show(truncate=False)

# ==============================================================
# 5. NULOS POR ESTAÇÃO
# ==============================================================

print("\n" + "=" * 70)
print("NULOS POR ESTAÇÃO")
print("=" * 70)

df_nulos_estacao = (
    df_diario
    .groupBy("codigo_estacao")
    .agg(
        spark_max("estacao").alias("estacao"),
        count("*").alias("dias"),
        spark_sum(
            when(col("precipitacao_dia_mm").isNull(), 1).otherwise(0)
        ).alias("precipitacao_nula"),
        spark_sum(
            when(col("temperatura_media_dia_c").isNull(), 1).otherwise(0)
        ).alias("temperatura_nula"),
        spark_sum(
            when(col("umidade_media_dia_pct").isNull(), 1).otherwise(0)
        ).alias("umidade_nula"),
        spark_sum(
            when(col("vento_medio_dia_ms").isNull(), 1).otherwise(0)
        ).alias("vento_nulo"),
    )
    .withColumn(
        "pct_precip_nula",
        spark_round(col("precipitacao_nula") / col("dias") * 100, 2),
    )
    .withColumn(
        "pct_temp_nula",
        spark_round(col("temperatura_nula") / col("dias") * 100, 2),
    )
    .withColumn(
        "pct_umidade_nula",
        spark_round(col("umidade_nula") / col("dias") * 100, 2),
    )
    .withColumn(
        "pct_vento_nulo",
        spark_round(col("vento_nulo") / col("dias") * 100, 2),
    )
)

(
    df_nulos_estacao
    .orderBy(
        col("vento_nulo").desc(),
        col("precipitacao_nula").desc(),
    )
    .show(40, truncate=False)
)

# ==============================================================
# 6. ESTAÇÕES CRÍTICAS
# ==============================================================

print("\n" + "=" * 70)
print("ESTAÇÕES SEM COBERTURA COMPLETA")
print("=" * 70)

df_estacoes_criticas = (
    df_nulos_estacao
    .filter(
        (col("temperatura_nula") == col("dias"))
        | (col("precipitacao_nula") == col("dias"))
        | (col("umidade_nula") == col("dias"))
        | (col("vento_nulo") == col("dias"))
    )
)

total_estacoes_criticas = df_estacoes_criticas.count()

print(
    "Estações sem nenhuma medição válida de alguma variável: "
    f"{total_estacoes_criticas}"
)

if total_estacoes_criticas > 0:
    df_estacoes_criticas.show(40, truncate=False)

# ==============================================================
# 7. COBERTURA HORÁRIA REAL
# ==============================================================

print("\n" + "=" * 70)
print("COBERTURA HORÁRIA REAL")
print("=" * 70)

df_horario = ler_inmet_horario()

colunas_horario = [
    "codigo_estacao",
    "data_hora_utc",
    "estacao",
    "precipitacao_mm",
    "temperatura_c",
    "umidade_pct",
    "vento_velocidade_ms",
]

validar_colunas(df_horario, colunas_horario, "Silver INMET Horária")

df_horario = (
    df_horario
    .withColumn(
        "codigo_estacao",
        trim(upper(col("codigo_estacao").cast("string"))),
    )
    .withColumn(
        "data_validacao",
        to_date(col("data_hora_utc")),
    )
    .filter(
        (col("data_validacao") >= DATA_INICIO)
        & (col("data_validacao") <= DATA_FIM)
    )
)

total_horario = df_horario.count()
total_codigos_horario = (
    df_horario
    .select("codigo_estacao")
    .distinct()
    .count()
)
total_nomes_horario = (
    df_horario
    .select("estacao")
    .distinct()
    .count()
)

print(f"Registros horários: {total_horario}")
print(f"Códigos de estação: {total_codigos_horario}")
print(f"Nomes distintos de estação: {total_nomes_horario}")

if total_horario != REGISTROS_HORARIOS_ESPERADOS:
    raise ValueError(
        f"Quantidade horária inesperada: {total_horario}. "
        f"Esperado: {REGISTROS_HORARIOS_ESPERADOS}."
    )

if total_codigos_horario != ESTACOES_ESPERADAS:
    raise ValueError(
        f"Quantidade inesperada de códigos de estação na Silver horária: "
        f"{total_codigos_horario}."
    )

# ==============================================================
# 8. 41 NOMES X 40 CÓDIGOS
# ==============================================================

print("\n" + "=" * 70)
print("NOMES DE ESTAÇÃO POR CÓDIGO")
print("=" * 70)

df_nomes_por_codigo = (
    df_horario
    .groupBy("codigo_estacao")
    .agg(
        countDistinct("estacao").alias("qtd_nomes"),
        spark_min("estacao").alias("nome_min"),
        spark_max("estacao").alias("nome_max"),
    )
    .filter(col("qtd_nomes") > 1)
    .orderBy(col("qtd_nomes").desc(), "codigo_estacao")
)

codigos_com_multiplos_nomes = df_nomes_por_codigo.count()

print(
    f"Códigos com mais de um nome no período: "
    f"{codigos_com_multiplos_nomes}"
)

if codigos_com_multiplos_nomes > 0:
    df_nomes_por_codigo.show(40, truncate=False)

# ==============================================================
# 9. OBSERVAÇÕES VÁLIDAS POR DIA
# ==============================================================

df_cobertura_horaria = (
    df_horario
    .groupBy(
        "codigo_estacao",
        "data_validacao",
    )
    .agg(
        count("*").alias("horas_totais"),
        count("precipitacao_mm").alias("horas_precipitacao"),
        count("temperatura_c").alias("horas_temperatura"),
        count("umidade_pct").alias("horas_umidade"),
        count("vento_velocidade_ms").alias("horas_vento"),
    )
)

total_dias_horario = df_cobertura_horaria.count()
print(f"Chaves estação + dia no horário: {total_dias_horario}")

if total_dias_horario != REGISTROS_DIARIOS_ESPERADOS:
    raise ValueError(
        "Quantidade de chaves estação + dia na Silver horária "
        "é diferente do esperado."
    )

dias_com_horas_totais_invalidas = (
    df_cobertura_horaria
    .filter(col("horas_totais") != 24)
    .count()
)

print(
    f"Dias com total diferente de 24 horas: "
    f"{dias_com_horas_totais_invalidas}"
)

if dias_com_horas_totais_invalidas != 0:
    raise ValueError(
        "Existem estação/dia com quantidade de registros horários "
        "diferente de 24."
    )

# ==============================================================
# 10. RESUMO DAS HORAS VÁLIDAS
# ==============================================================

print("\nHoras válidas por estação/dia:")

resumo_horas = (
    df_cobertura_horaria
    .select(
        spark_min("horas_precipitacao").alias("precip_min"),
        avg("horas_precipitacao").alias("precip_media"),
        spark_max("horas_precipitacao").alias("precip_max"),
        spark_min("horas_temperatura").alias("temp_min"),
        avg("horas_temperatura").alias("temp_media"),
        spark_max("horas_temperatura").alias("temp_max"),
        spark_min("horas_umidade").alias("umidade_min"),
        avg("horas_umidade").alias("umidade_media"),
        spark_max("horas_umidade").alias("umidade_max"),
        spark_min("horas_vento").alias("vento_min"),
        avg("horas_vento").alias("vento_media"),
        spark_max("horas_vento").alias("vento_max"),
    )
)

resumo_horas.show(truncate=False)

# ==============================================================
# 11. 0H / 1–23H / 24H
# ==============================================================

print("\nDistribuição da cobertura horária:")

metricas_horarias = [
    "horas_precipitacao",
    "horas_temperatura",
    "horas_umidade",
    "horas_vento",
]

for campo in metricas_horarias:
    zero = (
        df_cobertura_horaria
        .filter(col(campo) == 0)
        .count()
    )
    parcial = (
        df_cobertura_horaria
        .filter((col(campo) > 0) & (col(campo) < 24))
        .count()
    )
    completo = (
        df_cobertura_horaria
        .filter(col(campo) == 24)
        .count()
    )
    print(
        f"{campo:<22} "
        f"0h={zero:<6} "
        f"1–23h={parcial:<6} "
        f"24h={completo:<6}"
    )

# ==============================================================
# 12. CONSISTÊNCIA HORÁRIO X DIÁRIO
# ==============================================================

print("\n" + "=" * 70)
print("CONSISTÊNCIA ENTRE HORÁRIO E DIÁRIO")
print("=" * 70)

df_consistencia = (
    df_diario.alias("d")
    .join(
        df_cobertura_horaria.alias("h"),
        (
            (col("d.codigo_estacao") == col("h.codigo_estacao"))
            & (col("d.data") == col("h.data_validacao"))
        ),
        "left",
    )
    .select(
        col("d.codigo_estacao").alias("codigo_estacao"),
        col("d.data").alias("data"),
        col("d.precipitacao_dia_mm").alias("precipitacao_dia_mm"),
        col("d.temperatura_media_dia_c").alias("temperatura_media_dia_c"),
        col("d.umidade_media_dia_pct").alias("umidade_media_dia_pct"),
        col("d.vento_medio_dia_ms").alias("vento_medio_dia_ms"),
        col("h.horas_precipitacao").alias("horas_precipitacao"),
        col("h.horas_temperatura").alias("horas_temperatura"),
        col("h.horas_umidade").alias("horas_umidade"),
        col("h.horas_vento").alias("horas_vento"),
    )
)

metricas_consistencia = [
    ("precipitacao_dia_mm", "horas_precipitacao"),
    ("temperatura_media_dia_c", "horas_temperatura"),
    ("umidade_media_dia_pct", "horas_umidade"),
    ("vento_medio_dia_ms", "horas_vento"),
]

erros_consistencia_total = 0

for campo_diario, campo_horas in metricas_consistencia:
    inconsistencias = (
        df_consistencia
        .filter(
            (
                col(campo_diario).isNull()
                & (col(campo_horas) > 0)
            )
            |
            (
                col(campo_diario).isNotNull()
                & (col(campo_horas) == 0)
            )
        )
        .count()
    )
    erros_consistencia_total += inconsistencias
    print(
        f"{campo_diario:<32} "
        f"inconsistências={inconsistencias}"
    )

if erros_consistencia_total != 0:
    raise ValueError(
        "Existem inconsistências entre a cobertura horária "
        "e os valores diários."
    )

print(
    "✓ NULL diário é consistente com ausência total "
    "de observações válidas."
)

# ==============================================================
# 13. VALIDAÇÃO DE DOMÍNIO
# ==============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DE DOMÍNIO")
print("=" * 70)

precipitacao_negativa = (
    df_diario
    .filter(col("precipitacao_dia_mm") < 0)
    .count()
)

umidade_invalida = (
    df_diario
    .filter(
        (col("umidade_minima_dia_pct") < 0)
        | (col("umidade_maxima_dia_pct") > 100)
    )
    .count()
)

vento_negativo = (
    df_diario
    .filter(col("vento_medio_dia_ms") < 0)
    .count()
)

temperatura_suspeita = (
    df_diario
    .filter(
        (col("temperatura_minima_dia_c") < -30)
        | (col("temperatura_maxima_dia_c") > 60)
    )
    .count()
)

temperatura_ordem_invalida = (
    df_diario
    .filter(
        (col("temperatura_minima_dia_c") > col("temperatura_media_dia_c"))
        | (col("temperatura_media_dia_c") > col("temperatura_maxima_dia_c"))
    )
    .count()
)

umidade_ordem_invalida = (
    df_diario
    .filter(
        (col("umidade_minima_dia_pct") > col("umidade_media_dia_pct"))
        | (col("umidade_media_dia_pct") > col("umidade_maxima_dia_pct"))
    )
    .count()
)

print(f"Precipitação negativa: {precipitacao_negativa}")
print(f"Umidade fora de 0–100%: {umidade_invalida}")
print(f"Vento negativo: {vento_negativo}")
print(f"Temperatura fora de -30 a 60 °C: {temperatura_suspeita}")
print(f"Ordem temperatura min/média/max inválida: {temperatura_ordem_invalida}")
print(f"Ordem umidade min/média/max inválida: {umidade_ordem_invalida}")

# ==============================================================
# 14. COMPARAÇÃO COM GOLD MUNICÍPIO → ESTAÇÃO
# ==============================================================

print("\n" + "=" * 70)
print("VALIDAÇÃO DOS CÓDIGOS DE ESTAÇÃO")
print("=" * 70)

df_estacoes_gold = (
    spark.read
    .parquet(GOLD_MUNICIPIO_ESTACAO)
    .select("codigo_estacao")
    .withColumn(
        "codigo_estacao",
        trim(upper(col("codigo_estacao").cast("string"))),
    )
    .distinct()
)

df_estacoes_diario = (
    df_diario
    .select("codigo_estacao")
    .distinct()
)

gold_sem_inmet = (
    df_estacoes_gold
    .join(
        df_estacoes_diario,
        "codigo_estacao",
        "left_anti",
    )
)

inmet_sem_gold = (
    df_estacoes_diario
    .join(
        df_estacoes_gold,
        "codigo_estacao",
        "left_anti",
    )
)

qtd_gold_sem_inmet = gold_sem_inmet.count()
qtd_inmet_sem_gold = inmet_sem_gold.count()

print(
    f"Estações da Gold sem INMET diário: "
    f"{qtd_gold_sem_inmet}"
)
print(
    f"Estações INMET sem uso na Gold: "
    f"{qtd_inmet_sem_gold}"
)

if qtd_gold_sem_inmet > 0:
    print("\nEstações Gold sem INMET:")
    gold_sem_inmet.show(40, truncate=False)

if qtd_inmet_sem_gold > 0:
    print("\nEstações INMET sem Gold:")
    inmet_sem_gold.show(40, truncate=False)

# ==============================================================
# 15. RESUMO FINAL
# ==============================================================

print("\n" + "=" * 70)
print("RESUMO FINAL")
print("=" * 70)
print(f"Registros horários: {total_horario}")
print(f"Registros diários: {total_diario}")
print(f"Códigos de estação: {total_estacoes}")
print(f"Nomes distintos no horário: {total_nomes_horario}")
print(f"Códigos com múltiplos nomes: {codigos_com_multiplos_nomes}")
print(f"Duplicidades diárias: {duplicidades}")
print(f"Estações críticas: {total_estacoes_criticas}")
print(f"Inconsistências horário x diário: {erros_consistencia_total}")
print(f"Gold sem INMET: {qtd_gold_sem_inmet}")
print(f"INMET sem Gold: {qtd_inmet_sem_gold}")
print("=" * 70)
print("VALIDAÇÃO FINALIZADA")
print("=" * 70)

spark.stop()