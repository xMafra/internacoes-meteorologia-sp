from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    trim,
    upper,
    regexp_replace,
    regexp_extract,
    length,
    when,
    substring,
    lit,
    coalesce,
)


# ==============================================================
# CONFIGURAÇÕES
# ==============================================================

BASE_PATH = "/home/jovyan/work/data"

SILVER_SIH = f"{BASE_PATH}/silver/sih"
SILVER_IBGE = f"{BASE_PATH}/silver/ibge"
SILVER_CID10 = f"{BASE_PATH}/silver/cid10"

GOLD_MUNICIPIO_ESTACAO = (
    f"{BASE_PATH}/gold/municipio_estacao"
)

GOLD_OUTPUT = (
    f"{BASE_PATH}/gold/fato_internacao"
)


# ==============================================================
# SPARK
# ==============================================================

spark = (
    SparkSession.builder
    .appName("Gold_Fato_Internacao")
    # A fato possui mais de 8 milhÃµes de registros. Desativamos broadcast
    # automÃ¡tico para impedir que o Spark exceda a memÃ³ria do driver ao
    # materializar dimensÃµes durante as validaÃ§Ãµes.
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("=" * 70)
print("INICIANDO GOLD — FATO INTERNAÇÃO")
print("=" * 70)


# ==============================================================
# 1. LEITURA SILVER SIH
# ==============================================================

print("\nLendo Silver SIH...")

df_sih = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(SILVER_SIH)
)

total_silver_sih = df_sih.count()

print(
    f"Registros encontrados na Silver SIH: "
    f"{total_silver_sih}"
)


# ==============================================================
# 2. FILTRO TEMPORAL
# ==============================================================

print("\nFiltrando período 2023–2025...")

df_sih = (
    df_sih
    .withColumn(
        "ANO_CMPT",
        col("ANO_CMPT").cast("int")
    )
    .filter(
        col("ANO_CMPT").between(2023, 2025)
    )
)

total_sih = df_sih.count()

print(
    f"Internações no período 2023–2025: "
    f"{total_sih}"
)


if total_sih == 0:
    raise ValueError(
        "Nenhum registro encontrado no período 2023–2025."
    )


# ==============================================================
# 3. PREPARAÇÃO SIH
# ==============================================================

print("\nPreparando dados da SIH...")

df_sih = (
    df_sih

    # A Silver SIH jÃ¡ possui atributos municipais enriquecidos. Nesta
    # fato, porÃ©m, o IBGE Ã© a fonte canÃ´nica desses atributos. RemovÃª-los
    # antes do JOIN evita colunas homÃ´nimas e referÃªncias ambÃ­guas.
    .drop(
        "NM_MUN",
        "SIGLA_UF",
        "AREA_KM2"
    )

    # ----------------------------------------------------------
    # MUNICÍPIO
    # ----------------------------------------------------------

    .withColumn(
        "MUNIC_RES",
        trim(
            col("MUNIC_RES").cast("string")
        )
    )

    .withColumn(
        "CD_MUN_JOIN",
        regexp_extract(
            regexp_replace(
                trim(
                    col("MUNIC_RES")
                ),
                "[^0-9]",
                ""
            ),
            "^(\\d{6})",
            1
        )
    )

    # ----------------------------------------------------------
    # DIAGNÓSTICO
    # ----------------------------------------------------------

    .withColumn(
        "DIAG_PRINC",
        trim(
            upper(
                regexp_replace(
                    col("DIAG_PRINC").cast("string"),
                    r"\.",
                    ""
                )
            )
        )
    )
)


# ==============================================================
# 4. LEITURA IBGE
# ==============================================================

print("\nLendo Silver IBGE...")


df_ibge = (
    spark.read
    .option(
        "recursiveFileLookup",
        "true"
    )
    .parquet(SILVER_IBGE)

    .select(
        "CD_MUN",
        "NM_MUN",
        "SIGLA_UF",
        "AREA_KM2",
        "latitude",
        "longitude",
    )

    .withColumn(
        "CD_MUN",
        regexp_replace(
            trim(
                col("CD_MUN").cast("string")
            ),
            "[^0-9]",
            ""
        )
    )

    .withColumn(
        "CD_MUN_JOIN",
        regexp_extract(
            col("CD_MUN"),
            "^(\\d{6})",
            1
        )
    )

    .dropDuplicates(
        ["CD_MUN_JOIN"]
    )
)


total_ibge = df_ibge.count()

print(
    f"Municípios IBGE: {total_ibge}"
)


# ==============================================================
# 5. VALIDAÇÃO SIH → IBGE
# ==============================================================

print("\nValidando municípios SIH → IBGE...")


df_municipios_sih = (
    df_sih
    .select(
        "MUNIC_RES",
        "CD_MUN_JOIN"
    )
    .where(
        col("CD_MUN_JOIN").isNotNull()
        & (col("CD_MUN_JOIN") != "")
    )
    .dropDuplicates(
        ["CD_MUN_JOIN"]
    )
)


total_municipios_sih = (
    df_municipios_sih.count()
)


df_sih_sem_ibge = (
    df_municipios_sih
    .join(
        df_ibge.select(
            "CD_MUN_JOIN"
        ),
        "CD_MUN_JOIN",
        "left_anti"
    )
)


municipios_sem_ibge = (
    df_sih_sem_ibge.count()
)


print(
    f"Municípios distintos na SIH: "
    f"{total_municipios_sih}"
)

print(
    f"Municípios sem correspondência no IBGE: "
    f"{municipios_sem_ibge}"
)


if municipios_sem_ibge > 0:

    print(
        "\nMunicípios SIH sem correspondência:"
    )

    print("\nAmostra das chaves SIH:")
    df_municipios_sih.orderBy(
        "CD_MUN_JOIN"
    ).show(
        20,
        truncate=False
    )

    print("\nAmostra das chaves IBGE:")
    df_ibge.select(
        "CD_MUN",
        "CD_MUN_JOIN",
        "NM_MUN",
        "SIGLA_UF"
    ).orderBy(
        "CD_MUN_JOIN"
    ).show(
        20,
        truncate=False
    )

    (
        df_sih_sem_ibge
        .select(
            "MUNIC_RES",
            "CD_MUN_JOIN"
        )
        .orderBy(
            "CD_MUN_JOIN"
        )
        .show(
            100,
            truncate=False
        )
    )

    raise ValueError(
        "Existem municípios da SIH "
        "sem correspondência no IBGE."
    )


print(
    "✓ Todos os municípios da SIH "
    "possuem correspondência no IBGE."
)


# ==============================================================
# 6. LEITURA GOLD MUNICÍPIO → ESTAÇÃO
# ==============================================================

print(
    "\nLendo Gold Município → Estação..."
)


df_municipio_estacao = (
    spark.read
    .option(
        "recursiveFileLookup",
        "true"
    )
    .parquet(
        GOLD_MUNICIPIO_ESTACAO
    )

    .select(
        "CD_MUN",
        "codigo_estacao",
        "estacao",
        "latitude_estacao",
        "longitude_estacao",
        "distancia_km",
    )

    .withColumn(
        "CD_MUN",
        regexp_replace(
            trim(
                col("CD_MUN").cast("string")
            ),
            "[^0-9]",
            ""
        )
    )

    .withColumn(
        "CD_MUN_JOIN",
        regexp_extract(
            col("CD_MUN"),
            "^(\\d{6})",
            1
        )
    )

    .dropDuplicates(
        ["CD_MUN_JOIN"]
    )
)


total_municipios_estacao = (
    df_municipio_estacao.count()
)


print(
    f"Municípios com estação associada: "
    f"{total_municipios_estacao}"
)


# ==============================================================
# 7. VALIDAÇÃO IBGE → ESTAÇÃO
# ==============================================================

print("\nValidando IBGE → estação...")


df_ibge_sem_estacao = (
    df_ibge
    .join(
        df_municipio_estacao.select(
            "CD_MUN_JOIN"
        ),
        "CD_MUN_JOIN",
        "left_anti"
    )
)


municipios_sem_estacao = (
    df_ibge_sem_estacao.count()
)


print(
    f"Municípios IBGE sem estação associada: "
    f"{municipios_sem_estacao}"
)


if municipios_sem_estacao > 0:

    print("\nAVISO:")

    print(
        "Existem municípios IBGE sem estação associada."
    )

    print(
        "Esses municípios receberão valores nulos "
        "nos atributos meteorológicos."
    )

else:

    print(
        "✓ Todos os municípios IBGE possuem estação associada."
    )


# ==============================================================
# 8. LEITURA SILVER CID-10
# ==============================================================

print("\nLendo Silver CID-10...")


df_cid10 = (
    spark.read
    .option(
        "recursiveFileLookup",
        "true"
    )
    .parquet(
        SILVER_CID10
    )

    .select(
        "codigo_cid",
        "descricao_cid",
        "codigo_categoria",
        "descricao_categoria",
        "cat_inicial_grupo",
        "cat_final_grupo",
        "descricao_grupo",
        "codigo_capitulo",
        "descricao_capitulo",
    )

    .withColumn(
        "codigo_cid",
        trim(
            upper(
                regexp_replace(
                    col("codigo_cid"),
                    r"\.",
                    ""
                )
            )
        )
    )

    .withColumn(
        "codigo_categoria",
        trim(
            upper(
                regexp_replace(
                    col("codigo_categoria"),
                    r"\.",
                    ""
                )
            )
        )
    )

    .dropDuplicates(
        ["codigo_cid"]
    )
)


total_cids_silver = df_cid10.count()


print(
    f"Códigos CID-10 na Silver: "
    f"{total_cids_silver}"
)


# ==============================================================
# 9. EXTENSÃO DE COMPATIBILIDADE CID-10
# ==============================================================

print(
    "\nVerificando códigos CID-10 "
    "posteriores à referência disponível..."
)


# --------------------------------------------------------------
# Alguns códigos presentes na SIH 2023–2025 não existem
# na versão histórica da CID-10 disponibilizada pelo DATASUS
# utilizada na Bronze/Silver.
#
# Não alteramos a Silver.
#
# Criamos apenas uma extensão temporária na Gold para permitir
# a integração desses códigos.
# --------------------------------------------------------------


# ==============================================================
# 9.1 CÓDIGOS C82.4 / C82.6
# ==============================================================

df_c824_c826 = (
    df_cid10
    .filter(
        col("codigo_categoria") == "C82"
    )
    .select(
        "codigo_categoria",
        "descricao_categoria",
        "cat_inicial_grupo",
        "cat_final_grupo",
        "descricao_grupo",
        "codigo_capitulo",
        "descricao_capitulo",
    )
    .dropDuplicates(
        ["codigo_categoria"]
    )
    .withColumn(
        "codigo_cid",
        when(
            col("codigo_categoria") == "C82",
            lit("C824")
        )
        .otherwise(
            lit("C826")
        )
    )
    .withColumn(
        "descricao_cid",
        when(
            col("codigo_cid") == "C824",
            lit(
                "Linfoma folicular, grau III"
            )
        )
        .otherwise(
            lit(
                "Linfoma folicular, grau III, "
                "não especificado"
            )
        )
    )
)


# ==============================================================
# 9.2 CÓDIGOS N18.2–N18.5
# ==============================================================

df_n182_n185_base = (
    df_cid10
    .filter(
        col("codigo_categoria") == "N18"
    )
    .select(
        "codigo_categoria",
        "descricao_categoria",
        "cat_inicial_grupo",
        "cat_final_grupo",
        "descricao_grupo",
        "codigo_capitulo",
        "descricao_capitulo",
    )
    .dropDuplicates(
        ["codigo_categoria"]
    )
)


df_n182_n185 = (
    df_n182_n185_base
    .crossJoin(
        spark.createDataFrame(
            [
                (
                    "N182",
                    "Doença renal crônica, estágio 2"
                ),
                (
                    "N183",
                    "Doença renal crônica, estágio 3"
                ),
                (
                    "N184",
                    "Doença renal crônica, estágio 4"
                ),
                (
                    "N185",
                    "Doença renal crônica, estágio 5"
                ),
            ],
            [
                "codigo_cid",
                "descricao_cid"
            ]
        )
    )
)


# ==============================================================
# 9.3 CÓDIGOS U09 / U099 / U10 / U109
# ==============================================================

df_u09_u10 = spark.createDataFrame(
    [
        (
            "U09",
            "Condição pós-COVID-19",
            "U09",
            "Condição pós-COVID-19",
        ),
        (
            "U099",
            "Condição pós-COVID-19, não especificada",
            "U09",
            "Condição pós-COVID-19",
        ),
        (
            "U10",
            "Síndrome inflamatória multissistêmica associada à COVID-19",
            "U10",
            "Síndrome inflamatória multissistêmica associada à COVID-19",
        ),
        (
            "U109",
            "Síndrome inflamatória multissistêmica associada à COVID-19, não especificada",
            "U10",
            "Síndrome inflamatória multissistêmica associada à COVID-19",
        ),
    ],
    [
        "codigo_cid",
        "descricao_cid",
        "codigo_categoria",
        "descricao_categoria",
    ]
)

df_u09_u10 = (
    df_u09_u10
    .withColumn(
        "cat_inicial_grupo",
        lit("U00")
    )
    .withColumn(
        "cat_final_grupo",
        lit("U99")
    )
    .withColumn(
        "descricao_grupo",
        lit(
            "Códigos para propósitos especiais"
        )
    )
    .withColumn(
        "codigo_capitulo",
        lit("22")
    )
    .withColumn(
        "descricao_capitulo",
        lit(
            "Capítulo XXII - Códigos para propósitos especiais"
        )
    )
)


# ==============================================================
# 9.4 PADRONIZAÇÃO DAS EXTENSÕES
# ==============================================================

colunas_cid = [
    "codigo_cid",
    "descricao_cid",
    "codigo_categoria",
    "descricao_categoria",
    "cat_inicial_grupo",
    "cat_final_grupo",
    "descricao_grupo",
    "codigo_capitulo",
    "descricao_capitulo",
]


df_c824_c826 = df_c824_c826.select(
    colunas_cid
)

df_n182_n185 = df_n182_n185.select(
    colunas_cid
)

df_u09_u10 = df_u09_u10.select(
    colunas_cid
)


# ==============================================================
# 9.5 UNIÃO
# ==============================================================

df_cid10_extensao = (
    df_c824_c826
    .unionByName(df_n182_n185)
    .unionByName(df_u09_u10)
)


df_cid10 = (
    df_cid10
    .unionByName(
        df_cid10_extensao
    )
    .dropDuplicates(
        ["codigo_cid"]
    )
)


total_cids_gold = df_cid10.count()


print(
    f"Códigos CID-10 disponíveis para a Gold: "
    f"{total_cids_gold}"
)

print(
    "✓ Extensão de compatibilidade CID-10 aplicada."
)


# ==============================================================
# 10. VALIDAÇÃO SIH → CID-10
# ==============================================================

print(
    "\nValidando diagnósticos "
    "SIH → CID-10..."
)


df_cids_sih = (
    df_sih
    .select(
        "DIAG_PRINC"
    )
    .where(
        col("DIAG_PRINC").isNotNull()
        & (col("DIAG_PRINC") != "")
    )
    .dropDuplicates()
)


total_cids_sih = (
    df_cids_sih.count()
)


# --------------------------------------------------------------
# Validação direta pelo código CID
# --------------------------------------------------------------

df_cids_sem_cid10_direto = (
    df_cids_sih
    .join(
        df_cid10.select(
            "codigo_cid"
        ),
        df_cids_sih["DIAG_PRINC"]
        == df_cid10["codigo_cid"],
        "left_anti"
    )
)


# --------------------------------------------------------------
# Validação também pelo código da categoria
#
# Exemplo:
#
# SIH = A00
#
# CID Silver:
# A000
# A001
# A009
#
# Nesse caso A00 é uma categoria válida.
# --------------------------------------------------------------

df_cid10_validacao = (
    df_cid10
    .select(
        "codigo_cid",
        "codigo_categoria"
    )
    .dropDuplicates()
)


df_cids_sem_correspondencia = (
    df_cids_sem_cid10_direto
    .withColumn(
        "categoria_fallback",
        when(
            col("DIAG_PRINC").rlike("^[A-Z][0-9]{3}$"),
            substring(
                col("DIAG_PRINC"),
                1,
                3
            )
        ).otherwise(
            col("DIAG_PRINC")
        )
    )
    .join(
        df_cid10_validacao,
        col("categoria_fallback")
        == df_cid10_validacao["codigo_categoria"],
        "left_anti"
    )
)


cids_sem_correspondencia = (
    df_cids_sem_correspondencia.count()
)


print(
    f"Diagnósticos distintos na SIH: "
    f"{total_cids_sih}"
)

print(
    "Diagnósticos sem correspondência "
    f"CID-10: {cids_sem_correspondencia}"
)


if cids_sem_correspondencia > 0:

    print(
        "\nDiagnósticos sem correspondência:"
    )

    (
        df_cids_sem_correspondencia
        .orderBy(
            "DIAG_PRINC"
        )
        .show(
            100,
            truncate=False
        )
    )

    raise ValueError(
        "Existem diagnósticos da SIH "
        "sem correspondência na CID-10."
    )


print(
    "✓ Todos os diagnósticos possuem "
    "correspondência CID-10."
)


# ==============================================================
# 11. JOIN SIH → IBGE
# ==============================================================

print(
    "\nRealizando JOIN SIH → IBGE..."
)


# Renomeamos as chaves de integração das dimensões antes do JOIN.
# Isso evita que o Spark mantenha duas colunas CD_MUN_JOIN com o
# mesmo nome no dataframe resultante.
df_ibge_join = (
    df_ibge
    .withColumnRenamed(
        "CD_MUN_JOIN",
        "IBGE_CD_MUN_JOIN"
    )
)

df_gold = (
    df_sih.alias("sih")
    .join(
        df_ibge_join.alias("ibge"),
        col("sih.CD_MUN_JOIN")
        == col("ibge.IBGE_CD_MUN_JOIN"),
        "inner"
    )
    .select(
        col("sih.*"),
        col("ibge.CD_MUN").alias("CD_MUN"),
        col("ibge.NM_MUN").alias("NM_MUN"),
        col("ibge.SIGLA_UF").alias("SIGLA_UF"),
        col("ibge.AREA_KM2").alias("AREA_KM2"),
        col("ibge.latitude").alias("latitude_municipio"),
        col("ibge.longitude").alias("longitude_municipio")
    )
)


# ==============================================================
# 12. JOIN MUNICÍPIO → ESTAÇÃO
# ==============================================================

print(
    "Realizando JOIN Município → Estação..."
)


df_estacao_join = (
    df_municipio_estacao
    .withColumnRenamed(
        "CD_MUN_JOIN",
        "EST_CD_MUN_JOIN"
    )
)

df_gold = (
    df_gold.alias("base")
    .join(
        df_estacao_join.alias("est"),
        col("base.CD_MUN_JOIN")
        == col("est.EST_CD_MUN_JOIN"),
        "left"
    )
    .select(
        col("base.*"),
        col("est.codigo_estacao").alias("codigo_estacao"),
        col("est.estacao").alias("estacao"),
        col("est.latitude_estacao").alias("latitude_estacao"),
        col("est.longitude_estacao").alias("longitude_estacao"),
        col("est.distancia_km").alias("distancia_km")
    )
)


# ==============================================================
# 13. JOIN SIH → CID-10
# ==============================================================

print(
    "Realizando JOIN SIH → CID-10..."
)


# ----------------------------------------------------------------
# OBJETIVO
# ----------------------------------------------------------------
#
# A SIH pode apresentar o diagnóstico em diferentes níveis:
#
#   A000  -> subcategoria CID-10
#   A00   -> categoria CID-10
#   C826  -> subcategoria que pode não existir na versão histórica
#            da CID-10 utilizada na Silver
#
# A estratégia será:
#
# 1. Tentar correspondência EXATA pelo codigo_cid.
# 2. Caso não exista, utilizar a categoria de 3 caracteres.
#
# IMPORTANTE:
# Não fazemos JOIN com OR entre codigo_cid e codigo_categoria,
# pois isso poderia gerar múltiplas correspondências e alterar
# a granularidade da fato.
# ----------------------------------------------------------------


# ==============================================================
# 13.1 DIMENSÃO CID PARA JOIN DIRETO
# ==============================================================

colunas_cid_join = [
    "codigo_cid",
    "descricao_cid",
    "codigo_categoria",
    "descricao_categoria",
    "cat_inicial_grupo",
    "cat_final_grupo",
    "descricao_grupo",
    "codigo_capitulo",
    "descricao_capitulo",
]


df_cid10_direto = (
    df_cid10
    .select(
        col("codigo_cid").alias(
            "codigo_cid_join"
        ),
        *colunas_cid_join
    )
    .where(
        col("codigo_cid_join").isNotNull()
        & (
            trim(
                col("codigo_cid_join")
            ) != ""
        )
    )
    .dropDuplicates(
        ["codigo_cid_join"]
    )
)


# ==============================================================
# 13.2 DIMENSÃO CID POR CATEGORIA
# ==============================================================

# Criamos uma única linha representativa para cada categoria.
#
# Exemplo:
#
#   C820
#   C821
#   C822
#   C823
#
# pertencem à categoria:
#
#   C82
#
# Caso o diagnóstico da SIH seja C826 e não exista C826 na Silver,
# utilizaremos essa linha de C82 como fallback.

df_cid10_categoria = (
    df_cid10
    .select(
        col("codigo_categoria").alias(
            "categoria_cid_join"
        ),
        col("codigo_categoria").alias(
            "codigo_categoria_fallback"
        ),
        col("descricao_categoria").alias(
            "descricao_categoria_fallback"
        ),
        col("cat_inicial_grupo").alias(
            "cat_inicial_grupo_fallback"
        ),
        col("cat_final_grupo").alias(
            "cat_final_grupo_fallback"
        ),
        col("descricao_grupo").alias(
            "descricao_grupo_fallback"
        ),
        col("codigo_capitulo").alias(
            "codigo_capitulo_fallback"
        ),
        col("descricao_capitulo").alias(
            "descricao_capitulo_fallback"
        ),
    )
    .where(
        col("categoria_cid_join").isNotNull()
        & (
            trim(
                col("categoria_cid_join")
            ) != ""
        )
    )
    .dropDuplicates(
        ["categoria_cid_join"]
    )
)


# ==============================================================
# 13.3 PREPARAÇÃO DA SIH PARA O FALLBACK
# ==============================================================

df_sih_cid = (
    df_gold
    .withColumn(
        "categoria_cid_sih",
        when(
            col("DIAG_PRINC").rlike(
                "^[A-Z][0-9]{2,3}"
            ),
            substring(
                col("DIAG_PRINC"),
                1,
                3
            )
        ).otherwise(
            col("DIAG_PRINC")
        )
    )
)


# ==============================================================
# 13.4 JOIN EXATO
# ==============================================================

df_gold_cid = (
    df_sih_cid.alias("base")
    .join(
        df_cid10_direto.alias("cid"),
        col("base.DIAG_PRINC")
        == col("cid.codigo_cid_join"),
        "left"
    )
    .select(
        col("base.*"),
        *[
            col(f"cid.{campo}").alias(f"direto_{campo}")
            for campo in ["codigo_cid_join", *colunas_cid_join]
        ]
    )
)


# ==============================================================
# 13.5 JOIN DE FALLBACK POR CATEGORIA
# ==============================================================

# O fallback só será utilizado quando o JOIN direto não encontrou
# um código CID.
#
# Assim:
#
# C000 -> encontra C000 diretamente.
#
# C826 -> se C826 não existir, procura C82.
#
# A00 -> pode encontrar diretamente uma linha correspondente
#        à categoria.
#
# Isso evita multiplicação de registros.

df_gold_cid = (
    df_gold_cid.alias("base")
    .join(
        df_cid10_categoria.alias("cat"),
        (
            col("base.direto_codigo_cid_join").isNull()
            &
            (
                col("base.categoria_cid_sih")
                == col("cat.categoria_cid_join")
            )
        ),
        "left"
    )
)


# ==============================================================
# 13.6 SELEÇÃO FINAL DA DIMENSÃO CID
# ==============================================================

print(
    "\nConsolidando correspondência CID-10..."
)


df_gold = df_gold_cid.select(

    # ----------------------------------------------------------
    # MANTÉM TODOS OS ATRIBUTOS DA FATO
    # ----------------------------------------------------------

    col("base.*"),

    # ----------------------------------------------------------
    # DESCRIÇÃO CID
    # ----------------------------------------------------------

    coalesce(
        col("base.direto_descricao_cid"),
        col("cat.descricao_categoria_fallback"),
        lit(
            "Descrição não disponível na fonte CID-10"
        )
    ).alias(
        "descricao_cid"
    ),

    # ----------------------------------------------------------
    # CATEGORIA
    # ----------------------------------------------------------

    coalesce(
        col("base.direto_codigo_categoria"),
        col("cat.codigo_categoria_fallback")
    ).alias(
        "codigo_categoria"
    ),

    coalesce(
        col("base.direto_descricao_categoria"),
        col("cat.descricao_categoria_fallback"),
        lit(
            "Descrição não disponível na fonte CID-10"
        )
    ).alias(
        "descricao_categoria"
    ),

    # ----------------------------------------------------------
    # GRUPO
    # ----------------------------------------------------------

    coalesce(
        col("base.direto_cat_inicial_grupo"),
        col("cat.cat_inicial_grupo_fallback")
    ).alias(
        "cat_inicial_grupo"
    ),

    coalesce(
        col("base.direto_cat_final_grupo"),
        col("cat.cat_final_grupo_fallback")
    ).alias(
        "cat_final_grupo"
    ),

    coalesce(
        col("base.direto_descricao_grupo"),
        col("cat.descricao_grupo_fallback"),
        lit(
            "Descrição não disponível na fonte CID-10"
        )
    ).alias(
        "descricao_grupo"
    ),

    # ----------------------------------------------------------
    # CAPÍTULO
    # ----------------------------------------------------------

    coalesce(
        col("base.direto_codigo_capitulo"),
        col("cat.codigo_capitulo_fallback")
    ).alias(
        "codigo_capitulo"
    ),

    coalesce(
        col("base.direto_descricao_capitulo"),
        col("cat.descricao_capitulo_fallback"),
        lit(
            "Descrição não disponível na fonte CID-10"
        )
    ).alias(
        "descricao_capitulo"
    )
)


# ==============================================================
# 13.7 REMOÇÃO DA COLUNA AUXILIAR
# ==============================================================

df_gold = (
    df_gold
    .drop(
        "categoria_cid_sih",
        *[
            f"direto_{campo}"
            for campo in ["codigo_cid_join", *colunas_cid_join]
        ]
    )
)


# ==============================================================
# 13.8 VALIDAÇÃO IMEDIATA DO JOIN CID
# ==============================================================

print(
    "\nValidando correspondência CID-10 após JOIN..."
)


cids_gold_sem_categoria = (
    df_gold
    .filter(
        col("codigo_categoria").isNull()
        |
        (
            trim(
                col("codigo_categoria")
            ) == ""
        )
    )
    .count()
)


print(
    "Registros Gold sem "
    f"codigo_categoria: {cids_gold_sem_categoria}"
)


if cids_gold_sem_categoria > 0:

    print(
        "\nDiagnósticos ainda sem categoria CID-10:"
    )

    (
        df_gold
        .filter(
            col("codigo_categoria").isNull()
            |
            (
                trim(
                    col("codigo_categoria")
                ) == ""
            )
        )
        .select(
            "DIAG_PRINC",
            "codigo_cid",
            "codigo_categoria",
            "descricao_cid"
        )
        .dropDuplicates()
        .orderBy(
            "DIAG_PRINC"
        )
        .show(
            100,
            truncate=False
        )
    )

    raise ValueError(
        "Existem registros Gold sem "
        "codigo_categoria após o JOIN CID-10."
    )


print(
    "✓ Todos os registros Gold possuem "
    "codigo_categoria."
)


# ==============================================================
# 14. SELEÇÃO DA FATO
# ==============================================================

print(
    "\nSelecionando atributos analíticos..."
)


df_gold = df_gold.select(

    # ----------------------------------------------------------
    # TEMPO
    # ----------------------------------------------------------

    col("base.ANO_CMPT").alias(
        "ano"
    ),

    col("base.MES_CMPT").alias(
        "mes"
    ),

    col("base.DT_INTER").alias(
        "data_internacao"
    ),

    col("base.DT_SAIDA").alias(
        "data_saida"
    ),


    # ----------------------------------------------------------
    # MUNICÍPIO
    # ----------------------------------------------------------

    col("base.CD_MUN").alias(
        "CD_MUN"
    ),

    col("base.NM_MUN"),

    col("base.SIGLA_UF"),

    col("base.AREA_KM2"),

    col("base.latitude_municipio"),

    col("base.longitude_municipio"),


    # ----------------------------------------------------------
    # ESTAÇÃO
    # ----------------------------------------------------------

    col("base.codigo_estacao"),

    col("base.estacao"),

    col("base.latitude_estacao"),

    col("base.longitude_estacao"),

    col("base.distancia_km"),


    # ----------------------------------------------------------
    # CID-10
    # ----------------------------------------------------------

    col("base.DIAG_PRINC").alias(
    "codigo_cid"),

    col("descricao_cid"),

    col("codigo_categoria"),

    col("descricao_categoria"),

    col("cat_inicial_grupo"),

    col("cat_final_grupo"),

    col("descricao_grupo"),

    col("codigo_capitulo"),

    col("descricao_capitulo"),


    # ----------------------------------------------------------
    # PERFIL DO PACIENTE
    # ----------------------------------------------------------

    col("base.SEXO").alias(
        "sexo"
    ),

    col("base.IDADE").alias(
        "idade"
    ),


    # ----------------------------------------------------------
    # INTERNAÇÃO
    # ----------------------------------------------------------

    col("base.DIAS_PERM").alias(
        "dias_permanencia"
    ),

    col("base.MORTE").alias(
        "obito"
    ),

    col("base.UTI_INT_TO").alias(
        "dias_uti"
    ),


    # ----------------------------------------------------------
    # VALORES FINANCEIROS
    # ----------------------------------------------------------

    col("base.VAL_TOT").alias(
        "valor_total"
    ),

    col("base.VAL_UTI").alias(
        "valor_uti"
    ),
)


# ==============================================================
# 15. VALIDAÇÃO DA GRANULARIDADE
# ==============================================================

print(
    "\nValidando granularidade..."
)


total_gold = df_gold.count()


print(
    f"Registros SIH filtrados: "
    f"{total_sih}"
)

print(
    f"Registros Gold: "
    f"{total_gold}"
)


if total_gold != total_sih:

    raise ValueError(
        "A quantidade de registros foi alterada "
        "durante os JOINs. "
        "Verifique duplicidades nas dimensões."
    )


print(
    "✓ Granularidade preservada: "
    "1 registro Gold = 1 internação SIH."
)


# ==============================================================
# 16. VALIDAÇÃO DOS MUNICÍPIOS
# ==============================================================

print(
    "\nValidando municípios na Gold..."
)


municipios_gold = (
    df_gold
    .select(
        "CD_MUN"
    )
    .distinct()
    .count()
)


print(
    f"Municípios distintos na SIH: "
    f"{total_municipios_sih}"
)

print(
    f"Municípios distintos na Gold: "
    f"{municipios_gold}"
)


if municipios_gold != total_municipios_sih:

    raise ValueError(
        "A Gold não contém todos os municípios "
        "presentes na SIH."
    )


print(
    "✓ Todos os municípios da SIH "
    "estão presentes na Gold."
)


# ==============================================================
# 17. VALIDAÇÃO EXPLÍCITA DOS MUNICÍPIOS
# ==============================================================

print(
    "\nValidando correspondência "
    "município a município..."
)


df_municipios_gold = (
    df_gold
    .select(
        substring(
            col("CD_MUN"),
            1,
            6
        ).alias("CD_MUN_JOIN")
    )
    .distinct()
)


df_municipios_gold_sem_sih = (
    df_municipios_gold
    .join(
        df_municipios_sih,
        "CD_MUN_JOIN",
        "left_anti"
    )
)


df_municipios_sih_sem_gold = (
    df_municipios_sih
    .join(
        df_municipios_gold,
        "CD_MUN_JOIN",
        "left_anti"
    )
)


municipios_gold_sem_sih = (
    df_municipios_gold_sem_sih.count()
)

municipios_sih_sem_gold = (
    df_municipios_sih_sem_gold.count()
)


print(
    f"Municípios existentes somente na Gold: "
    f"{municipios_gold_sem_sih}"
)

print(
    f"Municípios da SIH ausentes na Gold: "
    f"{municipios_sih_sem_gold}"
)


if (
    municipios_gold_sem_sih > 0
    or municipios_sih_sem_gold > 0
):

    raise ValueError(
        "A correspondência de municípios "
        "entre SIH e Gold não foi preservada."
    )


print(
    "✓ Correspondência dos municípios "
    "validada com sucesso."
)


# ==============================================================
# 18. VALIDAÇÃO DE NULOS
# ==============================================================

print(
    "\nValidando campos importantes..."
)


campos_importantes = [

    "ano",
    "mes",

    "CD_MUN",
    "NM_MUN",
    "SIGLA_UF",

    "codigo_cid",
    "descricao_cid",
    "codigo_categoria",
    "descricao_categoria",
    "descricao_grupo",
    "codigo_capitulo",
    "descricao_capitulo",
]


for campo in campos_importantes:

    quantidade_nulos = (
        df_gold
        .filter(
            col(campo).isNull()
            |
            (
                trim(
                    col(campo)
                    .cast("string")
                ) == ""
            )
        )
        .count()
    )


    print(
        f"{campo}: "
        f"{quantidade_nulos} "
        "nulos/vazios"
    )


    if quantidade_nulos > 0:

        raise ValueError(
            f"O campo {campo} "
            "possui valores nulos/vazios."
        )


# ==============================================================
# 19. VALIDAÇÃO DE DISTÂNCIA
# ==============================================================

print(
    "\nValidando distâncias..."
)


distancias_invalidas = (
    df_gold
    .filter(
        col("distancia_km").isNotNull()
        & (col("distancia_km") < 0)
    )
    .count()
)


print(
    f"Distâncias negativas: "
    f"{distancias_invalidas}"
)


if distancias_invalidas > 0:

    raise ValueError(
        "Existem distâncias negativas "
        "na Gold."
    )


# ==============================================================
# 20. ESTATÍSTICAS
# ==============================================================

print("\n" + "=" * 70)
print("ESTATÍSTICAS DA GOLD")
print("=" * 70)


# --------------------------------------------------------------
# INTERNAÇÕES POR ANO
# --------------------------------------------------------------

print(
    "\nInternações por ano:"
)

(
    df_gold
    .groupBy(
        "ano"
    )
    .count()
    .orderBy(
        "ano"
    )
    .show(
        10,
        truncate=False
    )
)


# --------------------------------------------------------------
# INTERNAÇÕES POR CAPÍTULO CID-10
# --------------------------------------------------------------

print(
    "\nInternações por capítulo CID-10:"
)

(
    df_gold
    .groupBy(
        "codigo_capitulo",
        "descricao_capitulo",
    )
    .count()
    .orderBy(
        "codigo_capitulo"
    )
    .show(
        30,
        truncate=False
    )
)


# --------------------------------------------------------------
# 10 MUNICÍPIOS COM MAIS INTERNAÇÕES
# --------------------------------------------------------------

print(
    "\n10 municípios com mais internações:"
)

(
    df_gold
    .groupBy(
        "CD_MUN",
        "NM_MUN",
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


# ==============================================================
# 21. GRAVAÇÃO
# ==============================================================

print(
    "\nGravando Gold..."
)


(
    df_gold
    .write
    .mode("overwrite")
    .parquet(
        GOLD_OUTPUT
    )
)


# ==============================================================
# 22. RESUMO FINAL
# ==============================================================

print("\n" + "=" * 70)
print("RESUMO GOLD FATO INTERNAÇÃO")
print("=" * 70)


print(
    "Período: 2023–2025"
)


print(
    f"Internações: "
    f"{total_gold}"
)


print(
    f"Municípios: "
    f"{municipios_gold}"
)


print(
    f"Códigos CID disponíveis: "
    f"{total_cids_gold}"
)


print(
    f"Códigos CID originais da Silver: "
    f"{total_cids_silver}"
)


print(
    f"Códigos CID adicionados na extensão: "
    f"{total_cids_gold - total_cids_silver}"
)


total_estacoes = (
    df_gold
    .select(
        "codigo_estacao"
    )
    .where(
        col("codigo_estacao").isNotNull()
    )
    .distinct()
    .count()
)


print(
    f"Estações meteorológicas: "
    f"{total_estacoes}"
)


distancia_media = (
    df_gold
    .agg(
        avg("distancia_km")
    )
    .first()[0]
)


if distancia_media is not None:

    print(
        f"Distância média "
        f"município-estação: "
        f"{distancia_media:.2f} km"
    )

else:

    print(
        "Distância média "
        "município-estação: N/A"
    )


print(
    "\nColunas:"
)

print(
    df_gold.columns
)


print(
    "\nAmostra:"
)

(
    df_gold
    .select(
        "ano",
        "mes",
        "CD_MUN",
        "NM_MUN",
        "codigo_estacao",
        "codigo_cid",
        "descricao_categoria",
        "descricao_grupo",
        "codigo_capitulo",
        "descricao_capitulo",
        "idade",
        "sexo",
        "dias_permanencia",
        "obito",
    )
    .show(
        10,
        truncate=False
    )
)


print("\n" + "=" * 70)
print("GOLD FATO INTERNAÇÃO CONCLUÍDA")
print("=" * 70)


print(
    f"Saída: {GOLD_OUTPUT}"
)


spark.stop()
