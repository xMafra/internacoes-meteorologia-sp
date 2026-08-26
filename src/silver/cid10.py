from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    regexp_replace,
    substring,
    trim,
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BASE_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
)

BRONZE_PATH = BASE_PATH / "bronze" / "cid10"
SILVER_PATH = BASE_PATH / "silver" / "cid10"


ARQUIVOS = {
    "capitulos": "CID-10-CAPITULOS.CSV",
    "grupos": "CID-10-GRUPOS.CSV",
    "categorias": "CID-10-CATEGORIAS.CSV",
    "subcategorias": "CID-10-SUBCATEGORIAS.CSV",
}


# ============================================================
# SPARK
# ============================================================

spark = (
    SparkSession.builder
    .appName("TCC-Silver-CID10")
    .master("local[2]")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


# ============================================================
# FUNÇÃO DE LEITURA
# ============================================================

def ler_csv(nome_arquivo):

    caminho = BRONZE_PATH / nome_arquivo

    return (
        spark.read
        .option("header", True)
        .option("sep", ";")
        .option("encoding", "ISO-8859-1")
        .csv(str(caminho))
    )


# ============================================================
# FUNÇÃO PARA NORMALIZAR CÓDIGOS
# ============================================================

def normalizar_codigo(nome_coluna):

    return regexp_replace(
        trim(col(nome_coluna)),
        r"\.",
        ""
    )


# ============================================================
# INÍCIO
# ============================================================

print("=" * 70)
print("INICIANDO SILVER CID-10")
print("=" * 70)


# ============================================================
# LEITURA BRONZE
# ============================================================

print("\nLendo Bronze CID-10...")


df_capitulos = ler_csv(
    ARQUIVOS["capitulos"]
)

df_grupos = ler_csv(
    ARQUIVOS["grupos"]
)

df_categorias = ler_csv(
    ARQUIVOS["categorias"]
)

df_subcategorias = ler_csv(
    ARQUIVOS["subcategorias"]
)


# ============================================================
# NORMALIZAÇÃO DOS CAPÍTULOS
# ============================================================

df_capitulos = (
    df_capitulos
    .select(
        trim(col("NUMCAP")).alias(
            "codigo_capitulo"
        ),

        normalizar_codigo("CATINIC").alias(
            "cat_inicial_capitulo"
        ),

        normalizar_codigo("CATFIM").alias(
            "cat_final_capitulo"
        ),

        trim(col("DESCRICAO")).alias(
            "descricao_capitulo"
        ),

        trim(col("DESCRABREV")).alias(
            "descricao_capitulo_abreviada"
        ),
    )
)


# ============================================================
# NORMALIZAÇÃO DOS GRUPOS
# ============================================================

df_grupos = (
    df_grupos
    .select(
        normalizar_codigo("CATINIC").alias(
            "cat_inicial_grupo"
        ),

        normalizar_codigo("CATFIM").alias(
            "cat_final_grupo"
        ),

        trim(col("DESCRICAO")).alias(
            "descricao_grupo"
        ),

        trim(col("DESCRABREV")).alias(
            "descricao_grupo_abreviada"
        ),
    )
)


# ============================================================
# NORMALIZAÇÃO DAS CATEGORIAS
# ============================================================

df_categorias = (
    df_categorias
    .select(
        normalizar_codigo("CAT").alias(
            "codigo_categoria"
        ),

        trim(col("DESCRICAO")).alias(
            "descricao_categoria"
        ),

        trim(col("DESCRABREV")).alias(
            "descricao_categoria_abreviada"
        ),
    )
)


# ============================================================
# NORMALIZAÇÃO DAS SUBCATEGORIAS
# ============================================================

df_subcategorias = (
    df_subcategorias
    .select(
        normalizar_codigo("SUBCAT").alias(
            "codigo_cid"
        ),

        trim(col("DESCRICAO")).alias(
            "descricao_cid"
        ),

        trim(col("DESCRABREV")).alias(
            "descricao_cid_abreviada"
        ),

        trim(col("RESTRSEXO")).alias(
            "restricao_sexo"
        ),

        trim(col("CAUSAOBITO")).alias(
            "causa_obito"
        ),
    )
)


# ============================================================
# IDENTIFICAR CATEGORIA DA SUBCATEGORIA
# ============================================================

# Exemplos:
#
# A000 -> A00
# I500 -> I50
# O809 -> O80
#
# O código da categoria corresponde aos
# três primeiros caracteres da subcategoria.

df_subcategorias = (
    df_subcategorias
    .withColumn(
        "codigo_categoria",
        substring(
            col("codigo_cid"),
            1,
            3
        )
    )
)


# ============================================================
# RELACIONAR SUBCATEGORIA → CATEGORIA
# ============================================================

df_cid = (
    df_subcategorias

    .join(
        df_categorias,
        on="codigo_categoria",
        how="left"
    )
)


# ============================================================
# RELACIONAR CATEGORIA → GRUPO
# ============================================================

# O DATASUS fornece os grupos como intervalos:
#
# A00 - A09
# A15 - A19
# ...
#
# Em vez de separar letra e número, utilizamos
# comparação lexicográfica do código completo.
#
# Exemplo:
#
# A00 <= A05 <= A09
#
# ou:
#
# C00 <= D48 <= D48
#
# Isso também permite tratar corretamente
# intervalos que atravessam letras.


df_categorias_grupo = (
    df_categorias.alias("cat")

    .join(
        df_grupos.alias("grp"),

        (
            (col("cat.codigo_categoria")
             >= col("grp.cat_inicial_grupo"))
            &
            (col("cat.codigo_categoria")
             <= col("grp.cat_final_grupo"))
        ),

        "left"
    )

    .select(
        col("cat.codigo_categoria"),

        col("grp.cat_inicial_grupo"),
        col("grp.cat_final_grupo"),

        col("grp.descricao_grupo"),
        col("grp.descricao_grupo_abreviada"),
    )
)


# ============================================================
# RELACIONAR GRUPO → CAPÍTULO
# ============================================================

# O mesmo princípio é utilizado para os capítulos.
#
# Exemplo:
#
# Capítulo II:
# C00 - D48
#
# Uma categoria D00:
#
# C00 <= D00 <= D48
#
# Portanto pertence ao capítulo II.


df_hierarquia = (
    df_categorias_grupo.alias("cg")

    .join(
        df_capitulos.alias("cap"),

        (
            (col("cg.codigo_categoria")
             >= col("cap.cat_inicial_capitulo"))
            &
            (col("cg.codigo_categoria")
             <= col("cap.cat_final_capitulo"))
        ),

        "left"
    )

    .select(
        col("cg.codigo_categoria"),

        col("cg.cat_inicial_grupo"),
        col("cg.cat_final_grupo"),

        col("cg.descricao_grupo"),
        col("cg.descricao_grupo_abreviada"),

        col("cap.codigo_capitulo"),
        col("cap.descricao_capitulo"),
        col("cap.descricao_capitulo_abreviada"),
    )
)


# ============================================================
# MONTAGEM DA SILVER FINAL
# ============================================================

df_silver = (
    df_cid

    .join(
        df_hierarquia,
        on="codigo_categoria",
        how="left"
    )

    .select(

        # ----------------------------------------------------
        # CID
        # ----------------------------------------------------

        "codigo_cid",

        "descricao_cid",

        "descricao_cid_abreviada",


        # ----------------------------------------------------
        # CATEGORIA
        # ----------------------------------------------------

        "codigo_categoria",

        "descricao_categoria",

        "descricao_categoria_abreviada",


        # ----------------------------------------------------
        # GRUPO
        # ----------------------------------------------------

        "cat_inicial_grupo",

        "cat_final_grupo",

        "descricao_grupo",

        "descricao_grupo_abreviada",


        # ----------------------------------------------------
        # CAPÍTULO
        # ----------------------------------------------------

        "codigo_capitulo",

        "descricao_capitulo",

        "descricao_capitulo_abreviada",


        # ----------------------------------------------------
        # METADADOS DO CID
        # ----------------------------------------------------

        "restricao_sexo",

        "causa_obito",
    )
)


# ============================================================
# REMOVER DUPLICIDADES
# ============================================================

df_silver = df_silver.dropDuplicates(
    ["codigo_cid"]
)


# ============================================================
# CRIAR DIRETÓRIO
# ============================================================

SILVER_PATH.mkdir(
    parents=True,
    exist_ok=True
)


# ============================================================
# GRAVAÇÃO
# ============================================================

print("\nGravando Silver CID-10...")


(
    df_silver
    .write
    .mode("overwrite")
    .parquet(str(SILVER_PATH))
)


# ============================================================
# SAÍDA
# ============================================================

print()
print("=" * 70)
print("SILVER CID-10 CONCLUÍDA")
print("=" * 70)

print(
    f"Saída: {SILVER_PATH}"
)

print("=" * 70)


# ============================================================
# ENCERRAR SPARK
# ============================================================

spark.stop()
