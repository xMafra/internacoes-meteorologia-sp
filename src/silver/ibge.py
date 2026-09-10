import os
import zipfile
import shutil
import tempfile

from dbfread import DBF
import shapefile

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
)
from pyspark.sql.functions import (
    col,
    substring,
    trim,
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BRONZE_IBGE = (
    "/home/jovyan/work/data/bronze/ibge/2022/"
    "SP_Municipios_2022.zip"
)

SILVER_IBGE = (
    "/home/jovyan/work/data/silver/ibge/2022"
)


# ============================================================
# SPARK
# ============================================================

def criar_spark():
    return (
        SparkSession.builder
        .appName("TCC-Silver-IBGE")
        .getOrCreate()
    )


# ============================================================
# LEITURA DO DBF
# ============================================================

def ler_dbf_ibge(caminho_zip):
    """
    Extrai e lê o DBF da malha municipal do IBGE.

    Retorna uma lista de dicionários contendo:

        CD_MUN
        NM_MUN
        SIGLA_UF
        AREA_KM2
    """

    if not os.path.exists(caminho_zip):
        raise FileNotFoundError(
            f"Arquivo Bronze IBGE não encontrado: {caminho_zip}"
        )

    diretorio_temp = tempfile.mkdtemp(
        prefix="ibge_"
    )

    try:

        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:

            nomes = zip_ref.namelist()

            dbf_files = [
                nome
                for nome in nomes
                if nome.lower().endswith(".dbf")
            ]

            if not dbf_files:
                raise FileNotFoundError(
                    "Nenhum arquivo DBF encontrado no ZIP do IBGE."
                )

            if len(dbf_files) > 1:
                raise ValueError(
                    f"Mais de um DBF encontrado no ZIP: {dbf_files}"
                )

            nome_dbf = dbf_files[0]

            zip_ref.extract(
                nome_dbf,
                diretorio_temp
            )

        caminho_dbf = os.path.join(
            diretorio_temp,
            nome_dbf
        )

        tabela = DBF(
            caminho_dbf,
            load=False,
            encoding="utf-8"
        )

        campos_esperados = [
            "CD_MUN",
            "NM_MUN",
            "SIGLA_UF",
            "AREA_KM2"
        ]

        campos_encontrados = tabela.field_names

        if campos_encontrados != campos_esperados:
            raise ValueError(
                "Schema inesperado no DBF do IBGE.\n"
                f"Esperado: {campos_esperados}\n"
                f"Encontrado: {campos_encontrados}"
            )

        registros = []

        for registro in tabela:

            registros.append(
                {
                    "CD_MUN": (
                        str(registro["CD_MUN"]).strip()
                        if registro["CD_MUN"] is not None
                        else None
                    ),

                    "NM_MUN": (
                        str(registro["NM_MUN"]).strip()
                        if registro["NM_MUN"] is not None
                        else None
                    ),

                    "SIGLA_UF": (
                        str(registro["SIGLA_UF"]).strip()
                        if registro["SIGLA_UF"] is not None
                        else None
                    ),

                    "AREA_KM2": (
                        float(registro["AREA_KM2"])
                        if registro["AREA_KM2"] is not None
                        else None
                    )
                }
            )

        return registros

    finally:

        shutil.rmtree(
            diretorio_temp,
            ignore_errors=True
        )


# ============================================================
# LEITURA DAS COORDENADAS DO SHAPEFILE
# ============================================================

def ler_centroides_ibge(caminho_zip):
    """
    Extrai o SHP do IBGE e obtém uma coordenada
    representativa de cada município.

    Retorna:

        CD_MUN
        latitude
        longitude

    IMPORTANTE:

    O shapefile está em coordenadas geográficas.
    Portanto:

        X = longitude
        Y = latitude

    Nenhum cálculo de distância é realizado nesta camada.
    O cálculo de distância será realizado posteriormente
    na camada Gold.
    """

    diretorio_temp = tempfile.mkdtemp(
        prefix="ibge_shp_"
    )

    try:

        with zipfile.ZipFile(caminho_zip, "r") as zip_ref:

            nomes = zip_ref.namelist()

            shp_files = [
                nome
                for nome in nomes
                if nome.lower().endswith(".shp")
            ]

            if not shp_files:
                raise FileNotFoundError(
                    "Nenhum arquivo SHP encontrado no ZIP do IBGE."
                )

            if len(shp_files) > 1:
                raise ValueError(
                    f"Mais de um SHP encontrado no ZIP: {shp_files}"
                )

            nome_shp = shp_files[0]
            nome_base, _ = os.path.splitext(nome_shp)

            # Um Shapefile nÃ£o Ã© composto apenas pelo arquivo .shp. O
            # pyshp precisa tambÃ©m do Ã­ndice (.shx) e da tabela de atributos
            # (.dbf) com o mesmo nome-base para abrir os registros.
            arquivos_shape = [
                f"{nome_base}{extensao}"
                for extensao in (".shp", ".shx", ".dbf")
            ]

            arquivos_ausentes = [
                arquivo
                for arquivo in arquivos_shape
                if arquivo not in nomes
            ]

            if arquivos_ausentes:
                raise FileNotFoundError(
                    "Arquivos obrigatÃ³rios do Shapefile ausentes no ZIP: "
                    f"{arquivos_ausentes}"
                )

            for arquivo in arquivos_shape:
                zip_ref.extract(arquivo, diretorio_temp)

        caminho_shp = os.path.join(
            diretorio_temp,
            nome_shp
        )

        reader = shapefile.Reader(
            caminho_shp,
            encoding="utf-8"
        )

        campos = [
            campo[0]
            for campo in reader.fields[1:]
        ]

        if "CD_MUN" not in campos:
            raise ValueError(
                f"Campo CD_MUN não encontrado no SHP. "
                f"Campos encontrados: {campos}"
            )

        indice_cd_mun = campos.index("CD_MUN")

        registros = []

        for shape_record in reader.iterShapeRecords():

            cd_mun = shape_record.record[indice_cd_mun]

            if cd_mun is None:
                continue

            cd_mun = str(cd_mun).strip()

            shape = shape_record.shape

            # ------------------------------------------------
            # COORDENADA REPRESENTATIVA DO MUNICÍPIO
            # ------------------------------------------------
            #
            # Calculamos a média das coordenadas dos vértices
            # da geometria.
            #
            # X = longitude
            # Y = latitude
            #
            # IMPORTANTE:
            # isso NÃO é cálculo de distância.
            # A distância será calculada somente na Gold.
            # ------------------------------------------------

            pontos = shape.points

            if not pontos:

                latitude = None
                longitude = None

            else:

                soma_x = sum(
                    ponto[0]
                    for ponto in pontos
                )

                soma_y = sum(
                    ponto[1]
                    for ponto in pontos
                )

                quantidade = len(pontos)

                longitude = (
                    soma_x / quantidade
                )

                latitude = (
                    soma_y / quantidade
                )

            registros.append(
                {
                    "CD_MUN": cd_mun,
                    "latitude": latitude,
                    "longitude": longitude
                }
            )

        reader.close()

        return registros

    finally:

        shutil.rmtree(
            diretorio_temp,
            ignore_errors=True
        )


# ============================================================
# PROCESSAMENTO SILVER
# ============================================================

def processar_ibge(
    spark,
    caminho_bronze=BRONZE_IBGE,
    caminho_silver=SILVER_IBGE
):

    print("=" * 70)
    print("INICIANDO SILVER IBGE")
    print("=" * 70)

    # --------------------------------------------------------
    # Leitura Bronze
    # --------------------------------------------------------

    registros = ler_dbf_ibge(
        caminho_bronze
    )

    print(
        f"Registros lidos do DBF: {len(registros)}"
    )

    # --------------------------------------------------------
    # Leitura das coordenadas
    # --------------------------------------------------------

    coordenadas = ler_centroides_ibge(
        caminho_bronze
    )

    print(
        f"Municípios com coordenadas: {len(coordenadas)}"
    )

    # --------------------------------------------------------
    # Schema DBF
    # --------------------------------------------------------

    schema = StructType(
        [
            StructField(
                "CD_MUN",
                StringType(),
                True
            ),

            StructField(
                "NM_MUN",
                StringType(),
                True
            ),

            StructField(
                "SIGLA_UF",
                StringType(),
                True
            ),

            StructField(
                "AREA_KM2",
                DoubleType(),
                True
            )
        ]
    )

    df = spark.createDataFrame(
        registros,
        schema=schema
    )

    # --------------------------------------------------------
    # Schema das coordenadas
    # --------------------------------------------------------

    schema_coordenadas = StructType(
        [
            StructField(
                "CD_MUN",
                StringType(),
                True
            ),

            StructField(
                "latitude",
                DoubleType(),
                True
            ),

            StructField(
                "longitude",
                DoubleType(),
                True
            )
        ]
    )

    df_coordenadas = spark.createDataFrame(
        coordenadas,
        schema=schema_coordenadas
    )

    # --------------------------------------------------------
    # Padronização
    # --------------------------------------------------------

    df = (
        df

        .withColumn(
            "CD_MUN",
            trim(col("CD_MUN"))
        )

        .withColumn(
            "NM_MUN",
            trim(col("NM_MUN"))
        )

        .withColumn(
            "SIGLA_UF",
            trim(col("SIGLA_UF"))
        )

        .withColumn(
            "CD_MUN_6",
            substring(
                col("CD_MUN"),
                1,
                6
            )
        )
    )

    df_coordenadas = (
        df_coordenadas

        .withColumn(
            "CD_MUN",
            trim(col("CD_MUN"))
        )
    )

    # --------------------------------------------------------
    # JOIN DBF + SHP
    # --------------------------------------------------------

    df = (
        df

        .join(
            df_coordenadas,
            on="CD_MUN",
            how="left"
        )

        .select(
            "CD_MUN",
            "CD_MUN_6",
            "NM_MUN",
            "SIGLA_UF",
            "AREA_KM2",
            "latitude",
            "longitude"
        )
    )

    # --------------------------------------------------------
    # Escrita Silver
    # --------------------------------------------------------

    os.makedirs(
        caminho_silver,
        exist_ok=True
    )

    (
        df
        .write
        .mode("overwrite")
        .parquet(caminho_silver)
    )

    print()
    print("=" * 70)
    print("RESUMO SILVER IBGE")
    print("=" * 70)

    print(
        f"Saída: {caminho_silver}"
    )

    print("=" * 70)

    return df


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = criar_spark()

    try:

        processar_ibge(spark)

    finally:

        spark.stop()
