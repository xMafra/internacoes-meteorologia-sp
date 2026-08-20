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
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config(
            "spark.pyspark.python",
            "/opt/conda/bin/python"
        )
        .config(
            "spark.pyspark.driver.python",
            "/opt/conda/bin/python"
        )
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
# LEITURA DOS CENTRÓIDES DO SHAPEFILE
# ============================================================

def ler_centroides_ibge(caminho_zip):
    """
    Extrai o SHP do IBGE e calcula o centróide de cada município.

    Retorna:

        CD_MUN
        latitude
        longitude

    IMPORTANTE:
    O shapefile está em coordenadas geográficas (graus),
    portanto longitude = X e latitude = Y.
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

            zip_ref.extract(
                nome_shp,
                diretorio_temp
            )

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
            # Cálculo do centróide
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

                longitude = soma_x / quantidade
                latitude = soma_y / quantidade

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
    # Leitura dos centróides
    # --------------------------------------------------------

    centroides = ler_centroides_ibge(
        caminho_bronze
    )

    print(
        f"Municípios com centróide: {len(centroides)}"
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
    # Schema dos centróides
    # --------------------------------------------------------

    schema_centroides = StructType(
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

    df_centroides = spark.createDataFrame(
        centroides,
        schema=schema_centroides
    )

    # --------------------------------------------------------
    # Padronização
    # --------------------------------------------------------

    from pyspark.sql.functions import (
        col,
        substring,
        trim,
    )

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

    df_centroides = (
        df_centroides

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
            df_centroides,
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
    # Materialização
    # --------------------------------------------------------

    df.cache()

    total = df.count()

    # --------------------------------------------------------
    # Validações
    # --------------------------------------------------------

    municipios_distintos = (
        df
        .select("CD_MUN")
        .distinct()
        .count()
    )

    codigos_6_distintos = (
        df
        .select("CD_MUN_6")
        .distinct()
        .count()
    )

    duplicados = (
        total - municipios_distintos
    )

    sem_codigo = (
        df
        .filter(
            col("CD_MUN").isNull()
            |
            (trim(col("CD_MUN")) == "")
        )
        .count()
    )

    sem_codigo_6 = (
        df
        .filter(
            col("CD_MUN_6").isNull()
            |
            (trim(col("CD_MUN_6")) == "")
        )
        .count()
    )

    sem_nome = (
        df
        .filter(
            col("NM_MUN").isNull()
            |
            (trim(col("NM_MUN")) == "")
        )
        .count()
    )

    sem_uf = (
        df
        .filter(
            col("SIGLA_UF").isNull()
            |
            (trim(col("SIGLA_UF")) == "")
        )
        .count()
    )

    sem_latitude = (
        df
        .filter(
            col("latitude").isNull()
        )
        .count()
    )

    sem_longitude = (
        df
        .filter(
            col("longitude").isNull()
        )
        .count()
    )

    ufs = [
        row["SIGLA_UF"]
        for row in (
            df
            .select("SIGLA_UF")
            .distinct()
            .orderBy("SIGLA_UF")
            .collect()
        )
    ]

    # --------------------------------------------------------
    # Validação específica de São Paulo
    # --------------------------------------------------------

    fora_sp = (
        df
        .filter(
            col("SIGLA_UF") != "SP"
        )
        .count()
    )

    codigos_fora_sp = (
        df
        .filter(
            ~col("CD_MUN_6").startswith("35")
        )
        .count()
    )

    # --------------------------------------------------------
    # Exibição
    # --------------------------------------------------------

    print()
    print("=" * 70)
    print("VALIDAÇÃO SILVER IBGE")
    print("=" * 70)

    print(
        f"Registros: {total}"
    )

    print(
        f"Municípios distintos: {municipios_distintos}"
    )

    print(
        f"Códigos 6 dígitos distintos: "
        f"{codigos_6_distintos}"
    )

    print(
        f"Duplicados: {duplicados}"
    )

    print(
        f"Sem código CD_MUN: {sem_codigo}"
    )

    print(
        f"Sem código CD_MUN_6: {sem_codigo_6}"
    )

    print(
        f"Sem nome: {sem_nome}"
    )

    print(
        f"Sem UF: {sem_uf}"
    )

    print(
        f"Sem latitude: {sem_latitude}"
    )

    print(
        f"Sem longitude: {sem_longitude}"
    )

    print(
        f"UFs: {ufs}"
    )

    print(
        f"Municípios fora de SP: {fora_sp}"
    )

    print(
        f"Códigos fora de SP: {codigos_fora_sp}"
    )

    # --------------------------------------------------------
    # Amostra
    # --------------------------------------------------------

    print()
    print("Amostra:")

    (
        df
        .select(
            "CD_MUN",
            "NM_MUN",
            "SIGLA_UF",
            "AREA_KM2",
            "latitude",
            "longitude"
        )
        .orderBy("CD_MUN")
        .show(
            10,
            truncate=False
        )
    )

    # --------------------------------------------------------
    # Verificação de integridade
    # --------------------------------------------------------

    if total != 645:

        raise ValueError(
            f"Quantidade inesperada de municípios: "
            f"{total}. Esperado: 645."
        )

    if municipios_distintos != 645:

        raise ValueError(
            "Existem municípios duplicados "
            "no dataset."
        )

    if codigos_6_distintos != 645:

        raise ValueError(
            "Os códigos de 6 dígitos "
            "não são únicos."
        )

    if sem_codigo > 0:

        raise ValueError(
            f"Existem {sem_codigo} municípios "
            "sem CD_MUN."
        )

    if sem_codigo_6 > 0:

        raise ValueError(
            f"Existem {sem_codigo_6} municípios "
            "sem CD_MUN_6."
        )

    if sem_nome > 0:

        raise ValueError(
            f"Existem {sem_nome} municípios "
            "sem nome."
        )

    if sem_uf > 0:

        raise ValueError(
            f"Existem {sem_uf} municípios "
            "sem UF."
        )

    if sem_latitude > 0:

        raise ValueError(
            f"Existem {sem_latitude} municípios "
            "sem latitude."
        )

    if sem_longitude > 0:

        raise ValueError(
            f"Existem {sem_longitude} municípios "
            "sem longitude."
        )

    if fora_sp > 0:

        raise ValueError(
            f"Existem {fora_sp} municípios "
            "fora de SP."
        )

    if codigos_fora_sp > 0:

        raise ValueError(
            f"Existem {codigos_fora_sp} códigos "
            "fora de SP."
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
        f"Registros Silver: {total}"
    )

    print(
        f"Municípios: {municipios_distintos}"
    )

    print(
        f"Códigos 6 dígitos: {codigos_6_distintos}"
    )

    print(
        f"UFs: {ufs}"
    )

    print(
        f"Latitude/longitude: "
        f"{total - sem_latitude}/{total - sem_longitude}"
    )

    print(
        f"Saída: {caminho_silver}"
    )

    print("=" * 70)

    df.unpersist()

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