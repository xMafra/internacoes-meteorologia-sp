import os
import re
import zipfile
import tempfile
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    lit,
    regexp_replace,
    substring,
    trim,
    to_timestamp,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# ============================================================
# CONFIGURAÇÕES
# ============================================================

BRONZE_BASE = "/home/jovyan/work/data/bronze/inmet"
SILVER_BASE = "/home/jovyan/work/data/silver/inmet"

# TESTE INICIAL:
# Processaremos somente 2023.
# Depois da validação, alteraremos para [2023, 2024, 2025].
ANOS = [2023, 2024, 2025]


# ============================================================
# SPARK
# ============================================================

def criar_spark():
    return (
        SparkSession.builder
        .appName("TCC-Silver-INMET")
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
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "false"
        )
        .getOrCreate()
    )


# ============================================================
# CONVERSÃO DECIMAL
# ============================================================

def converter_decimal(nome_coluna):
    """
    Converte valores numéricos do INMET.

    O INMET utiliza vírgula como separador decimal.
    Valores vazios são convertidos para NULL.
    """

    return (
        regexp_replace(
            trim(col(nome_coluna)),
            ",",
            "."
        )
        .cast(DoubleType())
    )


# ============================================================
# LISTAR ARQUIVOS DE SP
# ============================================================

def listar_arquivos_sp(caminho_zip):
    """
    Retorna os arquivos CSV das estações de SP
    existentes dentro do ZIP.
    """

    with zipfile.ZipFile(caminho_zip, "r") as arquivo_zip:

        arquivos = []

        for nome in arquivo_zip.namelist():

            nome_upper = nome.upper()

            if not nome_upper.endswith(".CSV"):
                continue

            # Aceita:
            #
            # 2023/2024:
            # INMET_SE_SP_....CSV
            #
            # 2025:
            # 2025/INMET_SE_SP_....CSV

            if re.search(
                r"(^|/)INMET_SE_SP_.*\.CSV$",
                nome_upper
            ):
                arquivos.append(nome)

        arquivos.sort()

        return arquivos


# ============================================================
# EXTRAIR METADADOS
# ============================================================

def extrair_metadados(caminho_csv):
    """
    Extrai os metadados das primeiras 8 linhas
    dos arquivos CSV do INMET.
    """

    metadados = {}

    with open(
        caminho_csv,
        "r",
        encoding="latin-1",
        errors="replace"
    ) as arquivo:

        for _ in range(8):

            linha = arquivo.readline().strip()

            if not linha:
                continue

            partes = linha.split(";", 1)

            if len(partes) != 2:
                continue

            chave = partes[0].strip()
            valor = partes[1].strip()

            metadados[chave] = valor

    return metadados


# ============================================================
# PROCESSAR UM CSV
# ============================================================

def processar_csv(
    spark,
    caminho_csv,
    ano
):
    """
    Processa um arquivo CSV de uma estação INMET.
    """

    metadados = extrair_metadados(
        caminho_csv
    )

    regiao = metadados.get("REGIAO:")
    uf = metadados.get("UF:")
    estacao = metadados.get("ESTACAO:")
    codigo_estacao = metadados.get("CODIGO (WMO):")
    latitude = metadados.get("LATITUDE:")
    longitude = metadados.get("LONGITUDE:")
    altitude = metadados.get("ALTITUDE:")

    # --------------------------------------------------------
    # Converter metadados numéricos
    # --------------------------------------------------------

    latitude_num = (
        latitude.replace(",", ".")
        if latitude
        else None
    )

    longitude_num = (
        longitude.replace(",", ".")
        if longitude
        else None
    )

    altitude_num = (
        altitude.replace(",", ".")
        if altitude
        else None
    )

    # --------------------------------------------------------
    # Schema das observações
    # --------------------------------------------------------

    schema = StructType(
        [
            StructField(
                "data",
                StringType(),
                True
            ),

            StructField(
                "hora_utc",
                StringType(),
                True
            ),

            StructField(
                "precipitacao_mm",
                StringType(),
                True
            ),

            StructField(
                "pressao_estacao_mb",
                StringType(),
                True
            ),

            StructField(
                "pressao_max_mb",
                StringType(),
                True
            ),

            StructField(
                "pressao_min_mb",
                StringType(),
                True
            ),

            StructField(
                "radiacao_global_kj_m2",
                StringType(),
                True
            ),

            StructField(
                "temperatura_c",
                StringType(),
                True
            ),

            StructField(
                "temperatura_orvalho_c",
                StringType(),
                True
            ),

            StructField(
                "temperatura_max_c",
                StringType(),
                True
            ),

            StructField(
                "temperatura_min_c",
                StringType(),
                True
            ),

            StructField(
                "orvalho_max_c",
                StringType(),
                True
            ),

            StructField(
                "orvalho_min_c",
                StringType(),
                True
            ),

            StructField(
                "umidade_max_pct",
                StringType(),
                True
            ),

            StructField(
                "umidade_min_pct",
                StringType(),
                True
            ),

            StructField(
                "umidade_pct",
                StringType(),
                True
            ),

            StructField(
                "vento_direcao_graus",
                StringType(),
                True
            ),

            StructField(
                "vento_rajada_max_ms",
                StringType(),
                True
            ),

            StructField(
                "vento_velocidade_ms",
                StringType(),
                True
            ),
        ]
    )

    # --------------------------------------------------------
    # Ler CSV
    # --------------------------------------------------------

    df = (
        spark.read
        .option("header", "true")
        .option("delimiter", ";")
        .option("encoding", "ISO-8859-1")
        .option("mode", "PERMISSIVE")
        .option("nullValue", "")
        .schema(schema)
        .csv(caminho_csv)
    )

    # --------------------------------------------------------
    # Manter somente linhas meteorológicas
    # --------------------------------------------------------

    df = df.filter(
        col("data").rlike(
            r"^\d{4}/\d{2}/\d{2}$"
        )
    )

    # --------------------------------------------------------
    # Converter valores numéricos
    # --------------------------------------------------------

    colunas_decimais = [
        "precipitacao_mm",
        "pressao_estacao_mb",
        "pressao_max_mb",
        "pressao_min_mb",
        "radiacao_global_kj_m2",
        "temperatura_c",
        "temperatura_orvalho_c",
        "temperatura_max_c",
        "temperatura_min_c",
        "orvalho_max_c",
        "orvalho_min_c",
        "umidade_max_pct",
        "umidade_min_pct",
        "umidade_pct",
        "vento_direcao_graus",
        "vento_rajada_max_ms",
        "vento_velocidade_ms",
    ]

    for nome_coluna in colunas_decimais:

        df = df.withColumn(
            nome_coluna,
            converter_decimal(nome_coluna)
        )

    # --------------------------------------------------------
    # Normalizar hora
    # --------------------------------------------------------

    df = df.withColumn(
        "hora_utc",
        substring(
            trim(col("hora_utc")),
            1,
            4
        )
    )

    # --------------------------------------------------------
    # Criar timestamp
    # --------------------------------------------------------

    df = df.withColumn(
        "data_hora_utc",
        to_timestamp(
            concat_ws(
                " ",
                col("data"),
                col("hora_utc")
            ),
            "yyyy/MM/dd HHmm"
        )
    )

    # --------------------------------------------------------
    # Adicionar metadados
    # --------------------------------------------------------

    df = (
        df
        .withColumn(
            "ano",
            lit(int(ano))
        )
        .withColumn(
            "codigo_estacao",
            lit(codigo_estacao)
        )
        .withColumn(
            "estacao",
            lit(estacao)
        )
        .withColumn(
            "uf",
            lit(uf)
        )
        .withColumn(
            "regiao",
            lit(regiao)
        )
        .withColumn(
            "latitude",
            lit(latitude_num).cast(DoubleType())
        )
        .withColumn(
            "longitude",
            lit(longitude_num).cast(DoubleType())
        )
        .withColumn(
            "altitude",
            lit(altitude_num).cast(DoubleType())
        )
    )

    # --------------------------------------------------------
    # Seleção final da Silver
    # --------------------------------------------------------

    df = df.select(
        "ano",
        "data_hora_utc",
        "data",
        "hora_utc",
        "codigo_estacao",
        "estacao",
        "uf",
        "regiao",
        "latitude",
        "longitude",
        "altitude",
        "precipitacao_mm",
        "pressao_estacao_mb",
        "pressao_max_mb",
        "pressao_min_mb",
        "radiacao_global_kj_m2",
        "temperatura_c",
        "temperatura_orvalho_c",
        "temperatura_max_c",
        "temperatura_min_c",
        "orvalho_max_c",
        "orvalho_min_c",
        "umidade_max_pct",
        "umidade_min_pct",
        "umidade_pct",
        "vento_direcao_graus",
        "vento_rajada_max_ms",
        "vento_velocidade_ms",
    )

    return df


# ============================================================
# PROCESSAR UM ANO
# ============================================================

def processar_ano(
    spark,
    ano
):
    """
    Processa todas as 40 estações de SP
    de determinado ano.
    """

    caminho_zip = os.path.join(
        BRONZE_BASE,
        str(ano),
        f"{ano}.zip"
    )

    if not os.path.exists(caminho_zip):
        raise FileNotFoundError(
            f"ZIP não encontrado: {caminho_zip}"
        )

    arquivos = listar_arquivos_sp(
        caminho_zip
    )

    print()
    print("=" * 70)
    print(f"PROCESSANDO INMET {ano}")
    print("=" * 70)
    print(
        f"Arquivos de SP encontrados: "
        f"{len(arquivos)}"
    )

    if len(arquivos) != 40:
        raise ValueError(
            f"Esperadas 40 estações de SP em "
            f"{ano}, mas foram encontradas "
            f"{len(arquivos)}."
        )

    diretorio_temp = tempfile.mkdtemp(
        prefix=f"inmet_{ano}_"
    )

    try:

        # ----------------------------------------------------
        # Extrair arquivos SP
        # ----------------------------------------------------

        with zipfile.ZipFile(
            caminho_zip,
            "r"
        ) as arquivo_zip:

            caminhos_csv = []

            for nome in arquivos:

                caminho_extraido = (
                    arquivo_zip.extract(
                        nome,
                        diretorio_temp
                    )
                )

                caminhos_csv.append(
                    caminho_extraido
                )

        # ----------------------------------------------------
        # Processar estações
        # ----------------------------------------------------

        dfs = []

        for indice, caminho_csv in enumerate(
            caminhos_csv,
            start=1
        ):

            nome_arquivo = os.path.basename(
                caminho_csv
            )

            print(
                f"[{indice:02d}/40] "
                f"{nome_arquivo}"
            )

            df_estacao = processar_csv(
                spark,
                caminho_csv,
                ano
            )

            dfs.append(df_estacao)

        # ----------------------------------------------------
        # União
        # ----------------------------------------------------

        df = dfs[0]

        for outro_df in dfs[1:]:
            df = df.unionByName(
                outro_df
            )

        df.cache()

        total = df.count()

        # ----------------------------------------------------
        # Validações
        # ----------------------------------------------------

        estacoes = (
            df
            .select("codigo_estacao")
            .distinct()
            .count()
        )

        sem_data = (
            df
            .filter(
                col("data_hora_utc").isNull()
            )
            .count()
        )

        sem_codigo = (
            df
            .filter(
                col("codigo_estacao").isNull()
                | (
                    trim(
                        col("codigo_estacao")
                    ) == ""
                )
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
            row["uf"]
            for row in (
                df
                .select("uf")
                .distinct()
                .orderBy("uf")
                .collect()
            )
        ]

        data_min = (
            df
            .selectExpr(
                "min(data_hora_utc) as data_min"
            )
            .collect()[0]["data_min"]
        )

        data_max = (
            df
            .selectExpr(
                "max(data_hora_utc) as data_max"
            )
            .collect()[0]["data_max"]
        )

        duplicados = (
            df
            .groupBy(
                "codigo_estacao",
                "data_hora_utc"
            )
            .count()
            .filter(
                col("count") > 1
            )
            .count()
        )

        # ----------------------------------------------------
        # Resumo
        # ----------------------------------------------------

        print()
        print("=" * 70)
        print(f"RESUMO SILVER INMET {ano}")
        print("=" * 70)
        print(f"Registros: {total}")
        print(f"Estações distintas: {estacoes}")
        print(f"UFs: {ufs}")
        print(f"Data mínima: {data_min}")
        print(f"Data máxima: {data_max}")
        print(f"Sem data/hora: {sem_data}")
        print(
            f"Sem código da estação: "
            f"{sem_codigo}"
        )
        print(
            f"Sem latitude: "
            f"{sem_latitude}"
        )
        print(
            f"Sem longitude: "
            f"{sem_longitude}"
        )
        print(
            f"Duplicidades estação/data: "
            f"{duplicados}"
        )
        print("=" * 70)

        # ----------------------------------------------------
        # Validações obrigatórias
        # ----------------------------------------------------

        if estacoes != 40:
            raise ValueError(
                f"Esperadas 40 estações, "
                f"encontradas {estacoes}."
            )

        if sem_data > 0:
            raise ValueError(
                f"Existem {sem_data} registros "
                f"sem data/hora."
            )

        if sem_codigo > 0:
            raise ValueError(
                f"Existem {sem_codigo} registros "
                f"sem código da estação."
            )

        if sem_latitude > 0:
            raise ValueError(
                f"Existem {sem_latitude} registros "
                f"sem latitude."
            )

        if sem_longitude > 0:
            raise ValueError(
                f"Existem {sem_longitude} registros "
                f"sem longitude."
            )

        if ufs != ["SP"]:
            raise ValueError(
                f"UFs inesperadas: {ufs}"
            )

        if duplicados > 0:
            raise ValueError(
                f"Foram encontradas "
                f"{duplicados} duplicidades."
            )

        # ----------------------------------------------------
        # Escrever Silver
        # ----------------------------------------------------

        caminho_silver = os.path.join(
            SILVER_BASE,
            str(ano)
        )

        os.makedirs(
            caminho_silver,
            exist_ok=True
        )

        (
            df
            .write
            .mode("overwrite")
            .partitionBy("codigo_estacao")
            .parquet(
                caminho_silver
            )
        )

        print()
        print(
            "Silver INMET criada com sucesso:"
        )
        print(caminho_silver)

        df.unpersist()

        return total

    finally:

        shutil.rmtree(
            diretorio_temp,
            ignore_errors=True
        )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    spark = criar_spark()

    try:

        total_geral = 0

        for ano in ANOS:

            total_ano = processar_ano(
                spark,
                ano
            )

            total_geral += total_ano

        print()
        print("=" * 70)
        print("PROCESSAMENTO CONCLUÍDO")
        print("=" * 70)
        print(
            f"Total de registros processados: "
            f"{total_geral}"
        )
        print(
            f"Anos processados: {ANOS}"
        )
        print("=" * 70)

    finally:

        spark.stop()