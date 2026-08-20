from pathlib import Path
import tempfile
from zipfile import ZipFile

import pyarrow as pa
import pyarrow.parquet as pq
from dbfread import DBF
from pyreaddbc.readdbc import dbc2dbf
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


TAMANHO_LOTE = 20_000

CAMINHO_BRONZE_SIH = (
    "/home/jovyan/work/data/bronze/sih"
)

CAMINHO_IBGE = (
    "/home/jovyan/work/data/bronze/ibge/2022/"
    "SP_Municipios_2022.zip"
)

CAMINHO_SILVER_SIH = (
    "/home/jovyan/work/data/silver/sih"
)


def criar_spark() -> SparkSession:
    """Cria uma sessão Spark local para processamento do SIH."""
    return (
        SparkSession.builder
        .appName("TCC-Silver-SIH")
        .master("local[2]")
        .getOrCreate()
    )


def obter_caminho_dbc(ano: int, mes: int) -> Path:
    """
    Retorna o caminho do arquivo DBC do SIH para determinado ano/mês.
    """

    nome_arquivo = f"RDSP{str(ano)[-2:]}{mes:02d}.dbc"

    return (
        Path(CAMINHO_BRONZE_SIH)
        / str(ano)
        / nome_arquivo
    )


def obter_caminho_dbf_temporario(
    ano: int,
    mes: int,
) -> Path:
    """Retorna o caminho do DBF temporário."""

    return (
        Path("/tmp")
        / f"RDSP{str(ano)[-2:]}{mes:02d}.dbf"
    )


def obter_caminho_parquet_temporario(
    ano: int,
    mes: int,
) -> Path:
    """Retorna o caminho do Parquet temporário."""

    return (
        Path("/tmp")
        / f"RDSP{str(ano)[-2:]}{mes:02d}.parquet"
    )


def obter_caminho_silver(
    ano: int,
    mes: int,
) -> Path:
    """Retorna o diretório da Silver para ano/mês."""

    return (
        Path(CAMINHO_SILVER_SIH)
        / str(ano)
        / f"{mes:02d}"
    )


def converter_dbc_para_dbf(
    caminho_dbc: str | Path,
    caminho_dbf: str | Path,
) -> None:
    """Converte um arquivo DBC do SIH para DBF."""

    print(f"Convertendo DBC para DBF:")
    print(f"Origem: {caminho_dbc}")
    print(f"Destino: {caminho_dbf}")

    dbc2dbf(
        str(caminho_dbc),
        str(caminho_dbf),
    )

    print("Conversão DBC → DBF concluída")


def converter_dbf_para_parquet(
    caminho_dbf: str | Path,
    caminho_parquet: str | Path,
    tamanho_lote: int = TAMANHO_LOTE,
) -> None:
    """
    Converte um DBF do SIH para Parquet em lotes.

    A leitura do DBF é feita de forma incremental para evitar
    carregar todos os registros simultaneamente na memória.
    """

    tabela_dbf = DBF(
        caminho_dbf,
        load=False,
        encoding="latin-1",
    )

    iterador = iter(tabela_dbf)
    escritor = None
    total_registros = 0
    numero_lote = 0

    try:
        while True:
            lote = []

            for _ in range(tamanho_lote):
                try:
                    lote.append(next(iterador))
                except StopIteration:
                    break

            if not lote:
                break

            tabela_arrow = pa.Table.from_pylist(lote)

            if escritor is None:
                escritor = pq.ParquetWriter(
                    str(caminho_parquet),
                    tabela_arrow.schema,
                    compression="snappy",
                )

            escritor.write_table(tabela_arrow)

            numero_lote += 1
            total_registros += len(lote)

            print(
                f"Lote {numero_lote}: "
                f"{len(lote)} registros | "
                f"total: {total_registros}"
            )

    finally:
        if escritor is not None:
            escritor.close()

    print(
        f"Conversão DBF → Parquet concluída: "
        f"{total_registros} registros"
    )
    print(f"Arquivo Parquet: {caminho_parquet}")


def ler_parquet_sih(
    caminho_parquet: str | Path,
    spark: SparkSession,
) -> DataFrame:
    """Lê o Parquet do SIH utilizando Spark."""

    return spark.read.parquet(
        str(caminho_parquet)
    )


def normalizar_strings_vazias(
    df: DataFrame,
) -> DataFrame:
    """
    Converte strings vazias ou contendo apenas espaços em NULL.
    """

    colunas_string = [
        campo.name
        for campo in df.schema.fields
        if campo.dataType.simpleString() == "string"
    ]

    for coluna in colunas_string:
        df = df.withColumn(
            coluna,
            F.when(
                F.trim(F.col(coluna)) == "",
                F.lit(None),
            ).otherwise(F.col(coluna)),
        )

    return df


def converter_datas(
    df: DataFrame,
) -> DataFrame:
    """
    Converte as colunas de data do SIH para o tipo DATE.

    As datas de origem estão no formato YYYYMMDD.
    """

    colunas_data = [
        "DT_INTER",
        "DT_SAIDA",
        "NASC",
    ]

    for coluna in colunas_data:
        df = df.withColumn(
            coluna,
            F.to_date(
                F.col(coluna),
                "yyyyMMdd",
            ),
        )

    return df


def ler_municipios_ibge(
    spark: SparkSession,
) -> DataFrame:
    """
    Lê a tabela DBF da malha municipal do IBGE.

    O código oficial do IBGE possui 7 dígitos. Para compatibilizar
    com MUNIC_RES do SIH, utilizamos os 6 primeiros dígitos.
    """

    with ZipFile(CAMINHO_IBGE) as arquivo_zip:
        dados_dbf = arquivo_zip.read(
            "SP_Municipios_2022.dbf"
        )

    arquivo_temporario = tempfile.NamedTemporaryFile(
        suffix=".dbf",
        delete=False,
    )

    try:
        arquivo_temporario.write(dados_dbf)
        arquivo_temporario.close()

        tabela = DBF(
            arquivo_temporario.name,
            load=False,
            encoding="utf-8",
        )

        registros = list(tabela)

    finally:
        Path(
            arquivo_temporario.name
        ).unlink(
            missing_ok=True
        )

    dados = [
        {
            "CD_MUN": str(
                registro["CD_MUN"]
            )[:6],
            "NM_MUN": registro["NM_MUN"],
            "SIGLA_UF": registro["SIGLA_UF"],
            "AREA_KM2": float(
                registro["AREA_KM2"]
            ),
        }
        for registro in registros
    ]

    return spark.createDataFrame(dados)


def juntar_sih_municipios(
    df_sih: DataFrame,
    df_municipios: DataFrame,
) -> DataFrame:
    """
    Realiza LEFT JOIN entre SIH e municípios do IBGE.

    MUNIC_RES do SIH é comparado ao código municipal do IBGE
    normalizado para 6 dígitos.
    """

    municipios = df_municipios.select(
        F.col("CD_MUN").alias(
            "MUNIC_RES_IBGE"
        ),
        F.col("NM_MUN"),
        F.col("SIGLA_UF"),
        F.col("AREA_KM2"),
    )

    return (
        df_sih
        .join(
            municipios,
            df_sih["MUNIC_RES"]
            == municipios["MUNIC_RES_IBGE"],
            "left",
        )
        .drop("MUNIC_RES_IBGE")
    )


def filtrar_municipios_sp(
    df: DataFrame,
) -> DataFrame:
    """
    Mantém somente registros com correspondência
    na malha municipal de São Paulo.
    """

    return df.filter(
        F.col("NM_MUN").isNotNull()
    )


def salvar_silver(
    df: DataFrame,
    caminho_saida: str | Path,
) -> None:
    """
    Salva a camada Silver em formato Parquet
    com compressão Snappy.
    """

    (
        df.write
        .mode("overwrite")
        .option(
            "compression",
            "snappy",
        )
        .parquet(
            str(caminho_saida)
        )
    )

    print(
        f"Silver salva em: {caminho_saida}"
    )


def processar_sih(
    ano: int,
    mes: int,
    spark: SparkSession,
) -> None:
    """
    Executa o pipeline completo de processamento
    de um mês do SIH.

    Fluxo:

    DBC
      ↓
    DBF
      ↓
    Parquet
      ↓
    Spark
      ↓
    Normalização
      ↓
    Datas
      ↓
    JOIN IBGE
      ↓
    Filtro SP
      ↓
    Silver Parquet
    """

    print()
    print("=" * 60)
    print(
        f"PROCESSANDO SIH {ano}/{mes:02d}"
    )
    print("=" * 60)

    caminho_dbc = obter_caminho_dbc(
        ano,
        mes,
    )

    caminho_dbf = obter_caminho_dbf_temporario(
        ano,
        mes,
    )

    caminho_parquet = obter_caminho_parquet_temporario(
        ano,
        mes,
    )

    caminho_silver = obter_caminho_silver(
        ano,
        mes,
    )

    # --------------------------------------------------------------
    # 1. Validação do DBC
    # --------------------------------------------------------------

    if not caminho_dbc.exists():
        raise FileNotFoundError(
            f"Arquivo DBC não encontrado: "
            f"{caminho_dbc}"
        )

    print(
        f"Arquivo DBC: {caminho_dbc}"
    )

    print(
        f"Tamanho DBC: "
        f"{caminho_dbc.stat().st_size} bytes"
    )

    # --------------------------------------------------------------
    # 2. DBC → DBF
    # --------------------------------------------------------------

    converter_dbc_para_dbf(
        caminho_dbc,
        caminho_dbf,
    )

    # --------------------------------------------------------------
    # 3. DBF → Parquet
    # --------------------------------------------------------------

    converter_dbf_para_parquet(
        caminho_dbf,
        caminho_parquet,
    )

    # --------------------------------------------------------------
    # 4. Leitura com Spark
    # --------------------------------------------------------------

    df = ler_parquet_sih(
        caminho_parquet,
        spark,
    )

    registros_antes = df.count()

    print(
        "Registros antes das transformações:",
        registros_antes,
    )

    print(
        "Colunas:",
        len(df.columns),
    )

    # --------------------------------------------------------------
    # 5. Normalização de strings
    # --------------------------------------------------------------

    df = normalizar_strings_vazias(
        df
    )

    print(
        "Strings vazias normalizadas para NULL"
    )

    # --------------------------------------------------------------
    # 6. Conversão de datas
    # --------------------------------------------------------------

    df = converter_datas(
        df
    )

    print(
        "Datas convertidas para DATE"
    )

    # --------------------------------------------------------------
    # 7. Leitura dos municípios IBGE
    # --------------------------------------------------------------

    df_municipios = ler_municipios_ibge(
        spark
    )

    total_municipios_ibge = (
        df_municipios.count()
    )

    print(
        "Municípios IBGE:",
        total_municipios_ibge,
    )

    # --------------------------------------------------------------
    # 8. JOIN SIH + IBGE
    # --------------------------------------------------------------

    df_join = juntar_sih_municipios(
        df,
        df_municipios,
    )

    registros_apos_join = (
        df_join.count()
    )

    print(
        "Registros após JOIN:",
        registros_apos_join,
    )

    # --------------------------------------------------------------
    # 9. Métricas de correspondência
    # --------------------------------------------------------------

    registros_com_municipio = (
        df_join
        .filter(
            F.col("NM_MUN").isNotNull()
        )
        .count()
    )

    registros_sem_municipio = (
        df_join
        .filter(
            F.col("NM_MUN").isNull()
        )
        .count()
    )

    print(
        "Com município:",
        registros_com_municipio,
    )

    print(
        "Sem município:",
        registros_sem_municipio,
    )

    # --------------------------------------------------------------
    # 10. Filtro territorial
    # --------------------------------------------------------------

    df_silver = filtrar_municipios_sp(
        df_join
    )

    registros_silver = (
        df_silver.count()
    )

    print(
        "Registros Silver:",
        registros_silver,
    )

    # --------------------------------------------------------------
    # 11. Validação
    # --------------------------------------------------------------

    if registros_apos_join != registros_antes:
        raise RuntimeError(
            "O JOIN alterou a quantidade de registros. "
            "Verifique possível duplicidade na dimensão IBGE."
        )

    if (
        registros_com_municipio
        + registros_sem_municipio
        != registros_antes
    ):
        raise RuntimeError(
            "A soma dos registros com e sem município "
            "não corresponde ao total original."
        )

    if registros_silver != registros_com_municipio:
        raise RuntimeError(
            "Quantidade da Silver diferente da "
            "quantidade de registros com município."
        )

    # --------------------------------------------------------------
    # 12. Salvamento da Silver
    # --------------------------------------------------------------

    salvar_silver(
        df_silver,
        caminho_silver,
    )

    # --------------------------------------------------------------
    # 13. Resumo
    # --------------------------------------------------------------

    print()
    print(
        f"===== RESUMO SILVER SIH {ano}/{mes:02d} ====="
    )

    print(
        "Registros antes:",
        registros_antes,
    )

    print(
        "Registros após JOIN:",
        registros_apos_join,
    )

    print(
        "Com município:",
        registros_com_municipio,
    )

    print(
        "Sem município:",
        registros_sem_municipio,
    )

    print(
        "Registros Silver:",
        registros_silver,
    )

    print(
        "Municípios IBGE:",
        total_municipios_ibge,
    )

    print(
        "==========================================="
    )

    # --------------------------------------------------------------
    # 14. Limpeza dos arquivos temporários
    # --------------------------------------------------------------

    if caminho_dbf.exists():
        caminho_dbf.unlink()
        print(
            f"DBF temporário removido: "
            f"{caminho_dbf}"
        )

    if caminho_parquet.exists():
        caminho_parquet.unlink()
        print(
            f"Parquet temporário removido: "
            f"{caminho_parquet}"
        )


if __name__ == "__main__":
    spark = criar_spark()

    try:
        processar_sih(
            ano=2023,
            mes=3,
            spark=spark,
        )

    finally:
        spark.stop()