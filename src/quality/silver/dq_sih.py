import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, registrar_cobertura,
    validar_colunas_obrigatorias, validar_condicao, validar_intervalo,
    validar_nulos, validar_quantidade_registros, validar_tipos,
    validar_valores_aceitos,
)
from src.quality.silver.common import criar_spark
from src.quality.silver.quality_config import ANOS, IBGE_PATH, SIH_PATH, SIH_TOTAL_ESPERADO


COLUNAS = [
    "ANO_CMPT", "MES_CMPT", "MUNIC_RES", "NM_MUN", "SIGLA_UF", "NASC",
    "SEXO", "IDADE", "MORTE", "DT_INTER", "DT_SAIDA", "DIAG_PRINC",
    "VAL_SH", "VAL_SP", "VAL_TOT",
]


def validar_sih(df, df_ibge) -> QualityReport:
    report = QualityReport("SILVER SIH")
    if not validar_colunas_obrigatorias(df, COLUNAS, report):
        return report
    validar_tipos(df, {"NASC": "date", "DT_INTER": "date", "DT_SAIDA": "date", "IDADE": "bigint", "VAL_TOT": "double"}, report)
    validar_quantidade_registros(df, SIH_TOTAL_ESPERADO, report)
    validar_nulos(df, ["ANO_CMPT", "MES_CMPT", "MUNIC_RES", "NM_MUN", "SIGLA_UF", "DT_INTER", "DT_SAIDA", "DIAG_PRINC"], report)
    validar_valores_aceitos(df, "ANO_CMPT", [str(ano) for ano in ANOS], report)
    validar_intervalo(df, "MES_CMPT", 1, 12, report)
    validar_valores_aceitos(df, "SIGLA_UF", ["SP"], report)
    validar_valores_aceitos(df, "SEXO", ["1", "3"], report)
    validar_valores_aceitos(df, "MORTE", [0, 1], report)
    validar_intervalo(df, "IDADE", 0, 130, report)
    validar_condicao(df, F.col("DT_INTER") > F.col("DT_SAIDA"), "Ordem das datas de internação e saída", report)
    validar_condicao(df, ~F.col("DIAG_PRINC").rlike(r"^[A-Z][0-9]{2}[0-9A-Z]?$"), "Diagnóstico principal padronizado", report)
    for coluna in ["VAL_SH", "VAL_SP", "VAL_TOT"]:
        validar_condicao(df, F.col(coluna) < 0, f"{coluna} não negativo", report)
    municipios_validos = df_ibge.select(F.col("CD_MUN_6").alias("MUNIC_RES")).distinct()
    invalidos = df.select("MUNIC_RES").distinct().join(municipios_validos, "MUNIC_RES", "left_anti").count()
    report.add("Municípios válidos no IBGE", invalidos == 0, f"códigos sem correspondência={invalidos}", metrica=invalidos)
    volumes = {r["ANO_CMPT"]: r["count"] for r in df.groupBy("ANO_CMPT").count().orderBy("ANO_CMPT").collect()}
    report.add("Volume por ano", True, str(volumes), "INFO", str(volumes))
    registrar_cobertura(df, ["DIAG_SECUN", "CID_MORTE"], report)
    return report


def main() -> int:
    spark = criar_spark("DQ-Silver-SIH")
    try:
        df = spark.read.option("recursiveFileLookup", "true").parquet(SIH_PATH)
        report = validar_sih(df, spark.read.parquet(IBGE_PATH))
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
