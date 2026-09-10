import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, validar_chave_unica,
    validar_colunas_obrigatorias, validar_condicao, validar_intervalo,
    validar_nulos, validar_quantidade_distinta, validar_quantidade_registros,
    validar_tipos, validar_valores_aceitos,
)
from src.quality.silver.spark_session import criar_spark
from src.quality.silver.quality_config import IBGE_MUNICIPIOS_ESPERADOS, IBGE_PATH


COLUNAS = ["CD_MUN", "CD_MUN_6", "NM_MUN", "SIGLA_UF", "AREA_KM2", "latitude", "longitude"]


def validar_ibge(df) -> QualityReport:
    report = QualityReport("SILVER IBGE")
    if not validar_colunas_obrigatorias(df, COLUNAS, report):
        return report
    validar_tipos(df, {"CD_MUN": "string", "AREA_KM2": "double", "latitude": "double", "longitude": "double"}, report)
    validar_quantidade_registros(df, IBGE_MUNICIPIOS_ESPERADOS, report)
    validar_quantidade_distinta(df, "CD_MUN", IBGE_MUNICIPIOS_ESPERADOS, report, "Municípios únicos")
    validar_chave_unica(df, ["CD_MUN"], report)
    validar_nulos(df, COLUNAS, report)
    validar_valores_aceitos(df, "SIGLA_UF", ["SP"], report)
    validar_condicao(df, ~F.col("CD_MUN").rlike(r"^35\d{5}$"), "CD_MUN padronizado (7 dígitos de SP)", report)
    validar_condicao(df, ~F.col("CD_MUN_6").rlike(r"^35\d{4}$"), "CD_MUN_6 padronizado", report)
    validar_intervalo(df, "latitude", -90, 90, report)
    validar_intervalo(df, "longitude", -180, 180, report)
    validar_condicao(df, F.col("AREA_KM2") <= 0, "Área municipal positiva", report)
    return report


def main() -> int:
    spark = criar_spark("DQ-Silver-IBGE")
    try:
        report = validar_ibge(spark.read.parquet(IBGE_PATH))
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
