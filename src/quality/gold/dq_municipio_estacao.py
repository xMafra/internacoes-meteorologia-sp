import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, validar_chave_unica,
    validar_colunas_obrigatorias, validar_condicao, validar_intervalo,
    validar_nulos, validar_quantidade_distinta, validar_quantidade_registros,
)
from src.quality.gold.common import criar_spark
from src.quality.gold.quality_config import (
    ESTACOES_ESPERADAS, MUNICIPIO_ESTACAO_PATH, MUNICIPIOS_ESPERADOS,
)


COLUNAS = [
    "CD_MUN", "NM_MUN", "SIGLA_UF", "latitude_municipio",
    "longitude_municipio", "codigo_estacao", "estacao",
    "latitude_estacao", "longitude_estacao", "distancia_km",
]


def validar_municipio_estacao(df) -> QualityReport:
    report = QualityReport("GOLD MUNICÍPIO ESTAÇÃO")
    if not validar_colunas_obrigatorias(df, COLUNAS, report):
        return report
    validar_quantidade_registros(df, MUNICIPIOS_ESPERADOS, report)
    validar_chave_unica(df, ["CD_MUN"], report)
    validar_nulos(df, COLUNAS, report)
    validar_condicao(df, F.col("distancia_km") < 0, "Distâncias não negativas", report)
    validar_intervalo(df, "latitude_municipio", -90, 90, report)
    validar_intervalo(df, "longitude_municipio", -180, 180, report)
    validar_intervalo(df, "latitude_estacao", -90, 90, report)
    validar_intervalo(df, "longitude_estacao", -180, 180, report)
    validar_quantidade_distinta(df, "codigo_estacao", ESTACOES_ESPERADAS, report, "Estações utilizadas")
    metricas = df.agg(F.avg("distancia_km").alias("media"), F.max("distancia_km").alias("maxima")).first()
    report.add("Distância média", True, f"{metricas['media']:.2f} km", "INFO", metricas["media"])
    report.add("Distância máxima", True, f"{metricas['maxima']:.2f} km", "INFO", metricas["maxima"])
    return report


def main() -> int:
    spark = criar_spark("DQ-Gold-Municipio-Estacao")
    try:
        report = validar_municipio_estacao(spark.read.parquet(MUNICIPIO_ESTACAO_PATH))
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
