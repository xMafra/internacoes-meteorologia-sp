import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, registrar_cobertura,
    validar_chave_unica, validar_colunas_obrigatorias, validar_condicao,
    validar_intervalo, validar_nulos, validar_periodo,
    validar_quantidade_distinta, validar_quantidade_registros, validar_tipos,
)
from src.quality.silver.common import criar_spark
from src.quality.silver.quality_config import (
    DATA_FIM, DATA_INICIO, INMET_DIARIO_ESPERADO,
    INMET_DIARIO_PATH, INMET_ESTACOES_ESPERADAS,
)


QTD_INDICADOR = {
    "qtd_obs_precipitacao": "precipitacao_dia_mm",
    "qtd_obs_temperatura": "temperatura_media_dia_c",
    "qtd_obs_umidade": "umidade_media_dia_pct",
    "qtd_obs_vento": "vento_medio_dia_ms",
}
COLUNAS = [
    "codigo_estacao", "data", "estacao", "latitude_estacao",
    "longitude_estacao", "qtd_observacoes", *QTD_INDICADOR.keys(),
    "precipitacao_dia_mm", "temperatura_media_dia_c",
    "temperatura_minima_dia_c", "temperatura_maxima_dia_c",
    "umidade_media_dia_pct", "umidade_minima_dia_pct",
    "umidade_maxima_dia_pct", "vento_medio_dia_ms", "vento_maximo_dia_ms",
]


def validar_inmet_diario(df) -> QualityReport:
    report = QualityReport("SILVER INMET DIÁRIO")
    if not validar_colunas_obrigatorias(df, COLUNAS, report):
        return report
    validar_tipos(df, {"data": "date", "qtd_observacoes": "bigint", "latitude_estacao": "double"}, report)
    validar_quantidade_registros(df, INMET_DIARIO_ESPERADO, report)
    validar_quantidade_distinta(df, "codigo_estacao", INMET_ESTACOES_ESPERADAS, report, "Estações")
    validar_chave_unica(df, ["codigo_estacao", "data"], report)
    validar_nulos(df, ["codigo_estacao", "data", "estacao", "latitude_estacao", "longitude_estacao", "qtd_observacoes"], report)
    validar_periodo(df, "data", DATA_INICIO, DATA_FIM, report)
    validar_condicao(df, F.col("qtd_observacoes") != 24, "24 observações de origem", report)
    validar_intervalo(df, "latitude_estacao", -90, 90, report)
    validar_intervalo(df, "longitude_estacao", -180, 180, report)
    for qtd, indicador in QTD_INDICADOR.items():
        validar_intervalo(df, qtd, 0, 24, report)
        inconsistente = ((F.col(qtd) == 0) & F.col(indicador).isNotNull()) | ((F.col(qtd) > 0) & F.col(indicador).isNull())
        validar_condicao(df, inconsistente, f"Consistência {qtd} x {indicador}", report)
    validar_condicao(df, F.col("precipitacao_dia_mm") < 0, "Precipitação diária não negativa", report)
    validar_intervalo(df, "umidade_media_dia_pct", 0, 100, report)
    validar_condicao(df, F.col("vento_medio_dia_ms") < 0, "Vento diário não negativo", report)
    validar_condicao(
        df,
        (F.col("temperatura_minima_dia_c") > F.col("temperatura_media_dia_c"))
        | (F.col("temperatura_media_dia_c") > F.col("temperatura_maxima_dia_c")),
        "Ordem temperatura mínima/média/máxima",
        report,
    )
    validar_condicao(
        df,
        (F.col("umidade_minima_dia_pct") > F.col("umidade_media_dia_pct"))
        | (F.col("umidade_media_dia_pct") > F.col("umidade_maxima_dia_pct")),
        "Ordem umidade mínima/média/máxima",
        report,
    )
    registrar_cobertura(df, QTD_INDICADOR.values(), report)
    return report


def main() -> int:
    spark = criar_spark("DQ-Silver-INMET-Diario")
    try:
        df = spark.read.option("mergeSchema", "true").parquet(INMET_DIARIO_PATH)
        # Evita reler milhares de pequenas partições para cada regra de DQ.
        df.cache()
        report = validar_inmet_diario(df)
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
