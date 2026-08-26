import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, registrar_cobertura,
    validar_chave_unica, validar_colunas_obrigatorias, validar_condicao,
    validar_intervalo, validar_nulos, validar_periodo,
    validar_quantidade_distinta, validar_quantidade_registros, validar_tipos,
    validar_valores_aceitos,
)
from src.quality.silver.common import criar_spark
from src.quality.silver.quality_config import (
    ANOS, DATA_FIM, DATA_INICIO, INMET_ESTACOES_ESPERADAS,
    INMET_HORARIO_ESPERADO, INMET_PATH,
)


COLUNAS = [
    "codigo_estacao", "data_hora_utc", "estacao", "uf", "latitude",
    "longitude", "precipitacao_mm", "temperatura_c", "umidade_pct",
    "vento_direcao_graus", "vento_velocidade_ms",
]
VARIAVEIS = ["precipitacao_mm", "temperatura_c", "umidade_pct", "vento_velocidade_ms"]


def validar_inmet(df) -> QualityReport:
    report = QualityReport("SILVER INMET HORÁRIO")
    if not validar_colunas_obrigatorias(df, COLUNAS, report):
        return report
    validar_tipos(df, {"data_hora_utc": "timestamp", "latitude": "double", "longitude": "double"}, report)
    validar_quantidade_registros(df, INMET_HORARIO_ESPERADO, report)
    validar_quantidade_distinta(df, "codigo_estacao", INMET_ESTACOES_ESPERADAS, report, "Códigos de estação")
    validar_chave_unica(df, ["codigo_estacao", "data_hora_utc"], report)
    validar_nulos(df, ["codigo_estacao", "data_hora_utc", "estacao", "uf", "latitude", "longitude"], report)
    validar_periodo(df, "data_hora_utc", f"{DATA_INICIO} 00:00:00", f"{DATA_FIM} 23:00:00", report)
    validar_valores_aceitos(df, "uf", ["SP"], report)
    validar_intervalo(df, "latitude", -90, 90, report)
    validar_intervalo(df, "longitude", -180, 180, report)
    validar_condicao(df, F.col("precipitacao_mm") < 0, "Precipitação não negativa", report)
    validar_intervalo(df, "umidade_pct", 0, 100, report)
    validar_intervalo(df, "vento_direcao_graus", 0, 360, report)
    validar_condicao(df, F.col("vento_velocidade_ms") < 0, "Vento não negativo", report)
    validar_intervalo(df, "temperatura_c", -30, 60, report, "WARNING")
    por_dia = df.groupBy("codigo_estacao", F.to_date("data_hora_utc").alias("data")).count()
    validar_condicao(por_dia, F.col("count") != 24, "24 registros por dia/estação", report)
    validar_quantidade_registros(por_dia, 43_840, report)
    registrar_cobertura(df, VARIAVEIS, report)
    return report


def main() -> int:
    spark = criar_spark("DQ-Silver-INMET-Horario")
    try:
        dataframes = [
            spark.read.option("mergeSchema", "true").parquet(f"{INMET_PATH}/{ano}")
            for ano in ANOS
        ]
        df = dataframes[0]
        for df_ano in dataframes[1:]:
            df = df.unionByName(df_ano, allowMissingColumns=True)
        report = validar_inmet(df)
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
