import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, validar_chave_unica,
    validar_colunas_obrigatorias, validar_condicao, validar_nulos,
    validar_quantidade_registros, validar_tipos, validar_valores_aceitos,
)
from src.quality.gold.common import criar_spark
from src.quality.gold.quality_config import (
    DATA_INMET_FIM, DATA_INMET_INICIO, FATO_INTERNACAO_PATH,
    FATO_METEOROLOGIA_PATH, MIN_COBERTURA_PRECIPITACAO,
    MIN_COBERTURA_TEMPERATURA, MIN_COBERTURA_UMIDADE,
    MIN_COBERTURA_VENTO, SILVER_INMET_DIARIO_PATH,
    TOTAL_INTERNACOES_ESPERADO,
)


ESSENCIAIS = [
    "data_internacao", "CD_MUN", "codigo_estacao", "codigo_cid",
    "meteorologia_disponivel", "motivo_sem_meteorologia",
    "qtd_observacoes_inmet", "qtd_obs_precipitacao",
    "qtd_obs_temperatura", "qtd_obs_umidade", "qtd_obs_vento",
]
COBERTURAS = {
    "temperatura": ("temperatura_media_dia_c", MIN_COBERTURA_TEMPERATURA),
    "umidade": ("umidade_media_dia_pct", MIN_COBERTURA_UMIDADE),
    "precipitacao": ("precipitacao_dia_mm", MIN_COBERTURA_PRECIPITACAO),
    "vento": ("vento_medio_dia_ms", MIN_COBERTURA_VENTO),
}


def validar_fato_meteorologia(df, df_fato, df_inmet) -> QualityReport:
    report = QualityReport("GOLD FATO INTERNAÇÃO METEOROLOGIA")
    if not validar_colunas_obrigatorias(df, ESSENCIAIS + [item[0] for item in COBERTURAS.values()], report):
        return report
    validar_tipos(df, {"data_internacao": "date", "meteorologia_disponivel": "int", "qtd_observacoes_inmet": "bigint"}, report)
    total = validar_quantidade_registros(df, TOTAL_INTERNACOES_ESPERADO, report)
    total_fato = df_fato.count()
    report.add("Preservação da fato base", total == total_fato, f"fato base={total_fato:,}, fato meteorológica={total:,}")
    validar_nulos(df, ["data_internacao", "CD_MUN", "codigo_estacao", "codigo_cid", "meteorologia_disponivel"], report)
    validar_valores_aceitos(df, "meteorologia_disponivel", [0, 1], report)

    validar_chave_unica(df_inmet, ["codigo_estacao", "data"], report)
    estacoes_fato = df.select("codigo_estacao").distinct()
    estacoes_inmet = df_inmet.select("codigo_estacao").distinct()
    fato_sem_inmet = estacoes_fato.join(estacoes_inmet, "codigo_estacao", "left_anti").count()
    inmet_sem_fato = estacoes_inmet.join(estacoes_fato, "codigo_estacao", "left_anti").count()
    report.add("Estações da fato presentes no INMET", fato_sem_inmet == 0, f"ausentes={fato_sem_inmet}")
    report.add("Estações INMET utilizadas na fato", inmet_sem_fato == 0, f"sem uso={inmet_sem_fato}")

    dentro_periodo = F.col("data_internacao").between(DATA_INMET_INICIO, DATA_INMET_FIM)
    sem_match_dentro = df.filter(dentro_periodo & (F.col("meteorologia_disponivel") == 0)).count()
    report.add("Sem match dentro do período INMET", sem_match_dentro == 0, f"internações={sem_match_dentro}")
    fora_periodo = df.filter(~dentro_periodo).count()
    report.add("Sem meteorologia por data fora do período", True, f"internações={fora_periodo:,}", "INFO", fora_periodo)

    validar_condicao(
        df,
        ((F.col("meteorologia_disponivel") == 1) & F.col("motivo_sem_meteorologia").isNotNull())
        | ((F.col("meteorologia_disponivel") == 0) & F.col("motivo_sem_meteorologia").isNull()),
        "Coerência flag x motivo sem meteorologia",
        report,
    )
    validar_condicao(
        df,
        (F.col("meteorologia_disponivel") == 1) & (F.col("qtd_observacoes_inmet") != 24),
        "Matches com 24 observações de origem",
        report,
    )
    validar_condicao(
        df,
        (F.col("meteorologia_disponivel") == 0) & F.col("qtd_observacoes_inmet").isNotNull(),
        "Ausência de métricas quando não há match",
        report,
    )

    matches = df.filter(F.col("meteorologia_disponivel") == 1)
    total_matches = matches.count()
    report.add("Internações com meteorologia", True, f"{total_matches:,}", "INFO", total_matches)
    contagens = matches.agg(*[F.count(coluna).alias(nome) for nome, (coluna, _) in COBERTURAS.items()]).first().asDict()
    for nome, (_, minimo) in COBERTURAS.items():
        cobertura = contagens[nome] / total_matches if total_matches else 0.0
        report.add(f"Cobertura {nome}", True, f"{cobertura:.2%}", "INFO", cobertura)
        report.add(
            f"Limiar de cobertura {nome}",
            cobertura >= minimo,
            f"encontrado={cobertura:.2%}, referência={minimo:.0%}",
            "WARNING",
            cobertura,
        )
    return report


def main() -> int:
    spark = criar_spark("DQ-Gold-Fato-Internacao-Meteorologia")
    try:
        df_inmet = spark.read.option("mergeSchema", "true").parquet(SILVER_INMET_DIARIO_PATH)
        df_inmet.cache()
        report = validar_fato_meteorologia(
            spark.read.option("mergeSchema", "true").parquet(FATO_METEOROLOGIA_PATH),
            spark.read.option("mergeSchema", "true").parquet(FATO_INTERNACAO_PATH),
            df_inmet,
        )
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
