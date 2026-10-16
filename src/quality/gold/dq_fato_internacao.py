import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, validar_colunas_obrigatorias,
    validar_condicao, validar_nulos, validar_quantidade_distinta,
    validar_quantidade_registros, validar_tipos,
)
from src.common.sih_io import ler_silver_sih
from src.quality.gold.common import criar_spark
from src.quality.gold.quality_config import (
    CATEGORIAS_CID_COMPLEMENTARES, DIAGNOSTICOS_DISTINTOS_ESPERADOS, ESTACOES_ESPERADAS,
    FATO_INTERNACAO_PATH, MUNICIPIO_ESTACAO_PATH, MUNICIPIOS_ESPERADOS,
    SILVER_CID10_PATH, SILVER_IBGE_PATH, SILVER_SIH_PATH,
    TOTAL_INTERNACOES_ESPERADO,
)
from src.common.study_period import DATA_FIM, DATA_INICIO


ESSENCIAIS = [
    "ano", "mes", "data_internacao", "CD_MUN", "NM_MUN",
    "codigo_estacao", "codigo_cid", "descricao_cid", "codigo_categoria",
    "descricao_categoria", "descricao_grupo", "codigo_capitulo",
]


def validar_fato(df, df_sih, df_ibge, df_cid10, df_municipio_estacao) -> QualityReport:
    report = QualityReport("GOLD FATO INTERNAÇÃO")
    if not validar_colunas_obrigatorias(df, ESSENCIAIS, report):
        return report
    validar_tipos(df, {"ano": "int", "data_internacao": "date", "idade": "bigint", "valor_total": "double"}, report)
    total_gold = validar_quantidade_registros(df, TOTAL_INTERNACOES_ESPERADO, report)
    total_sih = df_sih.filter(F.col("DT_INTER").between(DATA_INICIO, DATA_FIM)).count()
    report.add(
        "Preservação do universo analítico SIH",
        total_gold == total_sih,
        f"Silver SIH por DT_INTER={total_sih:,}, Gold={total_gold:,}",
    )
    gold_ano = {
        int(r["ano"]): r["count"]
        for r in df.groupBy("ano").count().orderBy("ano").collect()
    }
    sih_ano = {
        int(r["ano"]): r["count"]
        for r in (
            df_sih.filter(F.col("DT_INTER").between(DATA_INICIO, DATA_FIM))
            .groupBy(F.year("DT_INTER").alias("ano"))
            .count()
            .orderBy("ano")
            .collect()
        )
    }
    report.add(
        "Reconciliação anual Silver SIH x Gold",
        gold_ano == sih_ano,
        f"Silver={sih_ano}; Gold={gold_ano}",
        metrica=str(gold_ano),
    )
    validar_quantidade_distinta(df, "CD_MUN", MUNICIPIOS_ESPERADOS, report, "Municípios da fato")
    validar_quantidade_distinta(df, "codigo_estacao", ESTACOES_ESPERADAS, report, "Estações da fato")
    validar_quantidade_distinta(df, "codigo_cid", DIAGNOSTICOS_DISTINTOS_ESPERADOS, report, "Diagnósticos distintos")
    validar_nulos(df, ESSENCIAIS, report)
    validar_condicao(df, F.col("data_internacao") > F.col("data_saida"), "Ordem das datas", report)
    validar_condicao(df, F.col("distancia_km") < 0, "Distância não negativa", report)
    validar_condicao(df, F.col("valor_total") < 0, "Valor total não negativo", report)

    municipios_gold = df.select("CD_MUN").distinct()
    municipios_ibge = df_ibge.select("CD_MUN").distinct()
    sem_ibge = municipios_gold.join(municipios_ibge, "CD_MUN", "left_anti").count()
    report.add("Integridade IBGE", sem_ibge == 0, f"municípios sem IBGE={sem_ibge}")

    estacoes_gold = df.select("CD_MUN", "ano", "codigo_estacao").distinct()
    estacoes_dim = df_municipio_estacao.select("CD_MUN", "ano", "codigo_estacao").distinct()
    sem_estacao = estacoes_gold.join(
        estacoes_dim, ["CD_MUN", "ano", "codigo_estacao"], "left_anti"
    ).count()
    report.add("Integridade município-estação", sem_estacao == 0, f"relações inválidas={sem_estacao}")

    categorias_gold = df.select("codigo_categoria").distinct()
    categorias_oficiais = df_cid10.select("codigo_categoria").distinct()
    categorias_sem_cid = (
        categorias_gold.join(categorias_oficiais, "codigo_categoria", "left_anti")
        .filter(~F.col("codigo_categoria").isin(CATEGORIAS_CID_COMPLEMENTARES))
    )
    valores_sem_cid = [row["codigo_categoria"] for row in categorias_sem_cid.collect()]
    report.add("Integridade CID-10", not valores_sem_cid, f"categorias sem correspondência={valores_sem_cid}")
    cid_sem_match_ano = {
        int(r["ano"]): r["count"]
        for r in (
            df.filter(F.col("codigo_categoria").isNull())
            .groupBy("ano").count().orderBy("ano").collect()
        )
    }
    report.add(
        "Códigos CID sem correspondência por ano",
        not cid_sem_match_ano,
        f"registros={cid_sem_match_ano}",
        metrica=str(cid_sem_match_ano),
    )
    report.add(
        "Categorias CID complementares",
        True,
        f"aceitas explicitamente={CATEGORIAS_CID_COMPLEMENTARES}",
        "INFO",
    )
    return report


def main() -> int:
    spark = criar_spark("DQ-Gold-Fato-Internacao")
    try:
        report = validar_fato(
            spark.read.parquet(FATO_INTERNACAO_PATH),
            ler_silver_sih(spark, SILVER_SIH_PATH),
            spark.read.parquet(SILVER_IBGE_PATH),
            spark.read.parquet(SILVER_CID10_PATH),
            spark.read.parquet(MUNICIPIO_ESTACAO_PATH),
        )
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
