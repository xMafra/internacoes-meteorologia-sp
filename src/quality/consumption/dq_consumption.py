"""Data Quality da camada de consumo para Power BI."""

from __future__ import annotations

import sys
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport,
    imprimir_relatorio,
    registrar_cobertura,
    validar_chave_unica,
    validar_colunas_obrigatorias,
    validar_condicao,
    validar_nulos,
    validar_periodo,
    validar_valores_aceitos,
)
from src.consumption.transformations import (
    DATA_FIM,
    DATA_INICIO,
    FAIXAS_ETARIAS,
    METEOROLOGIA_ADICIONAL,
    METEOROLOGIA_GOLD,
    METRICAS_SAUDE,
    inconsistencias_metadados_estacao,
)


BASE_PATH = "/home/jovyan/work/data"
CONSUMPTION = f"{BASE_PATH}/consumption"
GOLD_FATO = f"{BASE_PATH}/gold/fato_internacao_meteorologia"
SILVER_INMET = f"{BASE_PATH}/silver/inmet"
SILVER_INMET_DIARIO = f"{BASE_PATH}/silver/inmet_diario"

COLUNAS_FATO_DIA = [
    "municipio_dia_id", "data", "codigo_municipio", "codigo_estacao",
    *METRICAS_SAUDE,
    *[c for c in METEOROLOGIA_GOLD if c != "codigo_estacao"],
    *METEOROLOGIA_ADICIONAL,
    "temperatura_media_lag_1d", "temperatura_media_lag_3d",
    "temperatura_media_lag_7d", "ano",
]
COLUNAS_FATO_CID = [
    "municipio_dia_id", "data", "codigo_municipio", "codigo_categoria",
    "sexo", "faixa_etaria", *METRICAS_SAUDE, "ano",
]


def ler_inmet_horario(spark):
    partes = [
        spark.read.option("mergeSchema", "true").parquet(f"{SILVER_INMET}/{ano}")
        for ano in range(2023, 2026)
    ]
    df = partes[0]
    for parte in partes[1:]:
        df = df.unionByName(parte, allowMissingColumns=True)
    return df


def totais_saude(df):
    return df.agg(*[F.sum(c).alias(c) for c in METRICAS_SAUDE]).first().asDict()


def totais_gold(df_gold):
    return (
        df_gold.filter(F.col("data_internacao").between(DATA_INICIO, DATA_FIM))
        .agg(
            F.count("*").cast("long").alias("qtd_internacoes"),
            F.sum(F.col("obito").cast("long")).alias("qtd_obitos"),
            F.sum(F.col("dias_permanencia").cast("long")).alias("dias_permanencia_total"),
            F.sum(F.col("dias_uti").cast("long")).alias("dias_uti_total"),
            F.sum(F.col("valor_total").cast("decimal(20,2)"))
            .cast("decimal(20,2)").alias("valor_total_internacoes"),
            F.sum(F.col("valor_uti").cast("decimal(20,2)"))
            .cast("decimal(20,2)").alias("valor_total_uti"),
        ).first().asDict()
    )


def comparar_totais(report, nome, encontrado, esperado):
    divergencias = {
        campo: {"encontrado": str(encontrado.get(campo)), "esperado": str(esperado.get(campo))}
        for campo in METRICAS_SAUDE
        if encontrado.get(campo) != esperado.get(campo)
    }
    report.add(nome, not divergencias, "totais idênticos" if not divergencias else str(divergencias))


def validar_lags_exatos(df_fato, df_inmet_diario, report):
    for dias in (1, 3, 7):
        nome = f"temperatura_media_lag_{dias}d"
        lookup = df_inmet_diario.select(
            F.col("codigo_estacao").alias("_estacao"),
            F.to_date("data").alias("_data"),
            F.col("temperatura_media_dia_c").alias("_esperada"),
        )
        invalidos = (
            df_fato.join(
                lookup,
                (F.col("codigo_estacao") == F.col("_estacao"))
                & (F.col("_data") == F.date_sub(F.col("data"), dias)),
                "left",
            )
            .filter(~F.col(nome).eqNullSafe(F.col("_esperada")))
            .count()
        )
        report.add(
            f"Lag exato de {dias} dia(s)", invalidos == 0,
            f"divergências em data - {dias} dia(s)={invalidos}", metrica=invalidos,
        )


def validar_consumption(
    fato_dia, fato_cid, dim_municipio, dim_cid, dim_tempo, dim_estacao,
    gold, inmet_diario, inmet_horario,
) -> QualityReport:
    report = QualityReport("CAMADA CONSUMPTION")
    schemas_ok = validar_colunas_obrigatorias(fato_dia, COLUNAS_FATO_DIA, report)
    schemas_ok &= validar_colunas_obrigatorias(fato_cid, COLUNAS_FATO_CID, report)
    if not schemas_ok:
        return report

    validar_chave_unica(fato_dia, ["municipio_dia_id"], report)
    validar_chave_unica(
        fato_cid,
        ["data", "codigo_municipio", "codigo_categoria", "sexo", "faixa_etaria"],
        report,
    )
    validar_nulos(
        fato_dia, ["municipio_dia_id", "data", "codigo_municipio", "codigo_estacao"], report
    )
    validar_nulos(
        fato_cid,
        ["municipio_dia_id", "data", "codigo_municipio", "codigo_categoria", "sexo", "faixa_etaria"],
        report,
    )
    validar_condicao(fato_dia, F.col("qtd_internacoes") <= 0, "Internações positivas na fato diária", report)
    validar_condicao(fato_cid, F.col("qtd_internacoes") <= 0, "Internações positivas na fato CID", report)
    for df, nome in ((fato_dia, "fato diária"), (fato_cid, "fato CID")):
        validar_condicao(
            df,
            (F.col("qtd_obitos") < 0) | (F.col("qtd_obitos") > F.col("qtd_internacoes")),
            f"Óbitos entre zero e internações — {nome}", report,
        )

    esperado = totais_gold(gold)
    totais_dia = totais_saude(fato_dia)
    totais_cid = totais_saude(fato_cid)
    comparar_totais(report, "Totais da fato diária reproduzem a Gold", totais_dia, esperado)
    comparar_totais(report, "Totais da fato CID reproduzem a Gold", totais_cid, esperado)
    comparar_totais(report, "Totais iguais entre as duas fatos", totais_dia, totais_cid)

    validar_valores_aceitos(fato_cid, "faixa_etaria", FAIXAS_ETARIAS, report)
    for fato, chave, dim, nome in (
        (fato_dia, "codigo_municipio", dim_municipio, "Municípios da fato diária"),
        (fato_cid, "codigo_municipio", dim_municipio, "Municípios da fato CID"),
        (fato_cid, "codigo_categoria", dim_cid, "Categorias da fato CID"),
        (fato_dia, "data", dim_tempo, "Datas da fato diária"),
        (fato_cid, "data", dim_tempo, "Datas da fato CID"),
        (fato_dia, "codigo_estacao", dim_estacao, "Estações da fato diária"),
    ):
        ausentes = fato.select(chave).distinct().join(dim.select(chave).distinct(), chave, "left_anti").count()
        report.add(nome, ausentes == 0, f"chaves ausentes na dimensão={ausentes}", metrica=ausentes)

    for dim, chave, nome in (
        (dim_municipio, ["codigo_municipio"], "dim_municipio"),
        (dim_cid, ["codigo_categoria"], "dim_cid"),
        (dim_tempo, ["data"], "dim_tempo"),
        (dim_estacao, ["codigo_estacao"], "dim_estacao"),
    ):
        validar_chave_unica(dim, chave, report)

    validar_periodo(dim_tempo, "data", DATA_INICIO, DATA_FIM, report)
    total_tempo = dim_tempo.count()
    report.add("Calendário contínuo", total_tempo == 1096, f"dias={total_tempo}, esperado=1096")

    colunas_meteo_cid = sorted(set(fato_cid.columns) & set(METEOROLOGIA_GOLD + METEOROLOGIA_ADICIONAL))
    report.add(
        "Meteorologia não expandida pela fato CID", not colunas_meteo_cid,
        "nenhuma métrica meteorológica" if not colunas_meteo_cid else f"colunas={colunas_meteo_cid}",
    )
    validar_chave_unica(fato_dia, ["data", "codigo_municipio"], report)
    for coluna in [c for c in fato_dia.columns if c.startswith("qtd_obs_")] + ["qtd_observacoes_inmet"]:
        validar_condicao(
            fato_dia, F.col(coluna).isNotNull() & ((F.col(coluna) < 0) | (F.col(coluna) > 24)),
            f"Contador meteorológico válido — {coluna}", report,
        )

    origem_contadores = (
        gold.filter(F.col("data_internacao").between(DATA_INICIO, DATA_FIM))
        .select(
            F.to_date("data_internacao").alias("data"),
            F.col("CD_MUN").alias("codigo_municipio"),
            "qtd_observacoes_inmet", "qtd_obs_temperatura", "qtd_obs_umidade",
            "qtd_obs_precipitacao", "qtd_obs_vento",
        ).distinct()
    )
    contador_cols = [
        "qtd_observacoes_inmet", "qtd_obs_temperatura", "qtd_obs_umidade",
        "qtd_obs_precipitacao", "qtd_obs_vento",
    ]
    divergentes = (
        fato_dia.alias("f").join(origem_contadores.alias("o"), ["data", "codigo_municipio"])
        .filter(~reduce(lambda a, b: a & b, [F.col(f"f.{c}").eqNullSafe(F.col(f"o.{c}")) for c in contador_cols]))
        .count()
    )
    report.add("Contadores da Gold preservados", divergentes == 0, f"chaves divergentes={divergentes}")

    validar_lags_exatos(fato_dia, inmet_diario, report)
    registrar_cobertura(fato_dia, METEOROLOGIA_ADICIONAL, report)

    revisoes = inconsistencias_metadados_estacao(inmet_horario).count()
    report.add(
        "Consistência histórica do cadastro de estações",
        revisoes == 0,
        f"estações com metadados revisados entre anos={revisoes}; dimensão usa 2025",
        "WARNING",
        revisoes,
    )
    return report


def main():
    spark = SparkSession.builder.appName("DQ-Consumption-PowerBI").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ler = lambda nome: spark.read.option("mergeSchema", "true").parquet(f"{CONSUMPTION}/{nome}")
        fato_dia = ler("fato_municipio_dia").cache()
        fato_cid = ler("fato_municipio_dia_cid").cache()
        gold = spark.read.option("mergeSchema", "true").parquet(GOLD_FATO).cache()
        inmet_diario = spark.read.option("mergeSchema", "true").parquet(SILVER_INMET_DIARIO).cache()
        inmet_horario = ler_inmet_horario(spark).cache()
        report = validar_consumption(
            fato_dia, fato_cid, ler("dim_municipio"), ler("dim_cid"),
            ler("dim_tempo"), ler("dim_estacao"), gold, inmet_diario, inmet_horario,
        )
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
