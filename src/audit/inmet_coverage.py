"""Materializa cobertura histórica das estações INMET."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico
from src.common.study_period import DATA_FIM, DATA_INICIO
from src.gold.municipio_estacao import cobertura_estacao_ano


INMET_DIARIO = "/home/jovyan/work/data/silver/inmet_diario"
SAIDA_ANO = "/home/jovyan/work/data/quality/inmet_cobertura_estacao_ano"
SAIDA_ESTACAO = "/home/jovyan/work/data/quality/inmet_cobertura_estacao"


def criar_auditoria(df):
    cobertura = (
        cobertura_estacao_ano(df)
        .withColumn(
            "cobertura_temperatura_pct",
            F.round(F.col("cobertura_temperatura") * 100, 2),
        )
        .drop("cobertura_temperatura")
    )
    validos = (
        df.filter(F.col("data").between(DATA_INICIO, DATA_FIM))
        .filter(F.col("temperatura_media_dia_c").isNotNull())
        .select("codigo_estacao", "data")
        .dropDuplicates()
    )
    janela = Window.partitionBy("codigo_estacao").orderBy("data")
    lacunas = (
        validos.withColumn("data_anterior", F.lag("data").over(janela))
        .withColumn(
            "dias_lacuna",
            F.greatest(
                F.datediff("data", "data_anterior") - F.lit(1), F.lit(0)
            ),
        )
        .groupBy("codigo_estacao")
        .agg(F.max("dias_lacuna").cast("long").alias("maior_lacuna_dias"))
    )
    resumo = (
        cobertura.groupBy("codigo_estacao")
        .agg(
            F.min("primeiro_dia_disponivel").alias("primeiro_dia_disponivel"),
            F.max("ultimo_dia_disponivel").alias("ultimo_dia_disponivel"),
            F.sort_array(F.collect_set("ano")).alias("anos_disponiveis"),
            F.sum("dias_validos_temperatura").alias("dias_validos_temperatura"),
            F.sum("dias_esperados_ano").alias("dias_esperados"),
        )
        .withColumn(
            "cobertura_temperatura_pct",
            F.round(
                100 * F.col("dias_validos_temperatura") / F.col("dias_esperados"),
                2,
            ),
        )
        .join(lacunas, "codigo_estacao", "left")
    )
    return cobertura, resumo


def main() -> int:
    spark = (
        SparkSession.builder.appName("TCC-Auditoria-Cobertura-INMET")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = spark.read.option("mergeSchema", "true").parquet(INMET_DIARIO).cache()
        cobertura, resumo = criar_auditoria(df)
        with destino_parquet_atomico(SAIDA_ANO) as caminho_staging:
            cobertura.coalesce(1).write.mode("overwrite").parquet(caminho_staging)
        with destino_parquet_atomico(SAIDA_ESTACAO) as caminho_staging:
            resumo.coalesce(1).write.mode("overwrite").parquet(caminho_staging)
        print("Cobertura de temperatura por ano:")
        cobertura.groupBy("ano").agg(
            F.countDistinct("codigo_estacao").alias("estacoes"),
            F.sum("dias_validos_temperatura").alias("dias_validos"),
            F.sum("dias_esperados_ano").alias("dias_esperados"),
        ).withColumn(
            "cobertura_pct",
            F.round(100 * F.col("dias_validos") / F.col("dias_esperados"), 2),
        ).orderBy("ano").show(100, truncate=False)
        print("Estações com início tardio, baixa cobertura ou lacuna >= 30 dias:")
        resumo.filter(
            (F.col("primeiro_dia_disponivel") > F.lit(DATA_INICIO))
            | (F.col("cobertura_temperatura_pct") < 80)
            | (F.col("maior_lacuna_dias") >= 30)
        ).orderBy("codigo_estacao").show(500, truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
