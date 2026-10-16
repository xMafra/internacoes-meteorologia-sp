"""Gera evidências tabulares para as seis perguntas de pesquisa."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico

BASE = "/home/jovyan/work/data/consumption"
SAIDA = "/home/jovyan/work/data/validation/perguntas_pesquisa"
AMOSTRA_MINIMA = 30


def faixa_temperatura(coluna="temperatura_media_dia_c"):
    temperatura = F.col(coluna)
    return (
        F.when(temperatura.isNull(), F.lit(None).cast("string"))
        .when(temperatura < 15, F.lit("< 15 °C"))
        .when(temperatura < 20, F.lit("15 a 19,9 °C"))
        .when(temperatura < 25, F.lit("20 a 24,9 °C"))
        .when(temperatura < 30, F.lit("25 a 29,9 °C"))
        .otherwise(F.lit(">= 30 °C"))
    )


def resumo_frio_quente(
    df: DataFrame,
    grupos: list[str],
    temperatura: str = "temperatura_media_dia_c",
    internacoes: str = "qtd_internacoes",
) -> DataFrame:
    frio = F.col(temperatura) < 20
    quente = F.col(temperatura) >= 25
    agregado = df.groupBy(*grupos).agg(
        F.sum(F.when(frio, F.col(internacoes)).otherwise(0)).alias("internacoes_frio"),
        F.sum(F.when(quente, F.col(internacoes)).otherwise(0)).alias("internacoes_quente"),
        F.sum(F.when(frio, 1).otherwise(0)).cast("long").alias("amostra_frio"),
        F.sum(F.when(quente, 1).otherwise(0)).cast("long").alias("amostra_quente"),
    )
    return (
        agregado
        .withColumn(
            "media_frio",
            F.when(F.col("amostra_frio") > 0,
                   F.col("internacoes_frio") / F.col("amostra_frio")),
        )
        .withColumn(
            "media_quente",
            F.when(F.col("amostra_quente") > 0,
                   F.col("internacoes_quente") / F.col("amostra_quente")),
        )
        .withColumn("diferenca_absoluta", F.col("media_frio") - F.col("media_quente"))
        .withColumn(
            "diferenca_relativa",
            F.when(
                F.col("media_quente").isNotNull() & (F.col("media_quente") != 0),
                F.col("diferenca_absoluta") / F.col("media_quente"),
            ),
        )
        .withColumn(
            "amostra_valida",
            (
                (F.col("amostra_frio") >= AMOSTRA_MINIMA)
                & (F.col("amostra_quente") >= AMOSTRA_MINIMA)
            ).cast("int"),
        )
    )


def denominadores_exposicao(fato_dia: DataFrame) -> DataFrame:
    return resumo_frio_quente(
        fato_dia.withColumn("_zero", F.lit(0)), [], internacoes="_zero"
    ).select("amostra_frio", "amostra_quente")


def resumo_estrato_com_exposicao(
    df_eventos: DataFrame,
    grupos: list[str],
    denominadores: DataFrame,
) -> DataFrame:
    agregado = df_eventos.groupBy(*grupos).agg(
        F.sum(
            F.when(F.col("temperatura_media_dia_c") < 20, F.col("qtd_internacoes"))
            .otherwise(0)
        ).alias("internacoes_frio"),
        F.sum(
            F.when(F.col("temperatura_media_dia_c") >= 25, F.col("qtd_internacoes"))
            .otherwise(0)
        ).alias("internacoes_quente"),
    )
    return (
        agregado.crossJoin(denominadores)
        .withColumn("media_frio", F.col("internacoes_frio") / F.col("amostra_frio"))
        .withColumn("media_quente", F.col("internacoes_quente") / F.col("amostra_quente"))
        .withColumn("diferenca_absoluta", F.col("media_frio") - F.col("media_quente"))
        .withColumn(
            "diferenca_relativa",
            F.when(F.col("media_quente") != 0,
                   F.col("diferenca_absoluta") / F.col("media_quente")),
        )
        .withColumn(
            "amostra_valida",
            (
                (F.col("amostra_frio") >= AMOSTRA_MINIMA)
                & (F.col("amostra_quente") >= AMOSTRA_MINIMA)
            ).cast("int"),
        )
    )


def gerar_saidas(fato_dia, fato_cid, dim_cid, dim_municipio, dim_tempo):
    q1 = (
        fato_dia.withColumn("faixa_temperatura", faixa_temperatura())
        .filter(F.col("faixa_temperatura").isNotNull())
        .groupBy("faixa_temperatura")
        .agg(
            F.count("*").cast("long").alias("municipio_dias"),
            F.sum("qtd_internacoes").alias("internacoes"),
        )
        .withColumn(
            "media_internacoes_municipio_dia",
            F.col("internacoes") / F.col("municipio_dias"),
        )
    )

    q2_ano = (
        resumo_frio_quente(fato_dia, ["ano"])
        .withColumnRenamed("amostra_frio", "dias_frios")
        .withColumnRenamed("amostra_quente", "dias_quentes")
    )
    q2_mes = (
        resumo_frio_quente(
        fato_dia.join(dim_tempo.select("data", "ano_mes"), "data"), ["ano_mes"]
        )
        .withColumnRenamed("amostra_frio", "dias_frios")
        .withColumnRenamed("amostra_quente", "dias_quentes")
    )
    q2_faixa_ano = (
        fato_dia.withColumn("faixa_temperatura", faixa_temperatura())
        .filter(F.col("faixa_temperatura").isNotNull())
        .groupBy("ano", "faixa_temperatura")
        .agg(
            F.count("*").cast("long").alias("municipio_dias"),
            F.sum("qtd_internacoes").alias("internacoes"),
        )
        .withColumn(
            "media_internacoes_municipio_dia",
            F.col("internacoes") / F.col("municipio_dias"),
        )
    )

    exposicao = fato_dia.select("municipio_dia_id", "temperatura_media_dia_c")
    eventos = fato_cid.join(exposicao, "municipio_dia_id", "inner")
    denominadores = denominadores_exposicao(fato_dia)
    eventos_cid = eventos.join(dim_cid, "codigo_categoria", "left")
    q3_capitulo = resumo_estrato_com_exposicao(
        eventos_cid,
        ["codigo_capitulo", "descricao_capitulo"],
        denominadores,
    )
    q3_grupo = resumo_estrato_com_exposicao(
        eventos_cid,
        [
            "codigo_capitulo", "descricao_capitulo", "codigo_grupo",
            "descricao_grupo",
        ],
        denominadores,
    )
    q3 = resumo_estrato_com_exposicao(
        eventos_cid,
        [
            "codigo_capitulo", "descricao_capitulo", "codigo_grupo",
            "descricao_grupo", "codigo_categoria", "descricao_categoria",
        ],
        denominadores,
    )
    q4 = resumo_estrato_com_exposicao(
        eventos, ["faixa_etaria", "sexo", "sexo_descricao"], denominadores
    )
    q4_cid = resumo_estrato_com_exposicao(
        eventos,
        ["codigo_categoria", "faixa_etaria", "sexo", "sexo_descricao"],
        denominadores,
    )

    def resumo_lag(rotulo, coluna):
        return resumo_frio_quente(fato_dia, [], temperatura=coluna).withColumn(
            "defasagem", F.lit(rotulo)
        )

    q5 = resumo_lag("D0", "temperatura_media_dia_c")
    for rotulo, coluna in (
        ("D-1", "temperatura_media_lag_1d"),
        ("D-3", "temperatura_media_lag_3d"),
        ("D-7", "temperatura_media_lag_7d"),
    ):
        q5 = q5.unionByName(resumo_lag(rotulo, coluna))

    q6 = (
        resumo_frio_quente(fato_dia, ["codigo_municipio"])
        .withColumnRenamed("amostra_frio", "dias_frios")
        .withColumnRenamed("amostra_quente", "dias_quentes")
        .join(
            dim_municipio.select("codigo_municipio", "nome_municipio"),
            "codigo_municipio",
            "left",
        )
    )

    reconciliacao = fato_dia.groupBy("ano").agg(
        F.sum("qtd_internacoes").alias("internacoes"),
        F.sum(
            F.when(F.col("temperatura_media_dia_c").isNotNull(),
                   F.col("qtd_internacoes")).otherwise(0)
        ).alias("internacoes_com_temperatura"),
        F.count("*").alias("municipio_dias"),
        F.sum(F.col("temperatura_media_dia_c").isNotNull().cast("int"))
        .alias("municipio_dias_com_temperatura"),
    )
    return {
        "pergunta_1_faixa_temperatura": q1,
        "pergunta_2_ano": q2_ano,
        "pergunta_2_ano_mes": q2_mes,
        "pergunta_2_ano_faixa_temperatura": q2_faixa_ano,
        "pergunta_3_capitulo": q3_capitulo,
        "pergunta_3_grupo": q3_grupo,
        "pergunta_3_cid": q3,
        "pergunta_4_idade_sexo": q4,
        "pergunta_4_cid_idade_sexo": q4_cid,
        "pergunta_5_defasagens": q5,
        "pergunta_6_municipio": q6,
        "reconciliacao_ano": reconciliacao,
    }


def main() -> int:
    spark = (
        SparkSession.builder.appName("TCC-Validacao-Perguntas-Pesquisa")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ler = lambda nome: spark.read.option("mergeSchema", "true").parquet(
            f"{BASE}/{nome}"
        )
        saidas = gerar_saidas(
            ler("fato_municipio_dia"),
            ler("fato_municipio_dia_cid"),
            ler("dim_cid"),
            ler("dim_municipio"),
            ler("dim_tempo"),
        )
        for nome, df in saidas.items():
            caminho = f"{SAIDA}/{nome}"
            with destino_parquet_atomico(caminho) as caminho_staging:
                df.write.mode("overwrite").parquet(caminho_staging)
            print(f"\n{nome} -> {caminho}")
            df.orderBy(*df.columns[:1]).show(30, truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
