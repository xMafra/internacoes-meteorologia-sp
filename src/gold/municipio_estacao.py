"""Seleciona uma estação INMET auditável para cada município e ano.

A estação mais próxima só é a primeira opção quando possui cobertura
anual mínima de temperatura. Se nenhuma estação atingir o limiar, usa-se a de
maior cobertura (distância como desempate). Assim, uma estação inaugurada
depois de 2015 nunca é aplicada retroativamente.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico
from src.common.study_period import ANALYSIS_YEARS, DATA_FIM, DATA_INICIO


CAMINHO_SILVER_IBGE = "/home/jovyan/work/data/silver/ibge/2022"
CAMINHO_SILVER_INMET_DIARIO = "/home/jovyan/work/data/silver/inmet_diario"
CAMINHO_GOLD = "/home/jovyan/work/data/gold/municipio_estacao"
MIN_COBERTURA_TEMPERATURA = float(
    os.getenv("TCC_MIN_STATION_YEAR_TEMP_COVERAGE", "0.80")
)
RAIO_TERRA_KM = 6371.0


def cobertura_estacao_ano(df_inmet_diario: DataFrame) -> DataFrame:
    """Resume disponibilidade real de temperatura por estação e ano."""
    base = (
        df_inmet_diario
        .filter(F.col("data").between(DATA_INICIO, DATA_FIM))
        .withColumn("ano", F.year("data"))
    )
    return (
        base.groupBy("codigo_estacao", "ano")
        .agg(
            F.min("data").alias("primeiro_dia_disponivel"),
            F.max("data").alias("ultimo_dia_disponivel"),
            F.countDistinct("data").cast("long").alias("dias_com_registro"),
            F.countDistinct(
                F.when(F.col("temperatura_media_dia_c").isNotNull(), F.col("data"))
            ).cast("long").alias("dias_validos_temperatura"),
        )
        .withColumn(
            "inicio_ano",
            F.greatest(
                F.make_date("ano", F.lit(1), F.lit(1)),
                F.to_date(F.lit(DATA_INICIO)),
            ),
        )
        .withColumn(
            "fim_ano",
            F.least(
                F.make_date("ano", F.lit(12), F.lit(31)),
                F.to_date(F.lit(DATA_FIM)),
            ),
        )
        .withColumn(
            "dias_esperados_ano",
            (F.datediff("fim_ano", "inicio_ano") + F.lit(1)).cast("long"),
        )
        .withColumn(
            "cobertura_temperatura",
            F.col("dias_validos_temperatura") / F.col("dias_esperados_ano"),
        )
        .drop("inicio_ano", "fim_ano")
    )


def metadados_estacao_ano(df_inmet_diario: DataFrame) -> DataFrame:
    janela = Window.partitionBy("codigo_estacao", "ano").orderBy(
        F.col("data").desc()
    )
    return (
        df_inmet_diario
        .filter(F.year("data").isin(list(ANALYSIS_YEARS)))
        .withColumn("ano", F.year("data"))
        .withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .select(
            "codigo_estacao", "ano", "estacao",
            "latitude_estacao", "longitude_estacao",
        )
    )


def distancia_haversine():
    delta_lat = F.radians(F.col("latitude_estacao") - F.col("latitude_municipio"))
    delta_lon = F.radians(F.col("longitude_estacao") - F.col("longitude_municipio"))
    lat_municipio = F.radians(F.col("latitude_municipio"))
    lat_estacao = F.radians(F.col("latitude_estacao"))
    a = (
        F.sin(delta_lat / 2) ** 2
        + F.cos(lat_municipio) * F.cos(lat_estacao) * F.sin(delta_lon / 2) ** 2
    )
    return 2 * F.lit(RAIO_TERRA_KM) * F.asin(F.sqrt(a))


def criar_municipio_estacao_ano(
    df_ibge: DataFrame,
    df_inmet_diario: DataFrame,
    cobertura_minima: float = MIN_COBERTURA_TEMPERATURA,
) -> DataFrame:
    if not 0 <= cobertura_minima <= 1:
        raise ValueError("A cobertura mínima deve estar entre zero e um.")

    municipios = (
        df_ibge.select(
            "CD_MUN", "NM_MUN", "SIGLA_UF", "AREA_KM2",
            F.col("latitude").alias("latitude_municipio"),
            F.col("longitude").alias("longitude_municipio"),
        )
        .filter(F.col("latitude_municipio").isNotNull())
        .filter(F.col("longitude_municipio").isNotNull())
    )
    estacoes = metadados_estacao_ano(df_inmet_diario).join(
        cobertura_estacao_ano(df_inmet_diario),
        ["codigo_estacao", "ano"],
        "inner",
    )
    candidatos = (
        municipios.crossJoin(estacoes)
        .withColumn("distancia_km", distancia_haversine())
        .withColumn(
            "atinge_cobertura_minima",
            (F.col("cobertura_temperatura") >= F.lit(cobertura_minima)).cast("int"),
        )
    )
    janela = Window.partitionBy("CD_MUN", "ano").orderBy(
        F.col("atinge_cobertura_minima").desc(),
        F.when(F.col("atinge_cobertura_minima") == 1, F.col("distancia_km"))
        .otherwise(F.lit(None)).asc_nulls_last(),
        F.col("cobertura_temperatura").desc(),
        F.col("distancia_km").asc(),
        F.col("codigo_estacao").asc(),
    )
    return (
        candidatos
        .withColumn("_ranking", F.row_number().over(janela))
        .filter(F.col("_ranking") == 1)
        .withColumn(
            "criterio_selecao",
            F.when(
                F.col("atinge_cobertura_minima") == 1,
                F.lit("MAIS_PROXIMA_COM_COBERTURA_MINIMA"),
            ).otherwise(F.lit("MAIOR_COBERTURA_DISPONIVEL")),
        )
        .withColumn(
            "cobertura_temperatura_pct",
            F.round(F.col("cobertura_temperatura") * 100, 2),
        )
        .select(
            "CD_MUN", "NM_MUN", "SIGLA_UF", "AREA_KM2", "ano",
            "latitude_municipio", "longitude_municipio", "codigo_estacao",
            "estacao", "latitude_estacao", "longitude_estacao", "distancia_km",
            "primeiro_dia_disponivel", "ultimo_dia_disponivel",
            "dias_com_registro", "dias_validos_temperatura", "dias_esperados_ano",
            "cobertura_temperatura_pct", "criterio_selecao",
        )
    )


def main() -> int:
    spark = (
        SparkSession.builder
        .appName("TCC-Gold-Municipio-Estacao-Ano")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        resultado = criar_municipio_estacao_ano(
            spark.read.parquet(CAMINHO_SILVER_IBGE),
            spark.read.option("mergeSchema", "true").parquet(
                CAMINHO_SILVER_INMET_DIARIO
            ),
        ).cache()
        total = resultado.count()
        esperado = 645 * len(ANALYSIS_YEARS)
        if total != esperado:
            raise ValueError(
                f"Mapa município-estação incompleto: {total} linhas; esperado={esperado}."
            )
        with destino_parquet_atomico(CAMINHO_GOLD) as caminho_staging:
            (
                resultado.write.mode("overwrite")
                .partitionBy("ano")
                .parquet(caminho_staging)
            )
        print(f"Mapa município-estação-ano gravado: {total} linhas em {CAMINHO_GOLD}")
        resultado.groupBy("ano", "criterio_selecao").count().orderBy(
            "ano", "criterio_selecao"
        ).show(100, truncate=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
