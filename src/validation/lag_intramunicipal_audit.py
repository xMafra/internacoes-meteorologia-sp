"""Auditoria intramunicipal de D0, D-1, D-3 e D-7.

Cada município recebe o mesmo peso no resumo de uma defasagem. Para os
recortes respiratórios, a fato CID permanece esparsa no numerador, enquanto
os denominadores vêm do painel completo município-dia de cada lag.
"""

from __future__ import annotations

import sys
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico
from src.validation.metric_audit import (
    adicionar_metricas_contraste,
    contraste_frio_quente,
    resumo_distribuicao,
)


BASE = "/home/jovyan/work/data/consumption"
SAIDA = "/home/jovyan/work/data/validation/auditoria_lags_intramunicipais"
DEFASAGENS = (
    ("D0", "temperatura_media_dia_c"),
    ("D-1", "temperatura_media_lag_1d"),
    ("D-3", "temperatura_media_lag_3d"),
    ("D-7", "temperatura_media_lag_7d"),
)
ALVOS_RESPIRATORIOS = (
    ("CAPITULO_10", "Capítulo X - Doenças do aparelho respiratório"),
    ("GRUPO_J09-J18", "Influenza [gripe] e pneumonia"),
    (
        "GRUPO_J20-J22",
        "Outras infecções agudas das vias aéreas inferiores",
    ),
)


def _unir(dataframes: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b), dataframes)


def _nomes_metricas_municipais(df: DataFrame) -> DataFrame:
    return (
        df.withColumnRenamed("media_frio", "media_frio_municipio")
        .withColumnRenamed("media_quente", "media_quente_municipio")
    )


def contrastes_totais_por_lag(fato_dia: DataFrame) -> DataFrame:
    """Calcula um contraste por município e por lag, com amostra própria."""
    resultados = []
    for defasagem, temperatura in DEFASAGENS:
        resultados.append(
            contraste_frio_quente(
                fato_dia,
                ["codigo_municipio"],
                temperatura=temperatura,
            ).withColumn("defasagem", F.lit(defasagem))
        )
    return _nomes_metricas_municipais(_unir(resultados))


def eventos_respiratorios(eventos_cid: DataFrame) -> DataFrame:
    """Projeta cada evento para capítulo X e/ou para o grupo respiratório."""
    capitulo = (
        eventos_cid.filter(F.col("codigo_capitulo") == "10")
        .withColumn("alvo", F.lit("CAPITULO_10"))
        .withColumn(
            "descricao_alvo",
            F.lit("Capítulo X - Doenças do aparelho respiratório"),
        )
    )
    grupos = (
        eventos_cid.filter(F.col("codigo_grupo").isin("J09-J18", "J20-J22"))
        .withColumn("alvo", F.concat(F.lit("GRUPO_"), F.col("codigo_grupo")))
        .withColumn("descricao_alvo", F.col("descricao_grupo"))
    )
    return capitulo.unionByName(grupos, allowMissingColumns=True)


def _dimensao_alvos(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(ALVOS_RESPIRATORIOS, "alvo string, descricao_alvo string")


def contrastes_respiratorios_por_lag(
    fato_dia: DataFrame,
    eventos_cid: DataFrame,
) -> DataFrame:
    """Calcula contrastes respiratórios preservando dias de zero no denominador."""
    alvos = eventos_respiratorios(eventos_cid).persist(StorageLevel.MEMORY_AND_DISK)
    dimensao_alvos = _dimensao_alvos(fato_dia.sparkSession)
    resultados = []

    for defasagem, temperatura in DEFASAGENS:
        exposicao = (
            contraste_frio_quente(
                fato_dia.withColumn("_zero", F.lit(0)),
                ["codigo_municipio"],
                temperatura=temperatura,
                internacoes="_zero",
            )
            .select("codigo_municipio", "dias_frios", "dias_quentes")
            .crossJoin(dimensao_alvos)
        )
        frio = F.col(temperatura) < 20
        quente = F.col(temperatura) >= 25
        numeradores = alvos.groupBy(
            "codigo_municipio", "alvo", "descricao_alvo"
        ).agg(
            F.sum(F.when(frio, F.col("qtd_internacoes")).otherwise(0))
            .alias("internacoes_frio"),
            F.sum(F.when(quente, F.col("qtd_internacoes")).otherwise(0))
            .alias("internacoes_quente"),
        )
        resultados.append(
            adicionar_metricas_contraste(
                exposicao.join(
                    numeradores,
                    ["codigo_municipio", "alvo", "descricao_alvo"],
                    "left",
                ).fillna(0, subset=["internacoes_frio", "internacoes_quente"])
            ).withColumn("defasagem", F.lit(defasagem))
        )

    resultado = _nomes_metricas_municipais(_unir(resultados))
    alvos.unpersist()
    return resultado


def gerar_saidas(
    fato_dia: DataFrame,
    fato_cid: DataFrame,
    dim_cid: DataFrame,
    dim_municipio: DataFrame,
) -> dict[str, DataFrame]:
    municipios = dim_municipio.select(
        "codigo_municipio", "nome_municipio"
    ).dropDuplicates(["codigo_municipio"])
    detalhe_total = contrastes_totais_por_lag(fato_dia).join(
        municipios, "codigo_municipio", "left"
    )
    resumo_total = resumo_distribuicao(detalhe_total, ["defasagem"]).orderBy(
        F.expr("array_position(array('D0','D-1','D-3','D-7'), defasagem)")
    )

    temperaturas = fato_dia.select(
        "municipio_dia_id",
        *[coluna for _, coluna in DEFASAGENS],
    )
    eventos = (
        fato_cid.join(temperaturas, "municipio_dia_id", "inner")
        .join(dim_cid, "codigo_categoria", "left")
    )
    detalhe_respiratorio = contrastes_respiratorios_por_lag(
        fato_dia, eventos
    ).join(municipios, "codigo_municipio", "left")
    resumo_respiratorio = resumo_distribuicao(
        detalhe_respiratorio,
        ["alvo", "descricao_alvo", "defasagem"],
    ).orderBy(
        "alvo",
        F.expr("array_position(array('D0','D-1','D-3','D-7'), defasagem)"),
    )
    return {
        "contraste_total_municipio_lag": detalhe_total,
        "resumo_total_lag": resumo_total,
        "contraste_respiratorio_municipio_lag": detalhe_respiratorio,
        "resumo_respiratorio_lag": resumo_respiratorio,
    }


def publicar(nome: str, df: DataFrame) -> None:
    destino = f"{SAIDA}/{nome}"
    with destino_parquet_atomico(destino) as staging:
        df.write.mode("overwrite").parquet(staging)
    print(f"\n{nome} -> {destino}")
    df.show(50, truncate=False)


def main() -> int:
    spark = SparkSession.builder.appName(
        "TCC-Auditoria-Lags-Intramunicipais"
    ).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ler = lambda nome: spark.read.option("mergeSchema", "true").parquet(
            f"{BASE}/{nome}"
        )
        fato_dia = ler("fato_municipio_dia").persist(StorageLevel.MEMORY_AND_DISK)
        saidas = gerar_saidas(
            fato_dia,
            ler("fato_municipio_dia_cid"),
            ler("dim_cid"),
            ler("dim_municipio"),
        )
        for nome, df in saidas.items():
            publicar(nome, df)
        fato_dia.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
