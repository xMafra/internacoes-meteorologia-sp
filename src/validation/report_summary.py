"""Exibe recortes compactos das saídas analíticas para o relatório final."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/home/jovyan/work/data/validation/perguntas_pesquisa"
CONSUMPTION = "/home/jovyan/work/data/consumption"


def mostrar_extremos(spark, nome, colunas, positivos=10, negativos=5):
    df = (
        spark.read.parquet(f"{BASE}/{nome}")
        .filter(
            (F.col("amostra_valida") == 1)
            & F.col("diferenca_relativa").isNotNull()
        )
    )
    print(f"\n{nome} — maiores diferenças absolutas")
    df.orderBy(F.desc("diferenca_absoluta")).select(*colunas).show(
        positivos, truncate=False
    )
    print(f"\n{nome} — menores diferenças absolutas")
    df.orderBy(F.asc("diferenca_absoluta")).select(*colunas).show(
        negativos, truncate=False
    )


def main():
    spark = SparkSession.builder.appName("TCC-Resumo-Relatorio").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        print("\nCardinalidades da camada Consumption")
        for nome in (
            "dim_municipio", "dim_cid", "dim_tempo", "dim_estacao",
            "fato_municipio_dia", "fato_municipio_dia_cid",
        ):
            total = spark.read.option("mergeSchema", "true").parquet(
                f"{CONSUMPTION}/{nome}"
            ).count()
            print(f"{nome}={total}")
        mostrar_extremos(
            spark,
            "pergunta_3_grupo",
            [
                "codigo_grupo", "descricao_grupo", "internacoes_frio",
                "internacoes_quente", "media_frio", "media_quente",
                "diferenca_absoluta", "diferenca_relativa",
            ],
        )
        mostrar_extremos(
            spark,
            "pergunta_3_cid",
            [
                "codigo_categoria", "descricao_categoria", "internacoes_frio",
                "internacoes_quente", "media_frio", "media_quente",
                "diferenca_absoluta", "diferenca_relativa",
            ],
        )
        mostrar_extremos(
            spark,
            "pergunta_6_municipio",
            [
                "codigo_municipio", "nome_municipio", "dias_frios",
                "dias_quentes", "media_frio", "media_quente",
                "diferenca_absoluta", "diferenca_relativa",
            ],
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
