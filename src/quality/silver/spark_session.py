"""Sessão dos DQs Silver; runtime e tuning definidos pelo orquestrador.

O helper common permanece inalterado enquanto ainda atende ao DQ Gold.
"""

from pyspark.sql import SparkSession


def criar_spark(nome: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(nome)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
