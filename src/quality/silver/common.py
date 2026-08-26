from pyspark.sql import SparkSession


def criar_spark(nome: str) -> SparkSession:
    spark = (
        SparkSession.builder.appName(nome)
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
