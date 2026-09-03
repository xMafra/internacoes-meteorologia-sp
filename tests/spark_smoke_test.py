from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("TCC-Airflow-Spark-Smoke-Test")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        print("=" * 60)
        print("TESTE AIRFLOW WORKER + APACHE SPARK")
        print("=" * 60)

        print(f"Versão do Spark: {spark.version}")

        df = (
            spark.range(1, 6)
            .withColumnRenamed("id", "numero")
        )

        print("")
        print("DataFrame criado:")
        df.show()

        quantidade = df.count()

        soma = (
            df
            .agg(
                F.sum("numero").alias("soma")
            )
            .first()["soma"]
        )

        print(f"Quantidade de registros: {quantidade}")
        print(f"Soma dos números: {soma}")

        if quantidade != 5:
            raise ValueError(
                "Quantidade incorreta. "
                f"Esperado=5, obtido={quantidade}"
            )

        if soma != 15:
            raise ValueError(
                "Soma incorreta. "
                f"Esperado=15, obtido={soma}"
            )

        print("")
        print("TESTE CONCLUÍDO COM SUCESSO.")
        print("O Airflow Worker conseguiu executar Apache Spark.")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()