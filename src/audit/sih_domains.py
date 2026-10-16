"""Audita domínios históricos de sexo e codificação de idade do SIH."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.sih_io import ler_silver_sih


def main() -> int:
    spark = SparkSession.builder.appName("TCC-Auditoria-Dominios-SIH").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = ler_silver_sih(spark)
        print("Domínio de SEXO por ano de internação:")
        (
            df.groupBy(F.year("DT_INTER").alias("ano_internacao"), "SEXO")
            .count().orderBy("ano_internacao", "SEXO")
            .show(200, truncate=False)
        )
        print("Registros fora do domínio documentado 1/3:")
        (
            df.filter(~F.col("SEXO").isin("1", "3"))
            .select(
                "SEXO", "ANO_CMPT", "MES_CMPT", "DT_INTER", "MUNIC_RES",
                "DIAG_PRINC", "IDADE", "COD_IDADE",
            )
            .orderBy("ANO_CMPT", "MES_CMPT", "DT_INTER")
            .show(200, truncate=False)
        )
        print("Domínio COD_IDADE:")
        df.groupBy("COD_IDADE").count().orderBy("COD_IDADE").show(100, False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
