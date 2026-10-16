"""Audita a cauda de competências SIH que acrescenta internações de 2025."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico
from src.common.sih_io import ler_silver_sih
from src.common.study_period import ANALYSIS_END_DATE


SILVER_SIH = "/home/jovyan/work/data/silver/sih"
SAIDA = "/home/jovyan/work/data/quality/sih_competencias_2025"
ANO_FINAL = ANALYSIS_END_DATE.year


def auditar_competencias(df):
    competencia = F.concat_ws(
        "-",
        F.col("ANO_CMPT").cast("string"),
        F.lpad(F.col("MES_CMPT").cast("string"), 2, "0"),
    )
    return (
        df.withColumn("competencia", competencia)
        .groupBy("competencia")
        .agg(
            F.count("*").cast("long").alias("registros_competencia"),
            F.sum(F.when(F.year("DT_INTER") == ANO_FINAL, 1).otherwise(0))
            .cast("long").alias("internacoes_2025"),
            F.sum(
                F.when(
                    (F.year("DT_INTER") == ANO_FINAL) & (F.month("DT_INTER") == 11), 1
                ).otherwise(0)
            ).cast("long").alias("internacoes_novembro_2025"),
            F.sum(
                F.when(
                    (F.year("DT_INTER") == ANO_FINAL) & (F.month("DT_INTER") == 12), 1
                ).otherwise(0)
            ).cast("long").alias("internacoes_dezembro_2025"),
        )
        .filter(F.col("competencia") >= F.lit(f"{ANO_FINAL}-01"))
        .orderBy("competencia")
    )


def main() -> int:
    spark = (
        SparkSession.builder.appName("TCC-Auditoria-Competencias-SIH")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    try:
        df = ler_silver_sih(spark, SILVER_SIH)
        auditoria = auditar_competencias(df).cache()
        with destino_parquet_atomico(SAIDA) as caminho_staging:
            auditoria.coalesce(1).write.mode("overwrite").parquet(caminho_staging)
        auditoria.show(100, truncate=False)
        ultima = (
            auditoria.filter(F.col("internacoes_2025") > 0)
            .agg(F.max("competencia").alias("competencia"))
            .first()["competencia"]
        )
        posteriores = auditoria.filter(F.col("competencia") > F.lit(ultima)).count()
        print(
            "Última competência que acrescenta DT_INTER de 2025: "
            f"{ultima}; competências posteriores auditadas={posteriores}."
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
