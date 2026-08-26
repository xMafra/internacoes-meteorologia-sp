import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport, imprimir_relatorio, validar_chave_unica,
    validar_colunas_obrigatorias, validar_condicao, validar_nulos,
    validar_quantidade_distinta, validar_quantidade_registros, validar_tipos,
)
from src.quality.silver.common import criar_spark
from src.quality.silver.quality_config import CID10_CODIGOS_ESPERADOS, CID10_PATH


COLUNAS = [
    "codigo_cid", "descricao_cid", "codigo_categoria", "descricao_categoria",
    "cat_inicial_grupo", "cat_final_grupo", "descricao_grupo",
    "codigo_capitulo", "descricao_capitulo",
]


def validar_cid10(df) -> QualityReport:
    report = QualityReport("SILVER CID-10")
    if not validar_colunas_obrigatorias(df, COLUNAS, report):
        return report
    validar_tipos(df, {coluna: "string" for coluna in COLUNAS}, report)
    validar_quantidade_registros(df, CID10_CODIGOS_ESPERADOS, report)
    validar_quantidade_distinta(df, "codigo_cid", CID10_CODIGOS_ESPERADOS, report, "Códigos oficiais únicos")
    validar_chave_unica(df, ["codigo_cid"], report)
    validar_nulos(df, COLUNAS, report)
    validar_condicao(df, ~F.col("codigo_cid").rlike(r"^[A-Z][0-9]{2}[0-9A-Z]?$"), "Formato do código CID-10", report)
    validar_condicao(df, F.substring("codigo_cid", 1, 3) != F.col("codigo_categoria"), "CID pertence à categoria informada", report)
    validar_condicao(
        df,
        (F.col("codigo_categoria") < F.col("cat_inicial_grupo")) | (F.col("codigo_categoria") > F.col("cat_final_grupo")),
        "Categoria dentro do intervalo do grupo",
        report,
    )
    validar_condicao(
        df,
        ~F.col("codigo_capitulo").rlike(r"^\d{1,2}$")
        | ~F.col("codigo_capitulo").cast("int").between(1, 22),
        "Código de capítulo padronizado",
        report,
    )
    return report


def main() -> int:
    spark = criar_spark("DQ-Silver-CID10")
    try:
        report = validar_cid10(spark.read.parquet(CID10_PATH))
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
