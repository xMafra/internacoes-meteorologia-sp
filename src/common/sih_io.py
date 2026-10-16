"""Leitura explícita das competências publicadas da Silver SIH."""

from src.common.study_period import SIH_COMPETENCES


def caminhos_silver_sih(
    raiz: str = "/home/jovyan/work/data/silver/sih",
) -> list[str]:
    """Retorna somente os diretórios mensais oficiais do recorte configurado."""
    return [
        f"{raiz}/{ano}/{mes:02d}"
        for ano, mes in SIH_COMPETENCES
    ]


def ler_silver_sih(spark, raiz: str = "/home/jovyan/work/data/silver/sih"):
    """Evita que staging, backups ou outros derivados entrem por recursão."""
    return (
        spark.read.option("mergeSchema", "true")
        .parquet(*caminhos_silver_sih(raiz))
    )

