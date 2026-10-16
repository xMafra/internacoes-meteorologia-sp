"""Data Quality das evidências intramunicipais de defasagem."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/home/jovyan/work/data/validation/auditoria_lags_intramunicipais"
LAGS = {"D0", "D-1", "D-3", "D-7"}
ALVOS = {"CAPITULO_10", "GRUPO_J09-J18", "GRUPO_J20-J22"}


def main() -> int:
    spark = SparkSession.builder.appName("DQ-Lags-Intramunicipais").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    falhas: list[str] = []

    def verificar(condicao: bool, descricao: str) -> None:
        status = "PASS" if condicao else "FAIL"
        print(f"[{status}] {descricao}")
        if not condicao:
            falhas.append(descricao)

    try:
        total = spark.read.parquet(f"{BASE}/contraste_total_municipio_lag")
        resumo_total = spark.read.parquet(f"{BASE}/resumo_total_lag")
        respiratorio = spark.read.parquet(
            f"{BASE}/contraste_respiratorio_municipio_lag"
        )
        resumo_respiratorio = spark.read.parquet(
            f"{BASE}/resumo_respiratorio_lag"
        )

        requeridas = {
            "defasagem", "codigo_municipio", "media_frio_municipio",
            "media_quente_municipio", "diferenca_absoluta",
            "diferenca_relativa", "dias_frios", "dias_quentes",
            "elegivel_amostra", "contraste_relativo_valido",
        }
        verificar(requeridas <= set(total.columns), "schema do detalhe total")
        verificar(
            requeridas | {"alvo", "descricao_alvo"} <= set(respiratorio.columns),
            "schema do detalhe respiratório",
        )
        verificar(
            {r.defasagem for r in total.select("defasagem").distinct().collect()} == LAGS,
            "domínio de defasagens no detalhe total",
        )
        verificar(
            {r.defasagem for r in respiratorio.select("defasagem").distinct().collect()} == LAGS,
            "domínio de defasagens no detalhe respiratório",
        )
        verificar(
            {r.alvo for r in respiratorio.select("alvo").distinct().collect()} == ALVOS,
            "domínio dos alvos respiratórios",
        )

        duplicados_total = total.groupBy(
            "codigo_municipio", "defasagem"
        ).count().filter(F.col("count") != 1).count()
        duplicados_resp = respiratorio.groupBy(
            "codigo_municipio", "alvo", "defasagem"
        ).count().filter(F.col("count") != 1).count()
        verificar(duplicados_total == 0, "unicidade município + defasagem")
        verificar(
            duplicados_resp == 0,
            "unicidade município + alvo respiratório + defasagem",
        )

        municipios = total.select("codigo_municipio").distinct().count()
        verificar(
            total.count() == municipios * len(LAGS),
            "painel completo de municípios por defasagem",
        )
        verificar(
            respiratorio.count() == municipios * len(LAGS) * len(ALVOS),
            "painel respiratório completo município × alvo × defasagem",
        )

        for nome, df in (("total", total), ("respiratório", respiratorio)):
            elegibilidade_incorreta = df.filter(
                F.col("elegivel_amostra")
                != (
                    (F.col("dias_frios") >= 30) & (F.col("dias_quentes") >= 30)
                ).cast("int")
            ).count()
            validade_incorreta = df.filter(
                F.col("contraste_relativo_valido")
                != (
                    (F.col("elegivel_amostra") == 1)
                    & F.col("diferenca_relativa").isNotNull()
                ).cast("int")
            ).count()
            verificar(
                elegibilidade_incorreta == 0,
                f"elegibilidade independente por lag — {nome}",
            )
            verificar(
                validade_incorreta == 0,
                f"validade da diferença relativa — {nome}",
            )

        verificar(resumo_total.count() == 4, "quatro linhas no resumo total")
        verificar(
            resumo_respiratorio.count() == 12,
            "doze linhas no resumo respiratório",
        )
        resumo_requerido = {
            "defasagem", "municipios_validos", "mediana_diferenca_relativa",
            "media_diferenca_relativa", "q1", "q3", "minimo", "maximo",
            "percentual_positivo", "percentual_negativo",
            "percentual_aprox_zero",
        }
        verificar(
            resumo_requerido <= set(resumo_total.columns),
            "schema solicitado no resumo total",
        )
        verificar(
            resumo_requerido | {"alvo", "descricao_alvo"}
            <= set(resumo_respiratorio.columns),
            "schema solicitado no resumo respiratório",
        )

        for nome, detalhe, resumo, grupos in (
            ("total", total, resumo_total, ["defasagem"]),
            (
                "respiratório",
                respiratorio,
                resumo_respiratorio,
                ["alvo", "defasagem"],
            ),
        ):
            calculado = detalhe.groupBy(*grupos).agg(
                F.sum("contraste_relativo_valido").alias("validos_recalculados")
            )
            divergencias = resumo.join(calculado, grupos).filter(
                F.col("municipios_validos") != F.col("validos_recalculados")
            ).count()
            verificar(
                divergencias == 0,
                f"municípios válidos reconciliados — {nome}",
            )

        print("\nSTATUS FINAL: " + ("FAIL" if falhas else "PASS"))
        return 1 if falhas else 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
