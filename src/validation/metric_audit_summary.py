"""Imprime um extrato compacto das evidências da auditoria analítica."""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE = "/home/jovyan/work/data/validation/auditoria_metricas"
BASE_PERGUNTAS = "/home/jovyan/work/data/validation/perguntas_pesquisa"


def main() -> int:
    spark = SparkSession.builder.appName("TCC-Resumo-Auditoria-Metricas").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ler = lambda nome: spark.read.parquet(f"{BASE}/{nome}")

        selecoes = {
            "COMPARACAO": ler("comparacao_estadual_intramunicipal"),
            "DECIS": ler("contraste_intramunicipal_decis").orderBy("decil"),
            "ANOS": ler("resumo_intramunicipal_ano").orderBy("ano"),
            "CID": ler("resumo_cid_intramunicipal").orderBy("alvo"),
            "COMPOSICAO": ler("composicao_resumo"),
            "COMPOSICAO_DECIS": ler("composicao_decil_volume").orderBy("decil_volume"),
            "TOP_FRIO": ler("composicao_top_dias_frios")
            .select(
                "codigo_municipio", "nome_municipio", "dias_frios", "dias_quentes",
                "internacoes_total", "media_internacoes_periodo",
                "participacao_dias_frios", "participacao_internacoes_frio",
            )
            .orderBy(F.desc("dias_frios")),
            "TOP_QUENTE": ler("composicao_top_dias_quentes")
            .select(
                "codigo_municipio", "nome_municipio", "dias_frios", "dias_quentes",
                "internacoes_total", "media_internacoes_periodo",
                "participacao_dias_quentes", "participacao_internacoes_quente",
            )
            .orderBy(F.desc("dias_quentes")),
            "DENOMINADORES_SEXO": ler("auditoria_denominador_sexo").orderBy("sexo"),
            "DENOMINADOR_J18": ler("auditoria_denominador_categoria")
            .filter(F.col("codigo_categoria") == "J18"),
            "DENOMINADOR_O80": ler("auditoria_denominador_categoria")
            .filter(F.col("codigo_categoria") == "O80"),
            "DEFASAGENS": spark.read.parquet(
                f"{BASE_PERGUNTAS}/pergunta_5_defasagens"
            ).orderBy("defasagem"),
        }
        for titulo, df in selecoes.items():
            print(f"\n### {titulo}")
            limite = 10 if titulo.startswith("TOP_") else 100
            df.show(limite, truncate=False, vertical=False)
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
