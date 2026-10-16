"""Auditoria metodológica das métricas frio x quente do TCC.

O módulo mantém separado o contraste estadual, ponderado por município-dia,
do contraste intramunicipal, no qual cada município elegível contribui uma vez
para o resumo da distribuição.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico


BASE = "/home/jovyan/work/data/consumption"
SAIDA = "/home/jovyan/work/data/validation/auditoria_metricas"
AMOSTRA_MINIMA = 30
LIMIAR_APROX_ZERO = 0.01


def adicionar_metricas_contraste(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "media_frio",
            F.when(F.col("dias_frios") > 0,
                   F.col("internacoes_frio") / F.col("dias_frios")),
        )
        .withColumn(
            "media_quente",
            F.when(F.col("dias_quentes") > 0,
                   F.col("internacoes_quente") / F.col("dias_quentes")),
        )
        .withColumn("diferenca_absoluta", F.col("media_frio") - F.col("media_quente"))
        .withColumn(
            "diferenca_relativa",
            F.when(F.col("media_quente") != 0,
                   F.col("diferenca_absoluta") / F.col("media_quente")),
        )
        .withColumn(
            "elegivel_amostra",
            (
                (F.col("dias_frios") >= AMOSTRA_MINIMA)
                & (F.col("dias_quentes") >= AMOSTRA_MINIMA)
            ).cast("int"),
        )
        .withColumn(
            "contraste_relativo_valido",
            (
                (F.col("elegivel_amostra") == 1)
                & F.col("diferenca_relativa").isNotNull()
            ).cast("int"),
        )
    )


def contraste_frio_quente(
    df: DataFrame,
    grupos: list[str],
    temperatura: str = "temperatura_media_dia_c",
    internacoes: str = "qtd_internacoes",
) -> DataFrame:
    frio = F.col(temperatura) < 20
    quente = F.col(temperatura) >= 25
    agregado = df.groupBy(*grupos).agg(
        F.sum(F.when(frio, F.col(internacoes)).otherwise(0)).alias("internacoes_frio"),
        F.sum(F.when(quente, F.col(internacoes)).otherwise(0)).alias("internacoes_quente"),
        F.sum(F.when(frio, 1).otherwise(0)).cast("long").alias("dias_frios"),
        F.sum(F.when(quente, 1).otherwise(0)).cast("long").alias("dias_quentes"),
    )
    return adicionar_metricas_contraste(agregado)


def resumo_distribuicao(
    contrastes: DataFrame,
    grupos: list[str],
    entidade="municipios",
) -> DataFrame:
    valido = F.col("contraste_relativo_valido") == 1
    estatisticas = contrastes.groupBy(*grupos).agg(
        F.count("*").cast("long").alias(f"{entidade}_total"),
        F.sum("elegivel_amostra").cast("long").alias(f"{entidade}_elegiveis"),
        F.sum("contraste_relativo_valido").cast("long").alias(f"{entidade}_validos"),
        F.avg(F.when(valido, F.col("diferenca_relativa"))).alias("media_diferenca_relativa"),
        F.percentile_approx(
            F.when(valido, F.col("diferenca_relativa")),
            [0.25, 0.5, 0.75],
            10000,
        ).alias("quartis"),
        F.min(F.when(valido, F.col("diferenca_relativa"))).alias("minimo"),
        F.max(F.when(valido, F.col("diferenca_relativa"))).alias("maximo"),
        F.sum(F.when(valido & (F.col("diferenca_relativa") > 0), 1).otherwise(0))
        .alias("qtd_positivos"),
        F.sum(F.when(valido & (F.col("diferenca_relativa") < 0), 1).otherwise(0))
        .alias("qtd_negativos"),
        F.sum(F.when(valido & (F.col("diferenca_relativa") == 0), 1).otherwise(0))
        .alias("qtd_iguais_zero"),
        F.sum(
            F.when(
                valido & (F.col("diferenca_relativa") > LIMIAR_APROX_ZERO), 1
            ).otherwise(0)
        ).alias("qtd_acima_limiar"),
        F.sum(
            F.when(
                valido & (F.col("diferenca_relativa") < -LIMIAR_APROX_ZERO), 1
            ).otherwise(0)
        ).alias("qtd_abaixo_limiar"),
        F.sum(
            F.when(
                valido & (F.abs(F.col("diferenca_relativa")) <= LIMIAR_APROX_ZERO),
                1,
            ).otherwise(0)
        ).alias("qtd_aprox_zero"),
    )
    n = F.col(f"{entidade}_validos")
    return (
        estatisticas
        .withColumn("q1", F.col("quartis")[0])
        .withColumn("mediana_diferenca_relativa", F.col("quartis")[1])
        .withColumn("q3", F.col("quartis")[2])
        .drop("quartis")
        .withColumn("percentual_positivo", 100 * F.col("qtd_positivos") / n)
        .withColumn("percentual_negativo", 100 * F.col("qtd_negativos") / n)
        .withColumn("percentual_igual_zero", 100 * F.col("qtd_iguais_zero") / n)
        .withColumn("percentual_acima_limiar", 100 * F.col("qtd_acima_limiar") / n)
        .withColumn("percentual_abaixo_limiar", 100 * F.col("qtd_abaixo_limiar") / n)
        .withColumn("percentual_aprox_zero", 100 * F.col("qtd_aprox_zero") / n)
        .withColumn("limiar_aprox_zero", F.lit(LIMIAR_APROX_ZERO))
    )


def distribuicao_decis(contrastes: DataFrame) -> DataFrame:
    validos = contrastes.filter(F.col("contraste_relativo_valido") == 1)
    janela = Window.orderBy("diferenca_relativa")
    return (
        validos.withColumn("decil", F.ntile(10).over(janela))
        .groupBy("decil")
        .agg(
            F.count("*").alias("municipios"),
            F.min("diferenca_relativa").alias("minimo"),
            F.percentile_approx("diferenca_relativa", 0.5, 10000).alias("mediana"),
            F.avg("diferenca_relativa").alias("media"),
            F.max("diferenca_relativa").alias("maximo"),
        )
        .orderBy("decil")
    )


def denominadores_exposicao(fato_dia: DataFrame) -> DataFrame:
    return fato_dia.agg(
        F.sum((F.col("temperatura_media_dia_c") < 20).cast("int"))
        .cast("long").alias("municipio_dias_exposicao_frio"),
        F.sum((F.col("temperatura_media_dia_c") >= 25).cast("int"))
        .cast("long").alias("municipio_dias_exposicao_quente"),
    )


def auditoria_denominador_esparso(
    eventos: DataFrame,
    grupos: list[str],
    denominadores: DataFrame,
) -> DataFrame:
    frio = F.col("temperatura_media_dia_c") < 20
    quente = F.col("temperatura_media_dia_c") >= 25
    return (
        eventos.groupBy(*grupos)
        .agg(
            F.sum(F.when(frio, F.col("qtd_internacoes")).otherwise(0))
            .alias("internacoes_frio"),
            F.sum(F.when(quente, F.col("qtd_internacoes")).otherwise(0))
            .alias("internacoes_quente"),
            F.countDistinct(F.when(frio, F.col("municipio_dia_id")))
            .alias("municipio_dias_com_evento_frio"),
            F.countDistinct(F.when(quente, F.col("municipio_dia_id")))
            .alias("municipio_dias_com_evento_quente"),
        )
        .crossJoin(denominadores)
        .withColumn(
            "media_correta_frio",
            F.col("internacoes_frio") / F.col("municipio_dias_exposicao_frio"),
        )
        .withColumn(
            "media_correta_quente",
            F.col("internacoes_quente") / F.col("municipio_dias_exposicao_quente"),
        )
        .withColumn(
            "media_incorreta_so_dias_evento_frio",
            F.when(F.col("municipio_dias_com_evento_frio") > 0,
                   F.col("internacoes_frio") / F.col("municipio_dias_com_evento_frio")),
        )
        .withColumn(
            "media_incorreta_so_dias_evento_quente",
            F.when(F.col("municipio_dias_com_evento_quente") > 0,
                   F.col("internacoes_quente") / F.col("municipio_dias_com_evento_quente")),
        )
    )


def criar_eventos_alvo_cid(eventos: DataFrame) -> DataFrame:
    capitulos = (
        eventos.filter(F.col("codigo_capitulo").isin("9", "10", "15"))
        .withColumn(
            "alvo",
            F.concat(F.lit("CAPITULO_"), F.col("codigo_capitulo")),
        )
        .withColumn("descricao_alvo", F.col("descricao_capitulo"))
    )
    grupos = (
        eventos.filter(F.col("codigo_grupo").isin("J09-J18", "J20-J22", "O80-O84"))
        .withColumn("alvo", F.concat(F.lit("GRUPO_"), F.col("codigo_grupo")))
        .withColumn("descricao_alvo", F.col("descricao_grupo"))
    )
    return capitulos.unionByName(grupos, allowMissingColumns=True)


def contraste_cid_intramunicipal(
    fato_dia: DataFrame,
    eventos_cid: DataFrame,
) -> DataFrame:
    exposicao = contraste_frio_quente(
        fato_dia.withColumn("_zero", F.lit(0)),
        ["codigo_municipio"],
        internacoes="_zero",
    ).select("codigo_municipio", "dias_frios", "dias_quentes")

    alvos = criar_eventos_alvo_cid(eventos_cid).persist(StorageLevel.MEMORY_AND_DISK)
    dimensao_alvos = alvos.select("alvo", "descricao_alvo").dropDuplicates()
    frio = F.col("temperatura_media_dia_c") < 20
    quente = F.col("temperatura_media_dia_c") >= 25
    numeradores = alvos.groupBy("codigo_municipio", "alvo", "descricao_alvo").agg(
        F.sum(F.when(frio, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_frio"),
        F.sum(F.when(quente, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_quente"),
    )
    resultado = (
        exposicao.crossJoin(dimensao_alvos)
        .join(numeradores, ["codigo_municipio", "alvo", "descricao_alvo"], "left")
        .fillna(0, subset=["internacoes_frio", "internacoes_quente"])
    )
    alvos.unpersist()
    return adicionar_metricas_contraste(resultado)


def analise_composicao(fato_dia: DataFrame, contraste_municipio: DataFrame):
    volume = fato_dia.groupBy("codigo_municipio").agg(
        F.sum("qtd_internacoes").alias("internacoes_total"),
        F.count("*").alias("municipio_dias_total"),
    ).withColumn(
        "media_internacoes_periodo",
        F.col("internacoes_total") / F.col("municipio_dias_total"),
    )
    base = contraste_municipio.join(volume, "codigo_municipio")
    totais = base.agg(
        F.sum("dias_frios").alias("total_dias_frios"),
        F.sum("dias_quentes").alias("total_dias_quentes"),
        F.sum("internacoes_frio").alias("total_internacoes_frio"),
        F.sum("internacoes_quente").alias("total_internacoes_quente"),
    )
    detalhada = (
        base.crossJoin(totais)
        .withColumn("participacao_dias_frios", F.col("dias_frios") / F.col("total_dias_frios"))
        .withColumn("participacao_dias_quentes", F.col("dias_quentes") / F.col("total_dias_quentes"))
        .withColumn(
            "participacao_internacoes_frio",
            F.col("internacoes_frio") / F.col("total_internacoes_frio"),
        )
        .withColumn(
            "participacao_internacoes_quente",
            F.col("internacoes_quente") / F.col("total_internacoes_quente"),
        )
        .withColumn(
            "decil_volume",
            F.ntile(10).over(Window.orderBy("internacoes_total")),
        )
    )
    resumo = detalhada.agg(
        F.corr("internacoes_total", "dias_frios").alias("correlacao_volume_dias_frios"),
        F.corr("internacoes_total", "dias_quentes").alias("correlacao_volume_dias_quentes"),
        (
            F.sum(F.col("media_internacoes_periodo") * F.col("dias_frios"))
            / F.sum("dias_frios")
        ).alias("volume_base_ponderado_por_dias_frios"),
        (
            F.sum(F.col("media_internacoes_periodo") * F.col("dias_quentes"))
            / F.sum("dias_quentes")
        ).alias("volume_base_ponderado_por_dias_quentes"),
        (F.sum("internacoes_frio") / F.sum("dias_frios")).alias("media_agregada_frio"),
        (F.sum("internacoes_quente") / F.sum("dias_quentes")).alias("media_agregada_quente"),
    ).withColumn(
        "diferenca_base_composicao",
        F.col("volume_base_ponderado_por_dias_frios")
        - F.col("volume_base_ponderado_por_dias_quentes"),
    )
    por_decil = detalhada.groupBy("decil_volume").agg(
        F.count("*").alias("municipios"),
        F.avg("internacoes_total").alias("media_internacoes_total"),
        F.avg("media_internacoes_periodo").alias("media_volume_diario"),
        F.sum("dias_frios").alias("dias_frios"),
        F.sum("dias_quentes").alias("dias_quentes"),
        F.sum("participacao_dias_frios").alias("participacao_dias_frios"),
        F.sum("participacao_dias_quentes").alias("participacao_dias_quentes"),
    ).orderBy("decil_volume")
    return detalhada, resumo, por_decil


def publicar(nome: str, df: DataFrame):
    destino = f"{SAIDA}/{nome}"
    with destino_parquet_atomico(destino) as staging:
        df.write.mode("overwrite").parquet(staging)
    print(f"\n{nome} -> {destino}")
    df.show(30, truncate=False)


def main() -> int:
    spark = SparkSession.builder.appName("TCC-Auditoria-Metricas-Analiticas").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ler = lambda nome: spark.read.option("mergeSchema", "true").parquet(f"{BASE}/{nome}")
        fato_dia = ler("fato_municipio_dia").persist(StorageLevel.MEMORY_AND_DISK)
        fato_cid = ler("fato_municipio_dia_cid")
        dim_cid = ler("dim_cid")
        dim_municipio = ler("dim_municipio").select("codigo_municipio", "nome_municipio")

        exposicao = fato_dia.select("municipio_dia_id", "temperatura_media_dia_c")
        eventos = (
            fato_cid.join(exposicao, "municipio_dia_id", "inner")
            .join(dim_cid, "codigo_categoria", "left")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )
        denominadores = denominadores_exposicao(fato_dia).persist(StorageLevel.MEMORY_AND_DISK)

        estadual = contraste_frio_quente(fato_dia, [])
        municipal = contraste_frio_quente(fato_dia, ["codigo_municipio"]).join(
            dim_municipio, "codigo_municipio", "left"
        ).persist(StorageLevel.MEMORY_AND_DISK)
        resumo_municipal = resumo_distribuicao(municipal, [])
        comparacao = (
            estadual.select(
                F.col("media_frio").alias("media_agregada_frio"),
                F.col("media_quente").alias("media_agregada_quente"),
                F.col("diferenca_absoluta").alias("diferenca_absoluta_agregada"),
                F.col("diferenca_relativa").alias("diferenca_relativa_agregada"),
                "dias_frios", "dias_quentes",
            )
            .crossJoin(resumo_municipal)
        )

        municipal_ano = contraste_frio_quente(
            fato_dia, ["ano", "codigo_municipio"]
        ).persist(StorageLevel.MEMORY_AND_DISK)
        resumo_ano = resumo_distribuicao(municipal_ano, ["ano"]).orderBy("ano")

        audit_capitulo = auditoria_denominador_esparso(
            eventos, ["codigo_capitulo", "descricao_capitulo"], denominadores
        )
        audit_grupo = auditoria_denominador_esparso(
            eventos,
            ["codigo_capitulo", "codigo_grupo", "descricao_grupo"],
            denominadores,
        )
        audit_categoria = auditoria_denominador_esparso(
            eventos,
            ["codigo_capitulo", "codigo_grupo", "codigo_categoria", "descricao_categoria"],
            denominadores,
        )
        audit_idade = auditoria_denominador_esparso(
            eventos, ["faixa_etaria"], denominadores
        )
        audit_sexo = auditoria_denominador_esparso(
            eventos, ["sexo", "sexo_descricao"], denominadores
        )
        audit_cid_idade = auditoria_denominador_esparso(
            eventos, ["codigo_categoria", "faixa_etaria"], denominadores
        )
        audit_cid_sexo = auditoria_denominador_esparso(
            eventos, ["codigo_categoria", "sexo", "sexo_descricao"], denominadores
        )
        audit_cid_idade_sexo = auditoria_denominador_esparso(
            eventos,
            ["codigo_categoria", "faixa_etaria", "sexo", "sexo_descricao"],
            denominadores,
        )

        cid_municipal = contraste_cid_intramunicipal(fato_dia, eventos).persist(
            StorageLevel.MEMORY_AND_DISK
        )
        cid_resumo = resumo_distribuicao(cid_municipal, ["alvo", "descricao_alvo"])

        composicao, composicao_resumo, composicao_decil = analise_composicao(
            fato_dia, municipal
        )

        saidas = {
            "comparacao_estadual_intramunicipal": comparacao,
            "contraste_intramunicipal": municipal,
            "contraste_intramunicipal_decis": distribuicao_decis(municipal),
            "contraste_intramunicipal_ano": municipal_ano,
            "resumo_intramunicipal_ano": resumo_ano,
            "denominadores_exposicao": denominadores,
            "auditoria_denominador_capitulo": audit_capitulo,
            "auditoria_denominador_grupo": audit_grupo,
            "auditoria_denominador_categoria": audit_categoria,
            "auditoria_denominador_idade": audit_idade,
            "auditoria_denominador_sexo": audit_sexo,
            "auditoria_denominador_cid_idade": audit_cid_idade,
            "auditoria_denominador_cid_sexo": audit_cid_sexo,
            "auditoria_denominador_cid_idade_sexo": audit_cid_idade_sexo,
            "contraste_cid_intramunicipal": cid_municipal,
            "resumo_cid_intramunicipal": cid_resumo,
            "composicao_municipio": composicao,
            "composicao_resumo": composicao_resumo,
            "composicao_decil_volume": composicao_decil,
            "composicao_top_dias_frios": composicao.orderBy(F.desc("dias_frios")).limit(30),
            "composicao_top_dias_quentes": composicao.orderBy(F.desc("dias_quentes")).limit(30),
        }
        for nome, df in saidas.items():
            publicar(nome, df)

        cid_municipal.unpersist()
        municipal_ano.unpersist()
        municipal.unpersist()
        denominadores.unpersist()
        eventos.unpersist()
        fato_dia.unpersist()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
