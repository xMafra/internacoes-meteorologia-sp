"""Tabelas analíticas compactas para o dashboard das seis perguntas.

As tabelas mantêm o painel município-dia como denominador e atribuem o
mesmo peso a cada município elegível nos resumos intramunicipais. As entradas oficiais
main/gerar_saidas usam P20/P80 municipal (B). Auxiliares absolutos históricos
permanecem para compatibilidade e testes; não definem a metodologia principal.
"""

from __future__ import annotations

import sys
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.common.spark_io import destino_parquet_atomico


BASE = "/home/jovyan/work/data/consumption"
SAIDA = f"{BASE}/dashboard_analytics"
AMOSTRA_MINIMA = 30
LIMIAR_APROX_ZERO = 0.01
DEFASAGENS = (
    ("D0", 0, "temperatura_media_dia_c"),
    ("D-1", 1, "temperatura_media_lag_1d"),
    ("D-3", 3, "temperatura_media_lag_3d"),
    ("D-7", 7, "temperatura_media_lag_7d"),
)
FAIXAS_ETARIAS = (("0-17", 1), ("18-39", 2), ("40-59", 3), ("60-79", 4), ("80+", 5))
SEXOS = (("1", "Masculino", 1), ("3", "Feminino", 2))


def _unir(dataframes: list[DataFrame]) -> DataFrame:
    return reduce(lambda a, b: a.unionByName(b), dataframes)


def _adicionar_metricas(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "media_frio_municipio",
            F.when(F.col("dias_frios") > 0, F.col("internacoes_frio") / F.col("dias_frios")),
        )
        .withColumn(
            "media_quente_municipio",
            F.when(F.col("dias_quentes") > 0, F.col("internacoes_quente") / F.col("dias_quentes")),
        )
        .withColumn(
            "diferenca_absoluta",
            F.col("media_frio_municipio") - F.col("media_quente_municipio"),
        )
        .withColumn(
            "diferenca_relativa",
            F.when(
                F.col("media_quente_municipio") != 0,
                F.col("diferenca_absoluta") / F.col("media_quente_municipio"),
            ),
        )
        .withColumn("diferenca_por_100_municipio_dias", 100 * F.col("diferenca_absoluta"))
        .withColumn("diferenca_zero", F.col("diferenca_absoluta") == 0)
        .withColumn("media_frio_zero", F.col("media_frio_municipio") == 0)
        .withColumn("media_quente_zero", F.col("media_quente_municipio") == 0)
        .withColumn("ambas_medias_zero", F.col("media_frio_zero") & F.col("media_quente_zero"))
        .withColumn("direcao_diferenca", F.when(F.col("diferenca_absoluta").isNull(), "NAO_APLICAVEL")
                    .when(F.col("diferenca_absoluta") > 0, "FRIO")
                    .when(F.col("diferenca_absoluta") < 0, "QUENTE").otherwise("IGUAL"))
        .withColumn(
            "elegivel_amostra",
            ((F.col("dias_frios") >= AMOSTRA_MINIMA) & (F.col("dias_quentes") >= AMOSTRA_MINIMA)).cast("int"),
        )
        .withColumn(
            "contraste_relativo_valido",
            ((F.col("elegivel_amostra") == 1) & F.col("diferenca_relativa").isNotNull()).cast("int"),
        )
        .withColumn("contraste_absoluto_valido", ((F.col("elegivel_amostra") == 1)
                    & F.col("media_frio_municipio").isNotNull()
                    & F.col("media_quente_municipio").isNotNull()).cast("int"))
        .withColumn("classe_consistencia", F.when(F.col("contraste_relativo_valido") != 1, "NAO_APLICAVEL")
                    .when(F.abs(F.col("diferenca_relativa")) <= LIMIAR_APROX_ZERO, "PEQUENA")
                    .when(F.col("diferenca_absoluta") > 0, "FRIO").otherwise("QUENTE"))
    )


def _contraste(
    df: DataFrame,
    grupos: list[str],
    temperatura: str = "temperatura_media_dia_c",
    internacoes: str = "qtd_internacoes",
) -> DataFrame:
    frio = F.col(temperatura) < 20
    quente = F.col(temperatura) >= 25
    return _adicionar_metricas(
        df.groupBy(*grupos).agg(
            F.sum(F.when(frio, F.col(internacoes)).otherwise(0)).cast("long").alias("internacoes_frio"),
            F.sum(F.when(quente, F.col(internacoes)).otherwise(0)).cast("long").alias("internacoes_quente"),
            F.sum(F.when(frio, 1).otherwise(0)).cast("long").alias("dias_frios"),
            F.sum(F.when(quente, 1).otherwise(0)).cast("long").alias("dias_quentes"),
            F.sum(F.col(internacoes)).cast("long").alias("internacoes_total"),
            F.count("*").cast("long").alias("municipio_dias_total"),
        )
    )


def _resumo(contrastes: DataFrame, grupos: list[str]) -> DataFrame:
    valido = F.col("contraste_relativo_valido") == 1
    n = F.col("municipios_validos")
    absoluto = F.col("contraste_absoluto_valido") == 1
    return (
        contrastes.groupBy(*grupos)
        .agg(
            F.count("*").cast("long").alias("municipios_total"),

            F.sum("contraste_absoluto_valido").cast("long").alias("municipios_validos_absoluto"),
            F.percentile_approx(F.when(absoluto, F.col("diferenca_absoluta")), [0.25, 0.5, 0.75], 10000).alias("quartis_absolutos"),
            F.percentile_approx(F.when(absoluto, F.col("media_frio_municipio")), 0.5, 10000).alias("mediana_media_frio_elegiveis"),
            F.percentile_approx(F.when(absoluto, F.col("media_quente_municipio")), 0.5, 10000).alias("mediana_media_quente_elegiveis"),
            F.sum(F.when(absoluto & F.col("media_frio_zero"), 1).otherwise(0)).cast("long").alias("municipios_frio_zero"),
            F.sum(F.when(absoluto & F.col("media_quente_zero"), 1).otherwise(0)).cast("long").alias("municipios_quente_zero"),
            F.sum(F.when(absoluto & F.col("ambas_medias_zero"), 1).otherwise(0)).cast("long").alias("municipios_ambas_medias_zero"),
            F.sum(F.when(valido & F.col("media_frio_zero"), 1).otherwise(0)).cast("long").alias("municipios_frio_zero_relativo"),
            F.sum(F.when(absoluto & F.col("media_frio_zero") & (F.col("media_quente_municipio") > 0), 1).otherwise(0)).cast("long").alias("municipios_so_quente"),
            F.sum(F.when(absoluto & F.col("media_quente_zero") & (F.col("media_frio_municipio") > 0), 1).otherwise(0)).cast("long").alias("municipios_so_frio"),

            F.sum(F.when(absoluto & (F.col("diferenca_absoluta") > 0), 1).otherwise(0)).cast("long").alias("municipios_maior_frio_absoluto"),
            F.sum(F.when(absoluto & (F.col("diferenca_absoluta") < 0), 1).otherwise(0)).cast("long").alias("municipios_maior_quente_absoluto"),
            F.sum(F.when(absoluto & (F.col("diferenca_absoluta") == 0), 1).otherwise(0)).cast("long").alias("municipios_medias_iguais_absoluto"),
            F.sum(F.when(valido & (F.col("classe_consistencia") == "FRIO"), 1).otherwise(0)).cast("long").alias("municipios_frio_consistencia"),
            F.sum(F.when(valido & (F.col("classe_consistencia") == "QUENTE"), 1).otherwise(0)).cast("long").alias("municipios_quente_consistencia"),
            F.sum(F.when(valido & (F.col("classe_consistencia") == "PEQUENA"), 1).otherwise(0)).cast("long").alias("municipios_pequena_consistencia"),
            F.sum(F.when(absoluto, F.col("internacoes_total")).otherwise(0)).cast("long").alias("internacoes_total_validos_absoluto"),
            F.sum(F.when(valido, F.col("internacoes_total")).otherwise(0)).cast("long").alias("internacoes_total_validos_relativo"),
            F.sum(F.when(absoluto, F.col("internacoes_frio")).otherwise(0)).cast("long").alias("internacoes_frio_validos_absoluto"),
            F.sum(F.when(valido, F.col("internacoes_frio")).otherwise(0)).cast("long").alias("internacoes_frio_validos_relativo"),
            F.sum(F.when(absoluto, F.col("internacoes_quente")).otherwise(0)).cast("long").alias("internacoes_quente_validos_absoluto"),
            F.sum(F.when(valido, F.col("internacoes_quente")).otherwise(0)).cast("long").alias("internacoes_quente_validos_relativo"),
            F.sum(F.when(absoluto, F.col("dias_frios")).otherwise(0)).cast("long").alias("dias_frios_validos_absoluto"),
            F.sum(F.when(valido, F.col("dias_frios")).otherwise(0)).cast("long").alias("dias_frios_validos_relativo"),
            F.sum(F.when(absoluto, F.col("dias_quentes")).otherwise(0)).cast("long").alias("dias_quentes_validos_absoluto"),
            F.sum(F.when(valido, F.col("dias_quentes")).otherwise(0)).cast("long").alias("dias_quentes_validos_relativo"),

            F.sum("elegivel_amostra").cast("long").alias("municipios_elegiveis"),
            F.sum("contraste_relativo_valido").cast("long").alias("municipios_validos"),
            F.avg(F.when(valido, F.col("diferenca_relativa"))).alias("media_diferenca_relativa"),
            F.percentile_approx(F.when(valido, F.col("diferenca_relativa")), [0.25, 0.5, 0.75], 10000).alias("quartis"),
            F.min(F.when(valido, F.col("diferenca_relativa"))).alias("minimo"),
            F.max(F.when(valido, F.col("diferenca_relativa"))).alias("maximo"),
            F.percentile_approx(F.when(valido, F.col("media_frio_municipio")), 0.5, 10000).alias("mediana_media_frio"),
            F.percentile_approx(F.when(valido, F.col("media_quente_municipio")), 0.5, 10000).alias("mediana_media_quente"),
            F.percentile_approx(F.when(valido, F.col("diferenca_absoluta")), 0.5, 10000).alias("mediana_diferenca_absoluta"),
            F.sum(F.when(valido & (F.col("diferenca_relativa") > 0), 1).otherwise(0)).alias("qtd_positivos"),
            F.sum(F.when(valido & (F.col("diferenca_relativa") < 0), 1).otherwise(0)).alias("qtd_negativos"),
            F.sum(F.when(valido & (F.abs(F.col("diferenca_relativa")) <= LIMIAR_APROX_ZERO), 1).otherwise(0)).alias("qtd_aprox_zero"),
            F.sum("internacoes_total").cast("long").alias("internacoes_total"),
            F.sum("dias_frios").cast("long").alias("dias_frios_total"),
            F.sum("dias_quentes").cast("long").alias("dias_quentes_total"),
        )
        .withColumn("q1", F.col("quartis")[0])
        .withColumn("mediana_diferenca_relativa", F.col("quartis")[1])
        .withColumn("q3", F.col("quartis")[2])
        .drop("quartis")
        .withColumn("q1_diferenca_absoluta_elegiveis", F.col("quartis_absolutos")[0])
        .withColumn("mediana_diferenca_absoluta_elegiveis", F.col("quartis_absolutos")[1])
        .withColumn("q3_diferenca_absoluta_elegiveis", F.col("quartis_absolutos")[2])
        .withColumn("q1_diferenca_por_100", 100 * F.col("quartis_absolutos")[0])
        .withColumn("mediana_diferenca_por_100", 100 * F.col("quartis_absolutos")[1])
        .withColumn("q3_diferenca_por_100", 100 * F.col("quartis_absolutos")[2])
        .withColumn("direcao_diferenca", F.when(F.col("quartis_absolutos")[1].isNull(), "NAO_APLICAVEL")
                    .when(F.col("quartis_absolutos")[1] > 0, "FRIO")
                    .when(F.col("quartis_absolutos")[1] < 0, "QUENTE").otherwise("IGUAL"))
        .withColumn("media_frio_zero", F.col("mediana_media_frio_elegiveis") == 0)
        .withColumn("media_quente_zero", F.col("mediana_media_quente_elegiveis") == 0)
        .withColumn("ambas_medias_zero", F.col("media_frio_zero") & F.col("media_quente_zero"))
        .withColumn("percentual_frio_consistencia", F.when(n > 0, 100 * F.col("municipios_frio_consistencia") / n))
        .withColumn("percentual_quente_consistencia", F.when(n > 0, 100 * F.col("municipios_quente_consistencia") / n))
        .withColumn("percentual_pequena_consistencia", F.when(n > 0, 100 * F.col("municipios_pequena_consistencia") / n))
        .drop("quartis_absolutos")

        .withColumn("percentual_positivo", 100 * F.col("qtd_positivos") / n)
        .withColumn("percentual_negativo", 100 * F.col("qtd_negativos") / n)
        .withColumn("percentual_aproximadamente_zero", 100 * F.col("qtd_aprox_zero") / n)
        .withColumn("limiar_aproximadamente_zero", F.lit(LIMIAR_APROX_ZERO))
    )


def criar_contraste_municipio(
    fato_dia: DataFrame, dim_municipio: DataFrame
) -> DataFrame:
    municipios = dim_municipio.select(
        "codigo_municipio", "nome_municipio", "latitude_municipio", "longitude_municipio"
    ).dropDuplicates(["codigo_municipio"])
    return _contraste(fato_dia, ["codigo_municipio"]).join(
        municipios, "codigo_municipio", "left"
    )


def criar_temperatura_faixa(fato_dia: DataFrame) -> DataFrame:
    temperatura = F.col("temperatura_media_dia_c")
    com_faixa = (
        fato_dia.withColumn(
            "faixa_temperatura",
            F.when(temperatura < 15, "< 15 °C")
            .when(temperatura < 20, "15 a 19,9 °C")
            .when(temperatura < 25, "20 a 24,9 °C")
            .when(temperatura < 30, "25 a 29,9 °C")
            .when(temperatura.isNotNull(), ">= 30 °C"),
        )
        .withColumn(
            "ordem_faixa",
            F.when(temperatura < 15, 1)
            .when(temperatura < 20, 2)
            .when(temperatura < 25, 3)
            .when(temperatura < 30, 4)
            .when(temperatura.isNotNull(), 5),
        )
        .filter(F.col("faixa_temperatura").isNotNull())
    )
    municipal = com_faixa.groupBy(
        "codigo_municipio", "faixa_temperatura", "ordem_faixa"
    ).agg(
        F.sum("qtd_internacoes").cast("long").alias("internacoes_municipio"),
        F.count("*").cast("long").alias("dias_municipio"),
    ).withColumn(
        "media_municipal", F.col("internacoes_municipio") / F.col("dias_municipio")
    )
    estadual = com_faixa.groupBy("faixa_temperatura", "ordem_faixa").agg(
        F.sum("qtd_internacoes").cast("long").alias("internacoes_total"),
        F.count("*").cast("long").alias("municipio_dias"),
    ).withColumn(
        "media_agregada_municipio_dia", F.col("internacoes_total") / F.col("municipio_dias")
    )
    resumo = municipal.groupBy("faixa_temperatura", "ordem_faixa").agg(
        F.count("*").cast("long").alias("municipios_com_dados"),
        F.percentile_approx("media_municipal", [0.25, 0.5, 0.75], 10000).alias("quartis"),
    ).select(
        "faixa_temperatura", "ordem_faixa", "municipios_com_dados",
        F.col("quartis")[0].alias("q1_media_municipal"),
        F.col("quartis")[1].alias("mediana_media_municipal"),
        F.col("quartis")[2].alias("q3_media_municipal"),
    )
    return estadual.join(resumo, ["faixa_temperatura", "ordem_faixa"])


def criar_temporal(fato_dia: DataFrame) -> DataFrame:
    return _resumo(_contraste(fato_dia, ["ano", "codigo_municipio"]), ["ano"])


def criar_sazonalidade_mes(fato_dia: DataFrame) -> DataFrame:
    base = fato_dia.withColumn("mes", F.month("data"))
    municipal = base.groupBy("codigo_municipio", "mes").agg(
        F.sum("qtd_internacoes").alias("internacoes"),
        F.count("*").alias("municipio_dias"),
        F.avg("temperatura_media_dia_c").alias("temperatura_media"),
    ).withColumn("media_internacoes_municipio_dia", F.col("internacoes") / F.col("municipio_dias"))
    agregado = base.groupBy("mes").agg(
        F.sum("qtd_internacoes").cast("long").alias("internacoes_total"),
        F.count("*").cast("long").alias("municipio_dias"),
        F.avg("temperatura_media_dia_c").alias("temperatura_media_agregada"),
    ).withColumn("media_internacoes_agregada", F.col("internacoes_total") / F.col("municipio_dias"))
    resumo = municipal.groupBy("mes").agg(
        F.percentile_approx("media_internacoes_municipio_dia", 0.5, 10000).alias("mediana_media_internacoes_municipal"),
        F.percentile_approx("temperatura_media", 0.5, 10000).alias("mediana_temperatura_municipal"),
    )
    nomes = F.array(*[F.lit(x) for x in (
        "Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"
    )])
    return agregado.join(resumo, "mes").withColumn(
        "mes_abreviado", F.element_at(nomes, F.col("mes"))
    )


def _dimensao_alvos_cid(dim_cid: DataFrame) -> DataFrame:
    capitulos = dim_cid.select(
        F.concat(F.lit("CAPITULO_"), F.col("codigo_capitulo")).alias("chave_alvo"),
        F.lit("Capítulo").alias("nivel_cid"), F.lit(1).alias("ordem_nivel_cid"),
        "codigo_capitulo", "descricao_capitulo",
        F.lit(None).cast("string").alias("codigo_grupo"),
        F.lit(None).cast("string").alias("descricao_grupo"),
        F.lit(None).cast("string").alias("codigo_categoria"),
        F.lit(None).cast("string").alias("descricao_categoria"),
        F.concat_ws(" — ", "codigo_capitulo", "descricao_capitulo").alias("rotulo_alvo"),
    ).dropDuplicates(["chave_alvo"])
    grupos = dim_cid.select(
        F.concat(F.lit("GRUPO_"), F.col("codigo_grupo")).alias("chave_alvo"),
        F.lit("Grupo").alias("nivel_cid"), F.lit(2).alias("ordem_nivel_cid"),
        "codigo_capitulo", "descricao_capitulo", "codigo_grupo", "descricao_grupo",
        F.lit(None).cast("string").alias("codigo_categoria"),
        F.lit(None).cast("string").alias("descricao_categoria"),
        F.concat_ws(" — ", "codigo_grupo", "descricao_grupo").alias("rotulo_alvo"),
    ).dropDuplicates(["chave_alvo"])
    categorias = dim_cid.select(
        F.concat(F.lit("CATEGORIA_"), F.col("codigo_categoria")).alias("chave_alvo"),
        F.lit("Categoria").alias("nivel_cid"), F.lit(3).alias("ordem_nivel_cid"),
        "codigo_capitulo", "descricao_capitulo", "codigo_grupo", "descricao_grupo",
        "codigo_categoria", "descricao_categoria",
        F.concat_ws(" — ", "codigo_categoria", "descricao_categoria").alias("rotulo_alvo"),
    ).dropDuplicates(["chave_alvo"])
    return _unir([capitulos, grupos, categorias])


def _numeradores_alvos(eventos: DataFrame, demografia: bool = False) -> DataFrame:
    grupos_demo = ["faixa_etaria", "sexo", "sexo_descricao"] if demografia else []
    condicoes = [
        ("CAPITULO_", "codigo_capitulo"),
        ("GRUPO_", "codigo_grupo"),
        ("CATEGORIA_", "codigo_categoria"),
    ]
    frio = F.col("temperatura_media_dia_c") < 20
    quente = F.col("temperatura_media_dia_c") >= 25
    partes = []
    if demografia:
        partes.append(
            eventos.groupBy("codigo_municipio", *grupos_demo).agg(
                F.sum(F.when(frio, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_frio"),
                F.sum(F.when(quente, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_quente"),
                F.sum("qtd_internacoes").alias("internacoes_total"),
            ).withColumn("chave_alvo", F.lit("TOTAL"))
        )
    for prefixo, coluna in condicoes:
        partes.append(
            eventos.groupBy("codigo_municipio", coluna, *grupos_demo).agg(
                F.sum(F.when(frio, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_frio"),
                F.sum(F.when(quente, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_quente"),
                F.sum("qtd_internacoes").alias("internacoes_total"),
            ).withColumn("chave_alvo", F.concat(F.lit(prefixo), F.col(coluna))).drop(coluna)
        )
    return _unir(partes)


def _exposicao_municipal(fato_dia: DataFrame) -> DataFrame:
    return _contraste(
        fato_dia.withColumn("_zero", F.lit(0)), ["codigo_municipio"], internacoes="_zero"
    ).select("codigo_municipio", "dias_frios", "dias_quentes")


def criar_cid_detalhe(fato_dia: DataFrame, eventos: DataFrame, dim_cid: DataFrame, numeradores: DataFrame | None = None) -> DataFrame:
    dimensao = _dimensao_alvos_cid(dim_cid)
    numeradores = _numeradores_alvos(eventos) if numeradores is None else numeradores
    detalhe = (
        _exposicao_municipal(fato_dia).crossJoin(dimensao)
        .join(numeradores, ["codigo_municipio", "chave_alvo"], "left")
        .fillna(0, subset=["internacoes_frio", "internacoes_quente", "internacoes_total"])
    )
    detalhe = _adicionar_metricas(detalhe).withColumn("municipio_dias_total", F.lit(0).cast("long"))
    return detalhe


def criar_cid_resumo(fato_dia: DataFrame, eventos: DataFrame, dim_cid: DataFrame) -> DataFrame:
    detalhe = criar_cid_detalhe(fato_dia, eventos, dim_cid)
    campos = [
        "chave_alvo", "nivel_cid", "ordem_nivel_cid", "codigo_capitulo",
        "descricao_capitulo", "codigo_grupo", "descricao_grupo",
        "codigo_categoria", "descricao_categoria", "rotulo_alvo",
    ]
    return _resumo(detalhe, campos)


def criar_demografia_detalhe(
    spark: SparkSession, fato_dia: DataFrame, eventos: DataFrame, dim_cid: DataFrame, numeradores: DataFrame | None = None
) -> DataFrame:
    alvos = _dimensao_alvos_cid(dim_cid)
    total = spark.createDataFrame(
        [("TOTAL", "Total", 0, None, None, None, None, None, None, "Todas as internações")],
        "chave_alvo string, nivel_cid string, ordem_nivel_cid int, codigo_capitulo string, descricao_capitulo string, codigo_grupo string, descricao_grupo string, codigo_categoria string, descricao_categoria string, rotulo_alvo string",
    )
    alvos = total.unionByName(alvos)
    estratos = spark.createDataFrame(
        [(f, of, s, ds, os) for f, of in FAIXAS_ETARIAS for s, ds, os in SEXOS],
        "faixa_etaria string, ordem_faixa_etaria int, sexo string, sexo_descricao string, ordem_sexo int",
    )
    numeradores = numeradores if numeradores is not None else _numeradores_alvos(
        eventos.filter(F.col("faixa_etaria").isin([x[0] for x in FAIXAS_ETARIAS]))
        .filter(F.col("sexo").isin([x[0] for x in SEXOS])),
        demografia=True,
    )
    detalhe = (
        _exposicao_municipal(fato_dia).crossJoin(alvos).crossJoin(estratos)
        .join(
            numeradores,
            ["codigo_municipio", "chave_alvo", "faixa_etaria", "sexo", "sexo_descricao"],
            "left",
        )
        .fillna(0, subset=["internacoes_frio", "internacoes_quente", "internacoes_total"])
    )
    detalhe = _adicionar_metricas(detalhe).withColumn("municipio_dias_total", F.lit(0).cast("long"))
    return detalhe


def criar_demografia_resumo(
    spark: SparkSession, fato_dia: DataFrame, eventos: DataFrame, dim_cid: DataFrame
) -> DataFrame:
    detalhe = criar_demografia_detalhe(spark, fato_dia, eventos, dim_cid)
    campos = [
        "chave_alvo", "nivel_cid", "ordem_nivel_cid", "codigo_capitulo",
        "descricao_capitulo", "codigo_grupo", "descricao_grupo",
        "codigo_categoria", "descricao_categoria", "rotulo_alvo",
        "faixa_etaria", "ordem_faixa_etaria", "sexo", "sexo_descricao", "ordem_sexo",
    ]
    return _resumo(detalhe, campos)



def criar_estratos_detalhe(spark: SparkSession, fato_dia: DataFrame, eventos: DataFrame,
                          dim_cid: DataFrame, numeradores: DataFrame | None = None) -> DataFrame:
    """Marginais por idade OU sexo; nunca agrega medianas do estrato conjunto."""
    alvos = _dimensao_alvos_cid(dim_cid)
    total = spark.createDataFrame(
        [("TOTAL", "Total", 0, None, None, None, None, None, None, "Todas as internações")],
        alvos.schema,
    )
    alvos = total.unionByName(alvos)
    raw = _numeradores_alvos(eventos, demografia=True) if numeradores is None else numeradores
    exposicao = _exposicao_municipal(fato_dia)
    partes = []
    for nivel, chaves, estratos in [
        ("IDADE", ["faixa_etaria"], spark.createDataFrame(
            [(f,of,"TODOS","Todos os sexos",0) for f,of in FAIXAS_ETARIAS],
            "faixa_etaria string, ordem_faixa_etaria int, sexo string, sexo_descricao string, ordem_sexo int")),
        ("SEXO", ["sexo","sexo_descricao"], spark.createDataFrame(
            [("Todas as idades",0,sexo,desc,ordem) for sexo,desc,ordem in SEXOS],
            "faixa_etaria string, ordem_faixa_etaria int, sexo string, sexo_descricao string, ordem_sexo int")),
    ]:
        # Os numeradores são contagens municipais, não resumos/medianas.
        nums = raw.groupBy("codigo_municipio","chave_alvo",*chaves).agg(
            *[F.sum(c).cast("long").alias(c) for c in ("internacoes_frio","internacoes_quente","internacoes_total")])
        detalhe = (exposicao.crossJoin(alvos).crossJoin(estratos)
                   .join(nums,["codigo_municipio","chave_alvo",*chaves],"left")
                   .fillna(0,subset=["internacoes_frio","internacoes_quente","internacoes_total"]))
        partes.append(_adicionar_metricas(detalhe).withColumn("nivel_demografico",F.lit(nivel)))
    return _unir(partes)


def criar_estratos_resumo(spark: SparkSession, fato_dia: DataFrame, eventos: DataFrame,
                          dim_cid: DataFrame) -> DataFrame:
    detalhe = criar_estratos_detalhe(spark,fato_dia,eventos,dim_cid)
    return _resumo(detalhe, ["chave_alvo","nivel_cid","ordem_nivel_cid","codigo_capitulo",
        "descricao_capitulo","codigo_grupo","descricao_grupo","codigo_categoria","descricao_categoria",
        "rotulo_alvo","nivel_demografico","faixa_etaria","ordem_faixa_etaria","sexo","sexo_descricao","ordem_sexo"])


def criar_lag_resumo(
    spark: SparkSession, fato_dia: DataFrame, eventos: DataFrame
) -> DataFrame:
    alvos = [
        ("TOTAL", "Todas as internações", 1),
        ("CAPITULO_10", "Capítulo X — Doenças do aparelho respiratório", 2),
        ("GRUPO_J09-J18", "J09-J18 — Influenza e pneumonia", 3),
        ("GRUPO_J20-J22", "J20-J22 — Outras infecções agudas das vias aéreas inferiores", 4),
    ]
    dim_alvos = spark.createDataFrame(alvos, "chave_alvo string, alvo_analitico string, ordem_alvo int")
    resultados = []
    for rotulo, ordem, temperatura in DEFASAGENS:
        total = _contraste(fato_dia, ["codigo_municipio"], temperatura=temperatura)
        total = total.withColumn("chave_alvo", F.lit("TOTAL"))
        exposicao = total.select("codigo_municipio", "dias_frios", "dias_quentes")
        frio = F.col(temperatura) < 20
        quente = F.col(temperatura) >= 25
        filtrados = (
            eventos.filter(F.col("codigo_capitulo") == "10")
            .withColumn("chave_alvo", F.lit("CAPITULO_10"))
            .unionByName(
                eventos.filter(F.col("codigo_grupo") == "J09-J18").withColumn("chave_alvo", F.lit("GRUPO_J09-J18")),
                allowMissingColumns=True,
            )
            .unionByName(
                eventos.filter(F.col("codigo_grupo") == "J20-J22").withColumn("chave_alvo", F.lit("GRUPO_J20-J22")),
                allowMissingColumns=True,
            )
        )
        nums = filtrados.groupBy("codigo_municipio", "chave_alvo").agg(
            F.sum(F.when(frio, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_frio"),
            F.sum(F.when(quente, F.col("qtd_internacoes")).otherwise(0)).alias("internacoes_quente"),
            F.sum("qtd_internacoes").alias("internacoes_total"),
        )
        resp = (
            exposicao.crossJoin(dim_alvos.filter(F.col("chave_alvo") != "TOTAL").select("chave_alvo"))
            .join(nums, ["codigo_municipio", "chave_alvo"], "left")
            .fillna(0, subset=["internacoes_frio", "internacoes_quente", "internacoes_total"])
        )
        resp = _adicionar_metricas(resp).withColumn("municipio_dias_total", F.lit(0).cast("long"))
        detalhe = total.unionByName(resp.select(total.columns))
        resultados.append(
            _resumo(detalhe, ["chave_alvo"])
            .join(dim_alvos, "chave_alvo")
            .withColumn("defasagem", F.lit(rotulo))
            .withColumn("ordem_defasagem", F.lit(ordem))
        )
    return _unir(resultados)


def gerar_saidas(spark, fato_dia, fato_cid, dim_cid, dim_municipio):
    """Entrada oficial B; auxiliares absolutos acima preservam APIs históricas."""
    from src.consumption.promocao_metodologia import gerar_saidas_oficiais
    return gerar_saidas_oficiais(spark,fato_dia,fato_cid,dim_cid,dim_municipio)[0]


def publicar(nome: str, df: DataFrame) -> None:
    destino = f"{SAIDA}/{nome}"
    with destino_parquet_atomico(destino) as staging:
        df.coalesce(1).write.mode("overwrite").option("compression", "snappy").parquet(staging)
    print(f"Gravado: {destino}")


def main() -> int:
    from src.consumption.promocao_metodologia import main as promover
    return promover()


if __name__ == "__main__":
    raise SystemExit(main())
