"""Data Quality da camada de consumo para Power BI."""

from __future__ import annotations

import sys
from functools import reduce
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from src.common.validators import (
    QualityReport,
    imprimir_relatorio,
    registrar_cobertura,
    validar_chave_unica,
    validar_colunas_obrigatorias,
    validar_condicao,
    validar_nulos,
    validar_periodo,
    validar_valores_aceitos,
)
from src.consumption.transformations import (
    AUDITORIA_PAINEL,
    DATA_FIM,
    DATA_INICIO,
    FAIXAS_ETARIAS,
    METEOROLOGIA_ADICIONAL,
    METEOROLOGIA_GOLD,
    METRICAS_SAUDE,
    inconsistencias_metadados_estacao,
)
from src.common.study_period import (
    ANALYSIS_END_DATE,
    ANALYSIS_YEARS,
    EXPECTED_ANALYSIS_DAYS,
    INMET_YEARS,
)


BASE_PATH = "/home/jovyan/work/data"
CONSUMPTION = f"{BASE_PATH}/consumption"
GOLD_FATO = f"{BASE_PATH}/gold/fato_internacao_meteorologia"
SILVER_INMET = f"{BASE_PATH}/silver/inmet"
SILVER_INMET_DIARIO = f"{BASE_PATH}/silver/inmet_diario"

COLUNAS_FATO_DIA = [
    "municipio_dia_id", "data", "codigo_municipio", "codigo_estacao",
    *METRICAS_SAUDE,
    *[c for c in METEOROLOGIA_GOLD if c != "codigo_estacao"],
    *METEOROLOGIA_ADICIONAL,
    "temperatura_media_lag_1d", "temperatura_media_lag_3d",
    "temperatura_media_lag_7d", *AUDITORIA_PAINEL, "ano",
]
COLUNAS_FATO_CID = [
    "municipio_dia_id", "data", "codigo_municipio", "codigo_categoria",
    "sexo", "sexo_descricao", "faixa_etaria", *METRICAS_SAUDE, "ano",
]


def ler_inmet_horario(spark):
    partes = [
        spark.read.option("mergeSchema", "true").parquet(f"{SILVER_INMET}/{ano}")
        for ano in INMET_YEARS
    ]
    df = partes[0]
    for parte in partes[1:]:
        df = df.unionByName(parte, allowMissingColumns=True)
    return df


def totais_saude(df):
    return df.agg(*[F.sum(c).alias(c) for c in METRICAS_SAUDE]).first().asDict()


def totais_gold(df_gold):
    return (
        df_gold.filter(F.col("data_internacao").between(DATA_INICIO, DATA_FIM))
        .agg(
            F.count("*").cast("long").alias("qtd_internacoes"),
            F.sum(F.col("obito").cast("long")).alias("qtd_obitos"),
            F.sum(F.col("dias_permanencia").cast("long")).alias("dias_permanencia_total"),
            F.sum(F.col("dias_uti").cast("long")).alias("dias_uti_total"),
            F.sum(F.col("valor_total").cast("decimal(20,2)"))
            .cast("decimal(20,2)").alias("valor_total_internacoes"),
            F.sum(F.col("valor_uti").cast("decimal(20,2)"))
            .cast("decimal(20,2)").alias("valor_total_uti"),
        ).first().asDict()
    )


def comparar_totais(report, nome, encontrado, esperado):
    divergencias = {
        campo: {"encontrado": str(encontrado.get(campo)), "esperado": str(esperado.get(campo))}
        for campo in METRICAS_SAUDE
        if encontrado.get(campo) != esperado.get(campo)
    }
    report.add(nome, not divergencias, "totais idênticos" if not divergencias else str(divergencias))


def validar_lags_exatos(df_fato, df_inmet_diario, report):
    lookup_municipio = df_fato.select(
        F.col("codigo_municipio").alias("_municipio_lag"),
        F.to_date("data").alias("_data_municipio_lag"),
        F.col("temperatura_media_dia_c").alias("_temperatura_municipio_lag"),
    )
    for dias in (1, 3, 7):
        nome = f"temperatura_media_lag_{dias}d"
        data_alvo = F.date_sub(F.col("data"), dias)
        lookup_tecnico = df_inmet_diario.select(
            F.col("codigo_estacao").alias("_estacao_tecnica"),
            F.to_date("data").alias("_data_tecnica"),
            F.col("temperatura_media_dia_c").alias("_temperatura_tecnica"),
        )
        invalidos = (
            df_fato.join(
                lookup_municipio,
                (F.col("codigo_municipio") == F.col("_municipio_lag"))
                & (F.col("_data_municipio_lag") == data_alvo),
                "left",
            )
            .join(
                lookup_tecnico,
                (F.col("codigo_estacao") == F.col("_estacao_tecnica"))
                & (F.col("_data_tecnica") == data_alvo),
                "left",
            )
            .withColumn(
                "_esperada",
                F.when(
                    data_alvo < F.lit(DATA_INICIO).cast("date"),
                    F.col("_temperatura_tecnica"),
                ).otherwise(F.col("_temperatura_municipio_lag")),
            )
            .filter(~F.col(nome).eqNullSafe(F.col("_esperada")))
            .count()
        )
        report.add(
            f"Lag exato de {dias} dia(s)", invalidos == 0,
            f"divergências em data - {dias} dia(s)={invalidos}", metrica=invalidos,
        )


def validar_consumption(
    fato_dia, fato_cid, dim_municipio, dim_cid, dim_tempo, dim_estacao,
    gold, inmet_diario, inmet_horario,
) -> QualityReport:
    report = QualityReport("CAMADA CONSUMPTION")
    schemas_ok = validar_colunas_obrigatorias(fato_dia, COLUNAS_FATO_DIA, report)
    schemas_ok &= validar_colunas_obrigatorias(fato_cid, COLUNAS_FATO_CID, report)
    if not schemas_ok:
        return report

    validar_chave_unica(fato_dia, ["municipio_dia_id"], report)
    validar_chave_unica(
        fato_cid,
        ["data", "codigo_municipio", "codigo_categoria", "sexo", "faixa_etaria"],
        report,
    )
    validar_nulos(
        fato_dia, ["municipio_dia_id", "data", "codigo_municipio", "codigo_estacao"], report
    )
    validar_nulos(
        fato_cid,
        ["municipio_dia_id", "data", "codigo_municipio", "codigo_categoria", "sexo", "faixa_etaria"],
        report,
    )
    validar_condicao(fato_dia, F.col("qtd_internacoes") < 0, "Internações não negativas na fato diária", report)
    validar_condicao(fato_cid, F.col("qtd_internacoes") <= 0, "Internações positivas na fato CID", report)
    for df, nome in ((fato_dia, "fato diária"), (fato_cid, "fato CID")):
        validar_condicao(
            df,
            (F.col("qtd_obitos") < 0) | (F.col("qtd_obitos") > F.col("qtd_internacoes")),
            f"Óbitos entre zero e internações — {nome}", report,
        )

    esperado = totais_gold(gold)
    totais_dia = totais_saude(fato_dia)
    totais_cid = totais_saude(fato_cid)
    comparar_totais(report, "Totais da fato diária reproduzem a Gold", totais_dia, esperado)
    comparar_totais(report, "Totais da fato CID reproduzem a Gold", totais_cid, esperado)
    comparar_totais(report, "Totais iguais entre as duas fatos", totais_dia, totais_cid)

    gold_ano = {
        int(r["ano"]): r["internacoes"]
        for r in (
            gold.filter(F.col("data_internacao").between(DATA_INICIO, DATA_FIM))
            .groupBy(F.year("data_internacao").alias("ano"))
            .agg(F.count("*").alias("internacoes"))
            .orderBy("ano").collect()
        )
    }
    dia_ano = {
        int(r["ano"]): r["internacoes"]
        for r in (
            fato_dia.groupBy("ano")
            .agg(F.sum("qtd_internacoes").alias("internacoes"))
            .orderBy("ano").collect()
        )
    }
    cid_ano = {
        int(r["ano"]): r["internacoes"]
        for r in (
            fato_cid.groupBy("ano")
            .agg(F.sum("qtd_internacoes").alias("internacoes"))
            .orderBy("ano").collect()
        )
    }
    # O painel diário contém todos os anos do calendário, inclusive anos sem
    # eventos. A Gold e a fato CID, por construção, não materializam grupos de
    # ano cujo total é zero. Normalize as três séries no mesmo domínio antes da
    # comparação para que ausência de grupo e total zero sejam equivalentes.
    anos_reconciliacao = set(gold_ano) | set(dia_ano) | set(cid_ano)
    gold_ano = {ano: gold_ano.get(ano, 0) for ano in sorted(anos_reconciliacao)}
    dia_ano = {ano: dia_ano.get(ano, 0) for ano in sorted(anos_reconciliacao)}
    cid_ano = {ano: cid_ano.get(ano, 0) for ano in sorted(anos_reconciliacao)}
    report.add(
        "Reconciliação anual Gold x fatos de consumo",
        gold_ano == dia_ano == cid_ano,
        f"Gold={gold_ano}; dia={dia_ano}; CID={cid_ano}",
        metrica=str(dia_ano),
    )

    gold_municipio = (
        gold.filter(F.col("data_internacao").between(DATA_INICIO, DATA_FIM))
        .groupBy(F.col("CD_MUN").alias("codigo_municipio"))
        .agg(F.count("*").alias("gold"))
    )
    dia_municipio = fato_dia.groupBy("codigo_municipio").agg(
        F.sum("qtd_internacoes").alias("dia")
    )
    cid_municipio = fato_cid.groupBy("codigo_municipio").agg(
        F.sum("qtd_internacoes").alias("cid")
    )
    divergencias_municipio = (
        gold_municipio.join(dia_municipio, "codigo_municipio", "full")
        .join(cid_municipio, "codigo_municipio", "full")
        .filter(
            ~F.col("gold").eqNullSafe(F.col("dia"))
            | ~F.col("gold").eqNullSafe(F.col("cid"))
        )
        .count()
    )
    report.add(
        "Reconciliação municipal Gold x fatos de consumo",
        divergencias_municipio == 0,
        f"municípios divergentes={divergencias_municipio}",
        metrica=divergencias_municipio,
    )

    validar_valores_aceitos(fato_cid, "faixa_etaria", FAIXAS_ETARIAS, report)
    validar_valores_aceitos(fato_cid, "sexo", ["1", "3"], report)
    validar_condicao(
        fato_cid,
        ~(
            ((F.col("sexo") == "1") & (F.col("sexo_descricao") == "Masculino"))
            | ((F.col("sexo") == "3") & (F.col("sexo_descricao") == "Feminino"))
        ),
        "Rótulo de sexo coerente com o código SIH",
        report,
    )
    for fato, chave, dim, nome in (
        (fato_dia, "codigo_municipio", dim_municipio, "Municípios da fato diária"),
        (fato_cid, "codigo_municipio", dim_municipio, "Municípios da fato CID"),
        (fato_cid, "codigo_categoria", dim_cid, "Categorias da fato CID"),
        (fato_dia, "data", dim_tempo, "Datas da fato diária"),
        (fato_cid, "data", dim_tempo, "Datas da fato CID"),
        (fato_dia, "codigo_estacao", dim_estacao, "Estações da fato diária"),
    ):
        ausentes = fato.select(chave).distinct().join(dim.select(chave).distinct(), chave, "left_anti").count()
        report.add(nome, ausentes == 0, f"chaves ausentes na dimensão={ausentes}", metrica=ausentes)

    for dim, chave, nome in (
        (dim_municipio, ["codigo_municipio"], "dim_municipio"),
        (dim_cid, ["codigo_categoria"], "dim_cid"),
        (dim_tempo, ["data"], "dim_tempo"),
        (dim_estacao, ["codigo_estacao"], "dim_estacao"),
    ):
        validar_chave_unica(dim, chave, report)

    validar_periodo(dim_tempo, "data", DATA_INICIO, DATA_FIM, report)
    total_tempo = dim_tempo.count()
    report.add(
        "Calendário contínuo",
        total_tempo == EXPECTED_ANALYSIS_DAYS,
        f"dias={total_tempo}, esperado={EXPECTED_ANALYSIS_DAYS}",
    )
    municipios = dim_municipio.select("codigo_municipio").distinct().count()
    esperado_painel = municipios * EXPECTED_ANALYSIS_DAYS
    total_painel = fato_dia.count()
    report.add(
        "Painel completo município x dia",
        total_painel == esperado_painel,
        f"linhas={total_painel:,}, esperado={esperado_painel:,}",
        metrica=total_painel,
    )
    zeros_inconsistentes = fato_dia.filter(
        F.col("dia_sem_internacao")
        != (F.col("qtd_internacoes") == 0).cast("int")
    ).count()
    report.add(
        "Zero de internações separado de meteorologia ausente",
        zeros_inconsistentes == 0,
        f"linhas inconsistentes={zeros_inconsistentes}",
        metrica=zeros_inconsistentes,
    )
    meteo_preenchida_sem_registro = fato_dia.filter(
        (F.col("meteorologia_disponivel") == 0)
        & reduce(
            lambda a, b: a | b,
            [
                F.col(c).isNotNull()
                for c in (
                    "temperatura_media_dia_c",
                    "umidade_media_dia_pct",
                    "precipitacao_dia_mm",
                    "vento_medio_dia_ms",
                )
            ],
        )
    ).count()
    report.add(
        "Meteorologia ausente permanece NULL",
        meteo_preenchida_sem_registro == 0,
        f"linhas preenchidas sem registro estação-dia={meteo_preenchida_sem_registro}",
        metrica=meteo_preenchida_sem_registro,
    )

    colunas_meteo_cid = sorted(set(fato_cid.columns) & set(METEOROLOGIA_GOLD + METEOROLOGIA_ADICIONAL))
    report.add(
        "Meteorologia não expandida pela fato CID", not colunas_meteo_cid,
        "nenhuma métrica meteorológica" if not colunas_meteo_cid else f"colunas={colunas_meteo_cid}",
    )
    validar_chave_unica(fato_dia, ["data", "codigo_municipio"], report)
    for coluna in [c for c in fato_dia.columns if c.startswith("qtd_obs_")] + ["qtd_observacoes_inmet"]:
        validar_condicao(
            fato_dia, F.col(coluna).isNotNull() & ((F.col(coluna) < 0) | (F.col(coluna) > 24)),
            f"Contador meteorológico válido — {coluna}", report,
        )

    origem_contadores = (
        gold.filter(F.col("data_internacao").between(DATA_INICIO, DATA_FIM))
        .select(
            F.to_date("data_internacao").alias("data"),
            F.col("CD_MUN").alias("codigo_municipio"),
            "qtd_observacoes_inmet", "qtd_obs_temperatura", "qtd_obs_umidade",
            "qtd_obs_precipitacao", "qtd_obs_vento",
        ).distinct()
    )
    contador_cols = [
        "qtd_observacoes_inmet", "qtd_obs_temperatura", "qtd_obs_umidade",
        "qtd_obs_precipitacao", "qtd_obs_vento",
    ]
    divergentes = (
        fato_dia.alias("f").join(origem_contadores.alias("o"), ["data", "codigo_municipio"])
        .filter(~reduce(lambda a, b: a & b, [F.col(f"f.{c}").eqNullSafe(F.col(f"o.{c}")) for c in contador_cols]))
        .count()
    )
    report.add("Contadores da Gold preservados", divergentes == 0, f"chaves divergentes={divergentes}")

    validar_lags_exatos(fato_dia, inmet_diario, report)
    registrar_cobertura(fato_dia, METEOROLOGIA_ADICIONAL, report)
    cobertura_ano = (
        fato_dia.groupBy("ano")
        .agg(
            F.count("*").alias("municipio_dias"),
            F.count("temperatura_media_dia_c").alias("com_temperatura"),
            F.avg("cobertura_temperatura_estacao_ano_pct")
            .alias("cobertura_media_estacao_pct"),
        )
        .withColumn(
            "cobertura_temperatura_pct",
            100 * F.col("com_temperatura") / F.col("municipio_dias"),
        )
        .orderBy("ano")
        .collect()
    )
    for linha in cobertura_ano:
        cobertura = float(linha["cobertura_temperatura_pct"] or 0.0)
        report.add(
            f"Cobertura de temperatura {linha['ano']}",
            cobertura >= 80.0,
            f"{cobertura:.2f}% dos município-dias; "
            f"cobertura média da estação="
            f"{float(linha['cobertura_media_estacao_pct'] or 0.0):.2f}%",
            "WARNING",
            cobertura,
        )

    cobertura_municipio = (
        fato_dia.groupBy("codigo_municipio")
        .agg(
            F.count("*").alias("municipio_dias"),
            F.count("temperatura_media_dia_c").alias("com_temperatura"),
        )
        .withColumn(
            "cobertura_pct",
            100 * F.col("com_temperatura") / F.col("municipio_dias"),
        )
        .cache()
    )
    resumo_municipio = cobertura_municipio.agg(
        F.min("cobertura_pct").alias("minima"),
        F.avg("cobertura_pct").alias("media"),
        F.max("cobertura_pct").alias("maxima"),
    ).first()
    municipios_baixa_cobertura = cobertura_municipio.filter(
        F.col("cobertura_pct") < 80
    ).count()
    report.add(
        "Cobertura de temperatura por município",
        municipios_baixa_cobertura == 0,
        f"municípios abaixo de 80%={municipios_baixa_cobertura}; "
        f"mínima={float(resumo_municipio['minima'] or 0):.2f}%; "
        f"média={float(resumo_municipio['media'] or 0):.2f}%; "
        f"máxima={float(resumo_municipio['maxima'] or 0):.2f}%",
        "WARNING",
        municipios_baixa_cobertura,
    )
    cobertura_municipio.unpersist()

    revisoes = inconsistencias_metadados_estacao(inmet_horario).count()
    report.add(
        "Consistência histórica do cadastro de estações",
        revisoes == 0,
        f"estações com metadados revisados entre anos={revisoes}; "
        f"dimensão usa {ANALYSIS_END_DATE.year}",
        "WARNING",
        revisoes,
    )
    return report


def main():
    spark = SparkSession.builder.appName("DQ-Consumption-PowerBI").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        ler = lambda nome: spark.read.option("mergeSchema", "true").parquet(f"{CONSUMPTION}/{nome}")
        fato_dia = ler("fato_municipio_dia").cache()
        fato_cid = ler("fato_municipio_dia_cid").cache()
        gold = spark.read.option("mergeSchema", "true").parquet(GOLD_FATO).cache()
        inmet_diario = spark.read.option("mergeSchema", "true").parquet(SILVER_INMET_DIARIO).cache()
        inmet_horario = ler_inmet_horario(spark).cache()
        report = validar_consumption(
            fato_dia, fato_cid, ler("dim_municipio"), ler("dim_cid"),
            ler("dim_tempo"), ler("dim_estacao"), gold, inmet_diario, inmet_horario,
        )
        for nome in ("fato_municipio_dia", "fato_municipio_dia_cid"):
            raiz = Path(CONSUMPTION) / nome
            encontradas = sorted(
                int(p.name.split("=", 1)[1])
                for p in raiz.glob("ano=*")
                if p.is_dir() and p.name.split("=", 1)[1].isdigit()
            )
            report.add(
                f"Partições anuais físicas — {nome}",
                encontradas == list(ANALYSIS_YEARS),
                f"encontradas={encontradas}; esperadas={list(ANALYSIS_YEARS)}",
                metrica=str(encontradas),
            )
        imprimir_relatorio(report)
        report.exigir_aprovacao()
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
