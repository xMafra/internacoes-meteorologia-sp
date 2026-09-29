"""Transformações testáveis da camada de consumo.

As funções deste módulo não leem nem gravam arquivos. O script ``build.py``
fornece os DataFrames de origem e persiste os resultados.
"""

from __future__ import annotations

from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


DATA_INICIO = "2023-01-01"
DATA_FIM = "2025-12-31"
FAIXAS_ETARIAS = ["0-17", "18-39", "40-59", "60-79", "80+", "Ignorada"]
METRICAS_SAUDE = [
    "qtd_internacoes",
    "qtd_obitos",
    "dias_permanencia_total",
    "dias_uti_total",
    "valor_total_internacoes",
    "valor_total_uti",
]

METEOROLOGIA_GOLD = [
    "codigo_estacao",
    "temperatura_media_dia_c",
    "temperatura_minima_dia_c",
    "temperatura_maxima_dia_c",
    "umidade_media_dia_pct",
    "umidade_minima_dia_pct",
    "umidade_maxima_dia_pct",
    "precipitacao_dia_mm",
    "vento_medio_dia_ms",
    "vento_maximo_dia_ms",
    "qtd_observacoes_inmet",
    "qtd_obs_temperatura",
    "qtd_obs_umidade",
    "qtd_obs_precipitacao",
    "qtd_obs_vento",
    "meteorologia_disponivel",
    "motivo_sem_meteorologia",
]

METEOROLOGIA_ADICIONAL = [
    "radiacao_global_dia_kj_m2",
    "qtd_obs_radiacao",
    "pressao_media_dia_mb",
    "pressao_minima_dia_mb",
    "pressao_maxima_dia_mb",
    "qtd_obs_pressao",
    "orvalho_medio_dia_c",
    "orvalho_minimo_dia_c",
    "orvalho_maximo_dia_c",
    "qtd_obs_orvalho",
    "rajada_maxima_dia_ms",
    "qtd_obs_rajada",
]


def municipio_dia_id(data="data", municipio="codigo_municipio"):
    """Cria a chave estável AAAAMMDD_codigoIBGE."""
    return F.concat(F.date_format(F.col(data), "yyyyMMdd"), F.lit("_"), F.col(municipio))


def faixa_etaria(cod_idade="COD_IDADE", idade="IDADE"):
    """Interpreta a unidade etária do SIH e devolve a faixa analítica."""
    codigo = F.trim(F.col(cod_idade).cast("string"))
    valor = F.col(idade).cast("int")
    idade_anos = (
        F.when(codigo.isin("2", "3"), F.lit(0))
        .when(codigo == "4", valor)
        .when(codigo == "5", valor + F.lit(100))
    )
    return (
        F.when(codigo == "0", F.lit("Ignorada"))
        .when(idade_anos.isNull(), F.lit("Ignorada"))
        .when(idade_anos <= 17, F.lit("0-17"))
        .when(idade_anos <= 39, F.lit("18-39"))
        .when(idade_anos <= 59, F.lit("40-59"))
        .when(idade_anos <= 79, F.lit("60-79"))
        .otherwise(F.lit("80+"))
    )


def expressoes_saude(prefixo: str | None = None):
    def campo(nome):
        return F.col(f"{prefixo}.{nome}" if prefixo else nome)

    return [
        F.count(F.lit(1)).cast("long").alias("qtd_internacoes"),
        F.sum(campo("obito").cast("long")).alias("qtd_obitos"),
        F.sum(campo("dias_permanencia").cast("long")).alias("dias_permanencia_total"),
        F.sum(campo("dias_uti").cast("long")).alias("dias_uti_total"),
        F.sum(campo("valor_total").cast("decimal(20,2)"))
        .cast("decimal(20,2)").alias("valor_total_internacoes"),
        F.sum(campo("valor_uti").cast("decimal(20,2)"))
        .cast("decimal(20,2)").alias("valor_total_uti"),
    ]


def filtrar_periodo(df: DataFrame, coluna: str) -> DataFrame:
    return df.filter(F.to_date(F.col(coluna)).between(DATA_INICIO, DATA_FIM))


def contar_conflitos_meteorologia(df_gold: DataFrame) -> int:
    """Conta município-dias com mais de uma combinação meteorológica."""
    combinacoes = df_gold.select(
        F.to_date("data_internacao").alias("data"),
        F.col("CD_MUN").alias("codigo_municipio"),
        *METEOROLOGIA_GOLD,
    ).distinct()
    return (
        combinacoes.groupBy("data", "codigo_municipio")
        .count().filter(F.col("count") > 1).count()
    )


def criar_fato_municipio_dia_base(df_gold: DataFrame) -> DataFrame:
    """Agrega saúde e preserva a meteorologia única da Gold."""
    base = filtrar_periodo(df_gold, "data_internacao")
    saude = (
        base.groupBy(
            F.to_date("data_internacao").alias("data"),
            F.col("CD_MUN").alias("codigo_municipio"),
        )
        .agg(*expressoes_saude())
    )
    meteorologia = base.select(
        F.to_date("data_internacao").alias("data"),
        F.col("CD_MUN").alias("codigo_municipio"),
        *METEOROLOGIA_GOLD,
    ).distinct()
    return (
        saude.join(meteorologia, ["data", "codigo_municipio"], "inner")
        .withColumn("municipio_dia_id", municipio_dia_id())
    )


def agregar_meteorologia_adicional(df_inmet_horario: DataFrame) -> DataFrame:
    """Agrega os campos horários adicionais do INMET por estação e dia.

    ``radiacao_global_kj_m2`` é a energia global acumulada no intervalo
    horário, expressa em kJ/m² na fonte INMET. A soma dos intervalos válidos é
    semanticamente adequada para obter a energia diária. ``qtd_obs_radiacao``
    deve acompanhar a soma para revelar dias parciais.

    Pressão da estação e temperatura de orvalho são estados instantâneos:
    preservamos média, mínimo e máximo. Rajada representa um máximo do
    intervalo, por isso o indicador diário também usa máximo. Direção do vento
    não é agregada por média aritmética.
    """
    return (
        df_inmet_horario
        .withColumn("data", F.to_date("data_hora_utc"))
        .groupBy("codigo_estacao", "data")
        .agg(
            F.sum("radiacao_global_kj_m2").alias("radiacao_global_dia_kj_m2"),
            F.count("radiacao_global_kj_m2").cast("long").alias("qtd_obs_radiacao"),
            F.avg("pressao_estacao_mb").alias("pressao_media_dia_mb"),
            F.min("pressao_estacao_mb").alias("pressao_minima_dia_mb"),
            F.max("pressao_estacao_mb").alias("pressao_maxima_dia_mb"),
            F.count("pressao_estacao_mb").cast("long").alias("qtd_obs_pressao"),
            F.avg("temperatura_orvalho_c").alias("orvalho_medio_dia_c"),
            F.min("temperatura_orvalho_c").alias("orvalho_minimo_dia_c"),
            F.max("temperatura_orvalho_c").alias("orvalho_maximo_dia_c"),
            F.count("temperatura_orvalho_c").cast("long").alias("qtd_obs_orvalho"),
            F.max("vento_rajada_max_ms").alias("rajada_maxima_dia_ms"),
            F.count("vento_rajada_max_ms").cast("long").alias("qtd_obs_rajada"),
        )
    )


def adicionar_lags_exatos(
    df_fato: DataFrame,
    df_inmet_diario: DataFrame,
    dias=(1, 3, 7),
) -> DataFrame:
    """Anexa temperatura da data civil exata, sem usar posição de linha."""
    resultado = df_fato.alias("f")
    for dia in dias:
        nome = f"temperatura_media_lag_{dia}d"
        lookup = df_inmet_diario.select(
            F.col("codigo_estacao").alias(f"estacao_lag_{dia}"),
            F.to_date("data").alias(f"data_lag_{dia}"),
            F.col("temperatura_media_dia_c").alias(nome),
        )
        resultado = resultado.join(
            lookup,
            (F.col("codigo_estacao") == F.col(f"estacao_lag_{dia}"))
            & (F.col(f"data_lag_{dia}") == F.date_sub(F.col("data"), dia)),
            "left",
        ).drop(f"estacao_lag_{dia}", f"data_lag_{dia}")
    return resultado


def criar_fato_municipio_dia(
    df_gold: DataFrame,
    df_inmet_horario: DataFrame,
    df_inmet_diario: DataFrame,
) -> DataFrame:
    base = criar_fato_municipio_dia_base(df_gold)
    adicional = agregar_meteorologia_adicional(df_inmet_horario)
    enriquecida = base.join(adicional, ["codigo_estacao", "data"], "left")
    return adicionar_lags_exatos(enriquecida, df_inmet_diario).select(
        "municipio_dia_id", "data", "codigo_municipio", "codigo_estacao",
        *METRICAS_SAUDE,
        *[c for c in METEOROLOGIA_GOLD if c != "codigo_estacao"],
        *METEOROLOGIA_ADICIONAL,
        "temperatura_media_lag_1d", "temperatura_media_lag_3d",
        "temperatura_media_lag_7d",
    ).withColumn("ano", F.year("data"))


def criar_mapa_municipios(df_ibge: DataFrame) -> DataFrame:
    return df_ibge.select(
        F.col("CD_MUN").alias("codigo_municipio"),
        F.col("CD_MUN_6").alias("codigo_municipio_sih"),
    ).dropDuplicates(["codigo_municipio_sih"])


def criar_mapa_cid_resolvido(df_gold: DataFrame) -> DataFrame:
    return df_gold.select(
        F.upper(F.trim("codigo_cid")).alias("codigo_cid"),
        "codigo_categoria",
    ).dropDuplicates()


def criar_fato_municipio_dia_cid(
    df_sih: DataFrame,
    df_mapa_municipios: DataFrame,
    df_mapa_cid: DataFrame,
) -> DataFrame:
    base = (
        filtrar_periodo(df_sih, "DT_INTER")
        .select(
            F.to_date("DT_INTER").alias("data"),
            F.trim("MUNIC_RES").alias("codigo_municipio_sih"),
            F.upper(F.trim("DIAG_PRINC")).alias("codigo_cid"),
            F.trim("SEXO").alias("sexo"),
            "COD_IDADE", "IDADE",
            F.col("MORTE").alias("obito"),
            F.col("DIAS_PERM").alias("dias_permanencia"),
            F.col("UTI_INT_TO").alias("dias_uti"),
            F.col("VAL_TOT").alias("valor_total"),
            F.col("VAL_UTI").alias("valor_uti"),
        )
        .join(df_mapa_municipios, "codigo_municipio_sih", "left")
        .join(df_mapa_cid, "codigo_cid", "left")
        .withColumn("faixa_etaria", faixa_etaria())
    )
    return (
        base.groupBy(
            "data", "codigo_municipio", "codigo_categoria", "sexo", "faixa_etaria"
        )
        .agg(*expressoes_saude())
        .withColumn("municipio_dia_id", municipio_dia_id())
        .select(
            "municipio_dia_id", "data", "codigo_municipio",
            "codigo_categoria", "sexo", "faixa_etaria", *METRICAS_SAUDE,
        )
        .withColumn("ano", F.year("data"))
    )


def criar_dim_municipio(df_ibge: DataFrame, df_municipio_estacao: DataFrame) -> DataFrame:
    return (
        df_ibge.alias("i")
        .join(df_municipio_estacao.alias("m"), F.col("i.CD_MUN") == F.col("m.CD_MUN"), "left")
        .select(
            F.col("i.CD_MUN").alias("codigo_municipio"),
            F.col("i.CD_MUN_6").alias("codigo_municipio_sih"),
            F.col("i.NM_MUN").alias("nome_municipio"),
            F.col("i.SIGLA_UF").alias("sigla_uf"),
            F.col("i.AREA_KM2").alias("area_km2"),
            F.col("i.latitude").alias("latitude_municipio"),
            F.col("i.longitude").alias("longitude_municipio"),
            F.col("m.codigo_estacao").alias("codigo_estacao_referencia"),
            F.col("m.distancia_km").alias("distancia_estacao_km"),
            F.lit(2022).cast("int").alias("ano_referencia_ibge"),
        )
    )


def criar_dim_cid(df_gold: DataFrame) -> DataFrame:
    campos = [
        "codigo_categoria", "descricao_categoria", "cat_inicial_grupo",
        "cat_final_grupo", "descricao_grupo", "codigo_capitulo",
        "descricao_capitulo",
    ]
    return (
        df_gold.select(*campos).dropDuplicates()
        .withColumn(
            "codigo_grupo",
            F.concat_ws("-", "cat_inicial_grupo", "cat_final_grupo"),
        )
        .select(
            "codigo_categoria", "descricao_categoria", "cat_inicial_grupo",
            "cat_final_grupo", "codigo_grupo", "descricao_grupo",
            "codigo_capitulo", "descricao_capitulo",
        )
    )


def criar_dim_tempo(spark: SparkSession) -> DataFrame:
    meses = F.array(*[F.lit(x) for x in (
        "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
        "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro",
    )])
    dias = F.array(*[F.lit(x) for x in (
        "Domingo", "Segunda-feira", "Terça-feira", "Quarta-feira",
        "Quinta-feira", "Sexta-feira", "Sábado",
    )])
    return (
        spark.sql(
            f"SELECT explode(sequence(to_date('{DATA_INICIO}'), "
            f"to_date('{DATA_FIM}'), interval 1 day)) AS data"
        )
        .select(
            "data",
            F.date_format("data", "yyyyMMdd").cast("int").alias("data_id"),
            F.year("data").alias("ano"),
            F.month("data").alias("mes"),
            F.element_at(meses, F.month("data")).alias("nome_mes"),
            F.date_format("data", "yyyy-MM").alias("ano_mes"),
            F.quarter("data").alias("trimestre"),
            F.dayofmonth("data").alias("dia_mes"),
            F.dayofweek("data").alias("dia_semana_numero"),
            F.element_at(dias, F.dayofweek("data")).alias("dia_semana_nome"),
            F.dayofweek("data").isin(1, 7).alias("fim_de_semana"),
        )
    )


def inconsistencias_metadados_estacao(df_inmet_horario: DataFrame) -> DataFrame:
    campos = ["estacao", "latitude", "longitude", "uf", "regiao", "altitude"]
    expressoes = [F.countDistinct(c).alias(c) for c in campos]
    return (
        df_inmet_horario.groupBy("codigo_estacao").agg(*expressoes)
        .filter(reduce(lambda a, b: a | b, [F.col(c) > 1 for c in campos]))
    )


def criar_dim_estacao(df_inmet_horario: DataFrame) -> DataFrame:
    """Seleciona o cadastro mais recente após a validação histórica.

    A fonte contém pequenas revisões de nome, coordenadas e altitude entre
    anos. Como a dimensão solicitada tem uma linha por estação e não possui
    vigência, adotamos o metadado do ano mais recente de forma determinística.
    """
    cadastro = df_inmet_horario.select(
        "codigo_estacao", "ano", "estacao", "latitude", "longitude",
        "uf", "regiao", "altitude",
    ).dropDuplicates()
    janela = Window.partitionBy("codigo_estacao").orderBy(F.col("ano").desc())
    return (
        cadastro.withColumn("_ordem", F.row_number().over(janela))
        .filter(F.col("_ordem") == 1)
        .select(
            "codigo_estacao",
            F.col("estacao").alias("nome_estacao"),
            F.col("latitude").alias("latitude_estacao"),
            F.col("longitude").alias("longitude_estacao"),
            "uf", "regiao", F.col("altitude").alias("altitude_m"),
        )
    )
