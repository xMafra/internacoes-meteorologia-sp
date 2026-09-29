"""Constrói a camada de consumo para Power BI sem alterar Silver ou Gold."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from pyspark.sql import SparkSession

from src.consumption.transformations import (
    criar_dim_cid,
    criar_dim_estacao,
    criar_dim_municipio,
    criar_dim_tempo,
    criar_fato_municipio_dia,
    criar_fato_municipio_dia_cid,
    criar_mapa_cid_resolvido,
    criar_mapa_municipios,
    contar_conflitos_meteorologia,
    inconsistencias_metadados_estacao,
)


BASE_PATH = "/home/jovyan/work/data"
GOLD_FATO = f"{BASE_PATH}/gold/fato_internacao_meteorologia"
GOLD_MUNICIPIO_ESTACAO = f"{BASE_PATH}/gold/municipio_estacao"
SILVER_SIH = f"{BASE_PATH}/silver/sih"
SILVER_IBGE = f"{BASE_PATH}/silver/ibge/2022"
SILVER_INMET = f"{BASE_PATH}/silver/inmet"
SILVER_INMET_DIARIO = f"{BASE_PATH}/silver/inmet_diario"
CONSUMPTION = f"{BASE_PATH}/consumption"


def ler_inmet_horario(spark):
    partes = [
        spark.read.option("mergeSchema", "true").parquet(f"{SILVER_INMET}/{ano}")
        for ano in range(2023, 2026)
    ]
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte, allowMissingColumns=True)
    return resultado.select(
        "ano", "codigo_estacao", "data_hora_utc", "estacao", "latitude",
        "longitude", "uf", "regiao", "altitude",
        "radiacao_global_kj_m2", "pressao_estacao_mb",
        "temperatura_orvalho_c", "vento_rajada_max_ms",
    )


def ler_sih(spark):
    """Lê as raízes anuais não particionadas da Silver SIH."""
    partes = [
        spark.read.option("mergeSchema", "true")
        .option("recursiveFileLookup", "true")
        .parquet(f"{SILVER_SIH}/{ano}")
        for ano in range(2023, 2026)
    ]
    resultado = partes[0]
    for parte in partes[1:]:
        resultado = resultado.unionByName(parte, allowMissingColumns=True)
    return resultado


def gravar(df, nome, particoes=None, reparticoes=None):
    saida = df
    if reparticoes:
        saida = saida.repartition(reparticoes, *(particoes or []))
    escritor = saida.write.mode("overwrite").option("compression", "snappy")
    if particoes:
        escritor = escritor.partitionBy(*particoes)
    caminho = f"{CONSUMPTION}/{nome}"
    escritor.parquet(caminho)
    print(f"Gravado: {caminho}")


def main():
    spark = SparkSession.builder.appName("TCC-Consumption-PowerBI").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    try:
        gold = spark.read.option("mergeSchema", "true").parquet(GOLD_FATO)
        municipio_estacao = spark.read.parquet(GOLD_MUNICIPIO_ESTACAO)
        sih = ler_sih(spark)
        ibge = spark.read.parquet(SILVER_IBGE)
        inmet_diario = spark.read.option("mergeSchema", "true").parquet(SILVER_INMET_DIARIO)
        inmet_horario = ler_inmet_horario(spark).cache()

        conflitos_meteo = contar_conflitos_meteorologia(gold)
        if conflitos_meteo:
            raise ValueError(
                f"Meteorologia não é única em {conflitos_meteo} chaves data + município."
            )

        divergencias = inconsistencias_metadados_estacao(inmet_horario).cache()
        total_divergencias = divergencias.count()
        print(
            "Validação histórica dos metadados de estação: "
            f"{total_divergencias} estações com revisões entre anos."
        )
        if total_divergencias:
            divergencias.orderBy("codigo_estacao").show(40, truncate=False)
            print("Dimensão usará deterministicamente o cadastro mais recente (2025).")

        mapa_municipios = criar_mapa_municipios(ibge)
        mapa_cid = criar_mapa_cid_resolvido(gold)

        fatos_e_dimensoes = {
            "fato_municipio_dia": criar_fato_municipio_dia(
                gold, inmet_horario, inmet_diario
            ),
            "fato_municipio_dia_cid": criar_fato_municipio_dia_cid(
                sih, mapa_municipios, mapa_cid
            ),
            "dim_municipio": criar_dim_municipio(ibge, municipio_estacao),
            "dim_cid": criar_dim_cid(gold),
            "dim_tempo": criar_dim_tempo(spark),
            "dim_estacao": criar_dim_estacao(inmet_horario),
        }

        # Três partições anuais por fato: leitura temporal eficiente e poucos
        # arquivos para importação no Power BI. Dimensões são pequenas e não
        # recebem particionamento físico.
        gravar(fatos_e_dimensoes["dim_municipio"], "dim_municipio", reparticoes=1)
        gravar(fatos_e_dimensoes["dim_cid"], "dim_cid", reparticoes=1)
        gravar(fatos_e_dimensoes["dim_tempo"], "dim_tempo", reparticoes=1)
        gravar(fatos_e_dimensoes["dim_estacao"], "dim_estacao", reparticoes=1)
        gravar(
            fatos_e_dimensoes["fato_municipio_dia"],
            "fato_municipio_dia", ["ano"], 3,
        )
        gravar(
            fatos_e_dimensoes["fato_municipio_dia_cid"],
            "fato_municipio_dia_cid", ["ano"], 3,
        )
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
