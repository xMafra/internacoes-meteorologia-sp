"""Testes sintéticos da camada de consumo.

Execução:
spark-submit --master local[2] tests/test_consumption.py -v
"""

import unittest
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F

from src.consumption.transformations import (
    METRICAS_SAUDE,
    adicionar_lags_exatos,
    agregar_meteorologia_adicional,
    criar_dim_cid,
    criar_dim_estacao,
    criar_dim_municipio,
    criar_dim_tempo,
    criar_fato_municipio_dia,
    criar_fato_municipio_dia_cid,
    criar_mapa_cid_resolvido,
    criar_mapa_municipios,
    expressoes_saude,
    faixa_etaria,
    municipio_dia_id,
)
from src.quality.consumption.dq_consumption import validar_consumption


class ConsumptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Testes-Consumption")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_conversao_cod_idade_para_faixa(self):
        df = self.spark.createDataFrame([
            ("2", 30), ("3", 11), ("4", 17), ("4", 18), ("4", 40),
            ("4", 60), ("4", 80), ("5", 0), ("0", 0), (None, 20),
        ], "COD_IDADE string, IDADE int").withColumn("faixa", faixa_etaria())
        self.assertEqual(
            [r.faixa for r in df.collect()],
            ["0-17", "0-17", "0-17", "18-39", "40-59", "60-79",
             "80+", "80+", "Ignorada", "Ignorada"],
        )

    def test_municipio_dia_id(self):
        linha = (
            self.spark.createDataFrame([(date(2024, 1, 15), "3509502")], "data date, codigo_municipio string")
            .withColumn("id", municipio_dia_id()).first()
        )
        self.assertEqual(linha.id, "20240115_3509502")

    def test_agregacao_saude_preserva_numeradores(self):
        df = self.spark.createDataFrame([
            ("x", 0, 2, 0, 10.25, 0.0),
            ("x", 1, 5, 2, 20.15, 8.0),
        ], "chave string, obito long, dias_permanencia long, dias_uti long, valor_total double, valor_uti double")
        r = df.groupBy("chave").agg(*expressoes_saude()).first()
        self.assertEqual(tuple(r[c] for c in METRICAS_SAUDE[:4]), (2, 1, 7, 2))
        self.assertEqual(r.valor_total_internacoes, Decimal("30.40"))
        self.assertEqual(r.valor_total_uti, Decimal("8.00"))

    def test_agregacoes_meteorologicas_adicionais(self):
        df = self.spark.createDataFrame([
            ("A001", datetime(2024, 1, 1, 0), 10.0, 1000.0, 12.0, 4.0),
            ("A001", datetime(2024, 1, 1, 1), 20.0, 1002.0, 14.0, 7.0),
            ("A001", datetime(2024, 1, 1, 2), None, None, None, None),
        ], "codigo_estacao string, data_hora_utc timestamp, radiacao_global_kj_m2 double, pressao_estacao_mb double, temperatura_orvalho_c double, vento_rajada_max_ms double")
        r = agregar_meteorologia_adicional(df).first()
        self.assertEqual(r.radiacao_global_dia_kj_m2, 30.0)
        self.assertEqual(r.qtd_obs_radiacao, 2)
        self.assertEqual((r.pressao_media_dia_mb, r.pressao_minima_dia_mb, r.pressao_maxima_dia_mb), (1001.0, 1000.0, 1002.0))
        self.assertEqual((r.orvalho_medio_dia_c, r.orvalho_minimo_dia_c, r.orvalho_maximo_dia_c), (13.0, 12.0, 14.0))
        self.assertEqual((r.rajada_maxima_dia_ms, r.qtd_obs_rajada), (7.0, 2))

    def test_lags_usam_data_exata_em_serie_esparsa(self):
        fato = self.spark.createDataFrame(
            [(date(2024, 1, 8), "A001")], "data date, codigo_estacao string"
        )
        meteo = self.spark.createDataFrame([
            ("A001", date(2024, 1, 1), 1.0),
            ("A001", date(2024, 1, 5), 5.0),
            ("A001", date(2024, 1, 7), 7.0),
            ("A001", date(2024, 1, 6), 999.0),
        ], "codigo_estacao string, data date, temperatura_media_dia_c double")
        r = adicionar_lags_exatos(fato, meteo).first()
        self.assertEqual((r.temperatura_media_lag_1d, r.temperatura_media_lag_3d,
                          r.temperatura_media_lag_7d), (7.0, 5.0, 1.0))

    def test_hierarquia_cid_resolvida(self):
        gold = self.spark.createDataFrame([
            ("J00", "Infecção", "J00", "J06", "Infecções respiratórias", "X", "Respiratório"),
            ("J00", "Infecção", "J00", "J06", "Infecções respiratórias", "X", "Respiratório"),
        ], "codigo_categoria string, descricao_categoria string, cat_inicial_grupo string, cat_final_grupo string, descricao_grupo string, codigo_capitulo string, descricao_capitulo string")
        linhas = criar_dim_cid(gold).collect()
        self.assertEqual(len(linhas), 1)
        self.assertEqual(linhas[0].codigo_grupo, "J00-J06")

    def test_calendario_continuo(self):
        calendario = criar_dim_tempo(self.spark)
        self.assertEqual(calendario.count(), 1096)
        limites = calendario.agg(F.min("data"), F.max("data")).first()
        self.assertEqual(limites[0], date(2023, 1, 1))
        self.assertEqual(limites[1], date(2025, 12, 31))
        self.assertTrue(calendario.filter(F.col("data") == "2024-01-06").first().fim_de_semana)

    def test_regras_principais_dq_em_fluxo_sintetico(self):
        meteo_campos = {
            "codigo_estacao": "A001", "temperatura_media_dia_c": 20.0,
            "temperatura_minima_dia_c": 15.0, "temperatura_maxima_dia_c": 25.0,
            "umidade_media_dia_pct": 70.0, "umidade_minima_dia_pct": 60.0,
            "umidade_maxima_dia_pct": 80.0, "precipitacao_dia_mm": 2.0,
            "vento_medio_dia_ms": 3.0, "vento_maximo_dia_ms": 5.0,
            "qtd_observacoes_inmet": 24, "qtd_obs_temperatura": 24,
            "qtd_obs_umidade": 24, "qtd_obs_precipitacao": 24,
            "qtd_obs_vento": 24, "meteorologia_disponivel": 1,
            "motivo_sem_meteorologia": "",
        }
        gold_row = {
            "data_internacao": date(2024, 1, 15), "CD_MUN": "3509502",
            "codigo_cid": "J000", "codigo_categoria": "J00",
            "descricao_categoria": "Infecção", "cat_inicial_grupo": "J00",
            "cat_final_grupo": "J06", "descricao_grupo": "Respiratórias",
            "codigo_capitulo": "X", "descricao_capitulo": "Respiratório",
            "obito": 1, "dias_permanencia": 3, "dias_uti": 1,
            "valor_total": 10.0, "valor_uti": 2.0, **meteo_campos,
        }
        gold = self.spark.createDataFrame([gold_row])
        ibge = self.spark.createDataFrame([
            ("3509502", "350950", "Município", "SP", 100.0, -23.0, -46.0)
        ], "CD_MUN string, CD_MUN_6 string, NM_MUN string, SIGLA_UF string, AREA_KM2 double, latitude double, longitude double")
        municipio_estacao = self.spark.createDataFrame([
            ("3509502", "A001", 10.0)
        ], "CD_MUN string, codigo_estacao string, distancia_km double")
        sih = self.spark.createDataFrame([
            (date(2024, 1, 15), "350950", "J000", "1", "4", 80, 1, 3, 1, 10.0, 2.0)
        ], "DT_INTER date, MUNIC_RES string, DIAG_PRINC string, SEXO string, COD_IDADE string, IDADE int, MORTE long, DIAS_PERM long, UTI_INT_TO long, VAL_TOT double, VAL_UTI double")
        inmet_horario = self.spark.createDataFrame([
            (2025, "A001", "Estação", -23.1, -46.1, "SP", "SE", 700.0,
             datetime(2024, 1, 15, 0), 10.0, 1000.0, 12.0, 6.0)
        ], "ano int, codigo_estacao string, estacao string, latitude double, longitude double, uf string, regiao string, altitude double, data_hora_utc timestamp, radiacao_global_kj_m2 double, pressao_estacao_mb double, temperatura_orvalho_c double, vento_rajada_max_ms double")
        inmet_diario = self.spark.createDataFrame([
            ("A001", date(2024, 1, 8), 13.0),
            ("A001", date(2024, 1, 12), 16.0),
            ("A001", date(2024, 1, 14), 19.0),
        ], "codigo_estacao string, data date, temperatura_media_dia_c double")

        fato_dia = criar_fato_municipio_dia(gold, inmet_horario, inmet_diario)
        fato_cid = criar_fato_municipio_dia_cid(
            sih, criar_mapa_municipios(ibge), criar_mapa_cid_resolvido(gold)
        )
        report = validar_consumption(
            fato_dia, fato_cid,
            criar_dim_municipio(ibge, municipio_estacao), criar_dim_cid(gold),
            criar_dim_tempo(self.spark), criar_dim_estacao(inmet_horario),
            gold, inmet_diario, inmet_horario,
        )
        self.assertTrue(report.aprovado, [r.as_dict() for r in report.resultados if not r.passou])
        self.assertTrue(all(r.passou for r in report.resultados if r.nivel == "ERROR"))


if __name__ == "__main__":
    unittest.main()
