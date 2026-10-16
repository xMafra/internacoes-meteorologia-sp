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
    rotulo_sexo,
)
from src.quality.consumption.dq_consumption import validar_consumption
from src.gold.municipio_estacao import criar_municipio_estacao_ano


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
            ("1", 10), ("2", 30), ("3", 11), ("4", 17), ("4", 18), ("4", 40),
            ("4", 60), ("4", 80), ("5", 0), ("0", 0), (None, 20),
        ], "COD_IDADE string, IDADE int").withColumn("faixa", faixa_etaria())
        self.assertEqual(
            [r.faixa for r in df.collect()],
            ["0-17", "0-17", "0-17", "0-17", "18-39", "40-59", "60-79",
             "80+", "80+", "Ignorada", "Ignorada"],
        )

    def test_rotulo_sexo_preserva_codigo_oficial_sih(self):
        df = (
            self.spark.createDataFrame(
                [("1",), ("3",), ("0",), ("4",)], "sexo string"
            )
            .withColumn("sexo_descricao", rotulo_sexo())
        )
        self.assertEqual(
            [(r.sexo, r.sexo_descricao) for r in df.collect()],
            [
                ("1", "Masculino"), ("3", "Feminino"),
                ("0", "Ignorado"), ("4", "Não mapeado"),
            ],
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
        fato = self.spark.createDataFrame([
            (date(2024, 1, 1), "A001", "M1", 1.0),
            (date(2024, 1, 5), "A001", "M1", 5.0),
            (date(2024, 1, 6), "A001", "M1", 999.0),
            (date(2024, 1, 7), "A001", "M1", 7.0),
            (date(2024, 1, 8), "A001", "M1", 8.0),
        ], "data date, codigo_estacao string, codigo_municipio string, temperatura_media_dia_c double")
        meteo = self.spark.createDataFrame([
            ("A001", date(2014, 12, 31), 0.0),
        ], "codigo_estacao string, data date, temperatura_media_dia_c double")
        r = adicionar_lags_exatos(fato, meteo).filter(F.col("data") == "2024-01-08").first()
        self.assertEqual((r.temperatura_media_lag_1d, r.temperatura_media_lag_3d,
                          r.temperatura_media_lag_7d), (7.0, 5.0, 1.0))

    def test_lags_virada_ano_bissexto_e_isolamento_municipal(self):
        fato = self.spark.createDataFrame([
            (date(2015, 1, 1), "A001", "M1", 10.0),
            (date(2015, 1, 1), "A002", "M2", 110.0),
            (date(2020, 2, 29), "A001", "M1", 29.0),
            (date(2020, 3, 1), "A001", "M1", 30.0),
            (date(2020, 12, 25), "A001", "M1", 7.0),
            (date(2020, 12, 29), "A001", "M1", 3.0),
            (date(2020, 12, 31), "A001", "M1", 1.0),
            (date(2021, 1, 1), "B001", "M1", 20.0),
            (date(2020, 12, 31), "A002", "M2", 101.0),
            (date(2021, 1, 1), "B002", "M2", 120.0),
        ], "data date, codigo_estacao string, codigo_municipio string, temperatura_media_dia_c double")
        meteo = self.spark.createDataFrame([
            ("A001", date(2014, 12, 25), 7.0),
            ("A001", date(2014, 12, 29), 3.0),
            ("A001", date(2014, 12, 31), 1.0),
            ("A002", date(2014, 12, 31), 101.0),
            ("B001", date(2020, 12, 25), 700.0),
            ("B001", date(2020, 12, 29), 300.0),
            ("B001", date(2020, 12, 31), 100.0),
            ("B002", date(2020, 12, 31), 1001.0),
        ], "codigo_estacao string, data date, temperatura_media_dia_c double")
        linhas = {
            (r.codigo_municipio, r.data): r
            for r in adicionar_lags_exatos(fato, meteo).collect()
        }
        janeiro = linhas[("M1", date(2015, 1, 1))]
        self.assertEqual(
            (janeiro.temperatura_media_lag_1d,
             janeiro.temperatura_media_lag_3d,
             janeiro.temperatura_media_lag_7d),
            (1.0, 3.0, 7.0),
        )
        self.assertEqual(
            linhas[("M2", date(2015, 1, 1))].temperatura_media_lag_1d,
            101.0,
        )
        self.assertEqual(
            linhas[("M1", date(2020, 3, 1))].temperatura_media_lag_1d,
            29.0,
        )
        janeiro_2021 = linhas[("M1", date(2021, 1, 1))]
        self.assertEqual(
            (janeiro_2021.temperatura_media_lag_1d,
             janeiro_2021.temperatura_media_lag_3d,
             janeiro_2021.temperatura_media_lag_7d),
            (1.0, 3.0, 7.0),
        )
        self.assertEqual(
            linhas[("M2", date(2021, 1, 1))].temperatura_media_lag_1d,
            101.0,
        )

    def test_hierarquia_cid_resolvida(self):
        gold = self.spark.createDataFrame([
            ("J00", "Infecção", "J00", "J06", "Infecções respiratórias", "X", "Respiratório"),
            ("J00", "Infecção", "J00", "J06", "Infecções respiratórias", "X", "Respiratório"),
        ], "codigo_categoria string, descricao_categoria string, cat_inicial_grupo string, cat_final_grupo string, descricao_grupo string, codigo_capitulo string, descricao_capitulo string")
        linhas = criar_dim_cid(gold).collect()
        self.assertEqual(len(linhas), 1)
        self.assertEqual(linhas[0].codigo_grupo, "J00-J06")

    def test_estacao_e_selecionada_por_ano_sem_retroagir(self):
        ibge = self.spark.createDataFrame([
            ("3500000", "Teste", "SP", 10.0, -23.0, -46.0)
        ], "CD_MUN string, NM_MUN string, SIGLA_UF string, AREA_KM2 double, latitude double, longitude double")
        diario = self.spark.createDataFrame([
            ("A_OLD", date(2015, 1, 1), 20.0, "Antiga", -23.5, -46.5),
            ("A_OLD", date(2015, 1, 2), 21.0, "Antiga", -23.5, -46.5),
            ("A_NEW", date(2025, 1, 1), 22.0, "Nova", -23.01, -46.01),
            ("A_NEW", date(2025, 1, 2), 23.0, "Nova", -23.01, -46.01),
        ], "codigo_estacao string, data date, temperatura_media_dia_c double, estacao string, latitude_estacao double, longitude_estacao double")
        mapa = {
            r.ano: r.codigo_estacao
            for r in criar_municipio_estacao_ano(ibge, diario, 0.8).collect()
        }
        self.assertEqual(mapa, {2015: "A_OLD", 2025: "A_NEW"})

    def test_calendario_continuo(self):
        calendario = criar_dim_tempo(self.spark)
        self.assertEqual(calendario.count(), 4018)
        limites = calendario.agg(F.min("data"), F.max("data")).first()
        self.assertEqual(limites[0], date(2015, 1, 1))
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
            ("3509502", ano, "A001", 10.0, 100.0, "MAIS_PROXIMA_COM_COBERTURA_MINIMA")
            for ano in range(2015, 2026)
        ], "CD_MUN string, ano int, codigo_estacao string, distancia_km double, cobertura_temperatura_pct double, criterio_selecao string")
        sih = self.spark.createDataFrame([
            (date(2024, 1, 15), "350950", "J000", "1", "4", 80, 1, 3, 1, 10.0, 2.0)
        ], "DT_INTER date, MUNIC_RES string, DIAG_PRINC string, SEXO string, COD_IDADE string, IDADE int, MORTE long, DIAS_PERM long, UTI_INT_TO long, VAL_TOT double, VAL_UTI double")
        inmet_horario = self.spark.createDataFrame([
            (2025, "A001", "Estação", -23.1, -46.1, "SP", "SE", 700.0,
             datetime(2024, 1, 15, 0), 10.0, 1000.0, 12.0, 6.0)
        ], "ano int, codigo_estacao string, estacao string, latitude double, longitude double, uf string, regiao string, altitude double, data_hora_utc timestamp, radiacao_global_kj_m2 double, pressao_estacao_mb double, temperatura_orvalho_c double, vento_rajada_max_ms double")
        inmet_diario = self.spark.createDataFrame([
            ("A001", date(2024, 1, 8), 13.0, 13.0, 13.0, None, None, None,
             None, None, None, 24, 24, 0, 0, 0),
            ("A001", date(2024, 1, 12), 16.0, 16.0, 16.0, None, None, None,
             None, None, None, 24, 24, 0, 0, 0),
            ("A001", date(2024, 1, 14), 19.0, 19.0, 19.0, None, None, None,
             None, None, None, 24, 24, 0, 0, 0),
            ("A001", date(2024, 1, 15), 20.0, 15.0, 25.0, 70.0, 60.0, 80.0,
             2.0, 3.0, 5.0, 24, 24, 24, 24, 24),
        ], "codigo_estacao string, data date, temperatura_media_dia_c double, temperatura_minima_dia_c double, temperatura_maxima_dia_c double, umidade_media_dia_pct double, umidade_minima_dia_pct double, umidade_maxima_dia_pct double, precipitacao_dia_mm double, vento_medio_dia_ms double, vento_maximo_dia_ms double, qtd_observacoes long, qtd_obs_temperatura long, qtd_obs_umidade long, qtd_obs_precipitacao long, qtd_obs_vento long")

        fato_dia = criar_fato_municipio_dia(
            gold, inmet_horario, inmet_diario,
            ibge, municipio_estacao, self.spark,
        )
        self.assertEqual(fato_dia.count(), 4018)
        sem_internacao = fato_dia.filter(F.col("data") == "2015-01-01").first()
        self.assertEqual(sem_internacao.qtd_internacoes, 0)
        self.assertEqual(sem_internacao.dia_sem_internacao, 1)
        self.assertIsNone(sem_internacao.temperatura_media_dia_c)
        self.assertEqual(sem_internacao.meteorologia_disponivel, 0)
        com_internacao = fato_dia.filter(F.col("data") == "2024-01-15").first()
        self.assertEqual(com_internacao.qtd_internacoes, 1)
        self.assertEqual(com_internacao.dia_sem_internacao, 0)
        self.assertEqual(com_internacao.temperatura_media_dia_c, 20.0)
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
