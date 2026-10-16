"""Testes das tabelas analíticas compactas consumidas pelo Power BI."""

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from src.consumption.dashboard_analytics import (
    _contraste,
    _resumo,
    criar_lag_resumo,
)


class DashboardAnalyticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Testes-Dashboard-Analytics")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_resumo_da_peso_igual_a_cada_municipio(self):
        inicio = date(2024, 1, 1)
        linhas = []
        for municipio, frio, quente in (("M1", 4, 2), ("M2", 2, 4)):
            for indice in range(60):
                linhas.append(
                    (
                        municipio,
                        inicio + timedelta(days=indice),
                        10.0 if indice < 30 else 30.0,
                        frio if indice < 30 else quente,
                    )
                )
        fato = self.spark.createDataFrame(
            linhas,
            "codigo_municipio string, data date, temperatura_media_dia_c double, qtd_internacoes long",
        )
        resumo = _resumo(_contraste(fato, ["codigo_municipio"]), []).first()
        self.assertEqual(resumo.municipios_validos, 2)
        self.assertAlmostEqual(resumo.media_diferenca_relativa, 0.25)
        self.assertAlmostEqual(resumo.percentual_positivo, 50.0)
        self.assertAlmostEqual(resumo.percentual_negativo, 50.0)

    def test_elegibilidade_de_lag_e_independente(self):
        inicio = date(2024, 1, 1)
        linhas = []
        for indice in range(60):
            linhas.append(
                (
                    "M1",
                    inicio + timedelta(days=indice),
                    f"D{indice:02d}",
                    10.0 if indice < 30 else 30.0,
                    10.0 if indice < 29 else 30.0,
                    10.0 if indice < 30 else 30.0,
                    10.0 if indice < 30 else 30.0,
                    1,
                )
            )
        fato = self.spark.createDataFrame(
            linhas,
            "codigo_municipio string, data date, municipio_dia_id string, "
            "temperatura_media_dia_c double, temperatura_media_lag_1d double, "
            "temperatura_media_lag_3d double, temperatura_media_lag_7d double, "
            "qtd_internacoes long",
        )
        eventos = self.spark.createDataFrame(
            [],
            "codigo_municipio string, codigo_capitulo string, codigo_grupo string, "
            "qtd_internacoes long, temperatura_media_dia_c double, "
            "temperatura_media_lag_1d double, temperatura_media_lag_3d double, "
            "temperatura_media_lag_7d double",
        )
        linhas_lag = {
            (r.chave_alvo, r.defasagem): r
            for r in criar_lag_resumo(self.spark, fato, eventos).collect()
        }
        self.assertEqual(linhas_lag[("TOTAL", "D0")].municipios_validos, 1)
        self.assertEqual(linhas_lag[("TOTAL", "D-1")].municipios_validos, 0)
        self.assertEqual(linhas_lag[("TOTAL", "D-3")].municipios_validos, 1)
        self.assertEqual(linhas_lag[("TOTAL", "D-7")].municipios_validos, 1)


if __name__ == "__main__":
    unittest.main()
