"""Testes sintéticos dos denominadores e do efeito de composição."""

import sys
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from src.validation.metric_audit import (
    auditoria_denominador_esparso,
    contraste_frio_quente,
    denominadores_exposicao,
    resumo_distribuicao,
)
from src.validation.lag_intramunicipal_audit import (
    contrastes_respiratorios_por_lag,
    contrastes_totais_por_lag,
)


class AnalyticMetricTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Testes-Metricas-Analiticas")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def criar_exposicao(self):
        inicio = date(2024, 1, 1)
        linhas = []
        for indice in range(20):
            temperatura = 10.0 if indice < 10 else 30.0
            linhas.append((f"D{indice:02d}", "M1", inicio + timedelta(days=indice), temperatura, 0))
        return self.spark.createDataFrame(
            linhas,
            "municipio_dia_id string, codigo_municipio string, data date, "
            "temperatura_media_dia_c double, qtd_internacoes long",
        )

    def test_cid_esparsa_usa_todos_os_dias_de_exposicao(self):
        exposicao = self.criar_exposicao()
        eventos = self.spark.createDataFrame([
            ("D00", "J18", 10.0, 2),
            ("D01", "J18", 10.0, 3),
            ("D10", "J18", 30.0, 1),
            ("D11", "J18", 30.0, 3),
        ], "municipio_dia_id string, codigo_categoria string, temperatura_media_dia_c double, qtd_internacoes long")
        resultado = auditoria_denominador_esparso(
            eventos, ["codigo_categoria"], denominadores_exposicao(exposicao)
        ).first()
        self.assertEqual(resultado.municipio_dias_exposicao_frio, 10)
        self.assertEqual(resultado.municipio_dias_com_evento_frio, 2)
        self.assertAlmostEqual(resultado.media_correta_frio, 0.5)
        self.assertAlmostEqual(resultado.media_incorreta_so_dias_evento_frio, 2.5)
        self.assertAlmostEqual(resultado.media_correta_quente, 0.4)
        self.assertAlmostEqual(resultado.media_incorreta_so_dias_evento_quente, 2.0)

    def test_cid_idade_sexo_mantem_denominador_global_de_exposicao(self):
        exposicao = self.criar_exposicao()
        eventos = self.spark.createDataFrame([
            ("D00", "J18", "60-79", "1", "Masculino", 10.0, 2),
            ("D10", "J18", "60-79", "1", "Masculino", 30.0, 1),
        ], "municipio_dia_id string, codigo_categoria string, faixa_etaria string, sexo string, sexo_descricao string, temperatura_media_dia_c double, qtd_internacoes long")
        resultado = auditoria_denominador_esparso(
            eventos,
            ["codigo_categoria", "faixa_etaria", "sexo", "sexo_descricao"],
            denominadores_exposicao(exposicao),
        ).first()
        self.assertEqual(resultado.municipio_dias_exposicao_frio, 10)
        self.assertEqual(resultado.municipio_dias_exposicao_quente, 10)
        self.assertAlmostEqual(resultado.media_correta_frio, 0.2)
        self.assertAlmostEqual(resultado.media_correta_quente, 0.1)

    def test_contraste_agregado_pode_refletir_composicao_municipal(self):
        inicio = date(2024, 1, 1)
        linhas = []
        for indice in range(60):
            linhas.append(("M_ALTO", inicio + timedelta(days=indice), 10.0, 100))
        for indice in range(30):
            linhas.append(("M_ALTO", inicio + timedelta(days=60 + indice), 30.0, 100))
        for indice in range(30):
            linhas.append(("M_BAIXO", inicio + timedelta(days=indice), 10.0, 1))
        for indice in range(60):
            linhas.append(("M_BAIXO", inicio + timedelta(days=30 + indice), 30.0, 1))
        fato = self.spark.createDataFrame(
            linhas,
            "codigo_municipio string, data date, temperatura_media_dia_c double, qtd_internacoes long",
        )
        estadual = contraste_frio_quente(fato, []).first()
        municipal = contraste_frio_quente(fato, ["codigo_municipio"])
        resumo = resumo_distribuicao(municipal, []).first()
        self.assertGreater(estadual.diferenca_relativa, 0.9)
        self.assertAlmostEqual(resumo.media_diferenca_relativa, 0.0)
        self.assertAlmostEqual(resumo.mediana_diferenca_relativa, 0.0)

    def test_sinal_estrito_e_faixa_aproximadamente_zero_sao_separados(self):
        contrastes = self.spark.createDataFrame([
            ("M1", 1, 1, 0.005),
            ("M2", 1, 1, -0.005),
            ("M3", 1, 1, 0.0),
        ], (
            "codigo_municipio string, elegivel_amostra int, "
            "contraste_relativo_valido int, diferenca_relativa double"
        ))
        resumo = resumo_distribuicao(contrastes, []).first()
        self.assertEqual(resumo.qtd_positivos, 1)
        self.assertEqual(resumo.qtd_negativos, 1)
        self.assertEqual(resumo.qtd_iguais_zero, 1)
        self.assertEqual(resumo.qtd_aprox_zero, 3)
        self.assertEqual(resumo.qtd_acima_limiar, 0)
        self.assertEqual(resumo.qtd_abaixo_limiar, 0)

    def test_elegibilidade_e_calculada_independentemente_por_lag(self):
        inicio = date(2024, 1, 1)
        linhas = []
        for indice in range(60):
            linhas.append((
                "M1",
                inicio + timedelta(days=indice),
                f"D{indice:02d}",
                10.0 if indice < 30 else 30.0,
                10.0 if indice < 29 else 30.0,
                10.0 if indice < 30 else 30.0,
                10.0 if indice < 30 else 30.0,
                2,
            ))
        fato = self.spark.createDataFrame(linhas, (
            "codigo_municipio string, data date, municipio_dia_id string, "
            "temperatura_media_dia_c double, temperatura_media_lag_1d double, "
            "temperatura_media_lag_3d double, temperatura_media_lag_7d double, "
            "qtd_internacoes long"
        ))
        resultados = {
            r.defasagem: r for r in contrastes_totais_por_lag(fato).collect()
        }
        self.assertEqual((resultados["D0"].dias_frios, resultados["D0"].dias_quentes), (30, 30))
        self.assertEqual(resultados["D0"].elegivel_amostra, 1)
        self.assertEqual((resultados["D-1"].dias_frios, resultados["D-1"].dias_quentes), (29, 31))
        self.assertEqual(resultados["D-1"].elegivel_amostra, 0)

    def test_lag_respiratorio_preserva_zeros_no_denominador_municipal(self):
        inicio = date(2024, 1, 1)
        linhas = []
        for indice in range(60):
            temperatura = 10.0 if indice < 30 else 30.0
            linhas.append((
                "M1", inicio + timedelta(days=indice), f"D{indice:02d}",
                temperatura, temperatura, temperatura, temperatura, 0,
            ))
        fato = self.spark.createDataFrame(linhas, (
            "codigo_municipio string, data date, municipio_dia_id string, "
            "temperatura_media_dia_c double, temperatura_media_lag_1d double, "
            "temperatura_media_lag_3d double, temperatura_media_lag_7d double, "
            "qtd_internacoes long"
        ))
        eventos = self.spark.createDataFrame([
            ("M1", "D00", "10", "J09-J18", "Influenza [gripe] e pneumonia", 2, 10.0, 10.0, 10.0, 10.0),
            ("M1", "D01", "10", "J09-J18", "Influenza [gripe] e pneumonia", 3, 10.0, 10.0, 10.0, 10.0),
            ("M1", "D30", "10", "J09-J18", "Influenza [gripe] e pneumonia", 1, 30.0, 30.0, 30.0, 30.0),
            ("M1", "D31", "10", "J09-J18", "Influenza [gripe] e pneumonia", 3, 30.0, 30.0, 30.0, 30.0),
        ], (
            "codigo_municipio string, municipio_dia_id string, codigo_capitulo string, "
            "codigo_grupo string, descricao_grupo string, qtd_internacoes long, "
            "temperatura_media_dia_c double, temperatura_media_lag_1d double, "
            "temperatura_media_lag_3d double, temperatura_media_lag_7d double"
        ))
        resultado = (
            contrastes_respiratorios_por_lag(fato, eventos)
            .filter("defasagem = 'D0' AND alvo = 'GRUPO_J09-J18'")
            .first()
        )
        self.assertEqual((resultado.dias_frios, resultado.dias_quentes), (30, 30))
        self.assertAlmostEqual(resultado.media_frio_municipio, 5 / 30)
        self.assertAlmostEqual(resultado.media_quente_municipio, 4 / 30)
        self.assertEqual(resultado.elegivel_amostra, 1)


if __name__ == "__main__":
    unittest.main()
