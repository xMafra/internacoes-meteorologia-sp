import tempfile
import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession

from src.silver.inmet import extrair_metadados, processar_csv


class InmetHistoricalParserTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[1]")
            .appName("test-inmet-historical")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_formato_antigo_metadados_data_hora_e_sentinela(self):
        linhas = [
            "REGIÃO:;SE",
            "UF:;SP",
            "ESTAÇÃO:;ESTACAO HISTORICA",
            "CODIGO (WMO):;A999",
            "LATITUDE:;-23,5",
            "LONGITUDE:;-46,6",
            "ALTITUDE:;700,5",
            "DATA DE FUNDAÇÃO (YYYY-MM-DD):;2000-01-01",
            ";".join(f"campo_{i}" for i in range(19)) + ";",
            "2014-12-25;00:00;0;900;901;899;-9999;20,5;10;21;20;11;9;80;70;75;100;5;9999;",
            "2014-12-25;01:00;0;900;901;899;0;999;10;21;20;11;9;80;70;75;100;5;2;",
        ]
        with tempfile.TemporaryDirectory(prefix="test_inmet_") as tmp:
            caminho = Path(tmp) / "historico.csv"
            caminho.write_text("\n".join(linhas), encoding="latin-1")

            metadados = extrair_metadados(caminho)
            self.assertEqual(metadados["REGIAO:"], "SE")
            self.assertEqual(metadados["ESTACAO:"], "ESTACAO HISTORICA")

            registros = processar_csv(self.spark, caminho, 2014).orderBy(
                "data_hora_utc"
            ).collect()
            registro = registros[0]
            self.assertEqual(registro.codigo_estacao, "A999")
            self.assertEqual(str(registro.data_hora_utc), "2014-12-25 00:00:00")
            self.assertAlmostEqual(registro.temperatura_c, 20.5)
            self.assertIsNone(registro.radiacao_global_kj_m2)
            self.assertIsNone(registro.vento_velocidade_ms)
            self.assertIsNone(registros[1].temperatura_c)

    def test_metadados_utf8_com_acentos(self):
        linhas = [
            "REGIÃO:;SE", "UF:;SP", "ESTAÇÃO:;SÃO PAULO",
            "CODIGO (WMO):;A701", "LATITUDE:;-23,5", "LONGITUDE:;-46,6",
            "ALTITUDE:;700", "DATA DE FUNDAÇÃO:;2000-01-01",
        ]
        with tempfile.TemporaryDirectory(prefix="test_inmet_utf8_") as tmp:
            caminho = Path(tmp) / "utf8.csv"
            caminho.write_text("\n".join(linhas), encoding="utf-8")
            metadados = extrair_metadados(caminho)
            self.assertEqual(metadados["REGIAO:"], "SE")
            self.assertEqual(metadados["ESTACAO:"], "SÃO PAULO")

    def test_metadados_2019_com_interrogacao_na_fonte(self):
        linhas = [
            "REGI?O:;SE", "UF:;SP", "ESTAC?O:;SAO PAULO",
            "CODIGO (WMO):;A701", "LATITUDE:;-23,5", "LONGITUDE:;-46,6",
            "ALTITUDE:;700", "DATA DE FUNDAC?O:;2000-01-01",
        ]
        with tempfile.TemporaryDirectory(prefix="test_inmet_2019_") as tmp:
            caminho = Path(tmp) / "2019.csv"
            caminho.write_text("\n".join(linhas), encoding="ascii")
            metadados = extrair_metadados(caminho)
            self.assertEqual(metadados["REGIAO:"], "SE")
            self.assertEqual(metadados["ESTACAO:"], "SAO PAULO")


if __name__ == "__main__":
    unittest.main()
