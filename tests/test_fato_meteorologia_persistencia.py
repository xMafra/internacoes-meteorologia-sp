"""Teste sintético do fluxo real, sem executar leituras/escritas do pipeline.

spark-submit --master local[2] tests/test_fato_meteorologia_persistencia.py -v
"""

import ast
import contextlib
import copy
import io
import unittest
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, types as T


ARQUIVO = Path(__file__).resolve().parents[1] / "src/gold/fato_internacao_meteorologia.py"


def possui_chamada(node, metodo):
    return any(isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
               and n.func.attr == metodo for n in ast.walk(node))


def carregar_fluxo(persistir):
    fonte = ARQUIVO.read_text(encoding="utf-8")
    arvore = ast.parse(fonte)
    imports_helper = [n for n in arvore.body
                      if isinstance(n, (ast.Import, ast.ImportFrom))
                      or isinstance(n, ast.FunctionDef) and n.name == "validar_colunas"]
    def trecho(inicio, fim):
        return ast.parse(fonte[fonte.index(inicio):fonte.index(fim)]).body
    # A preparação da fato, métricas iniciais e todo o fluxo até a escrita
    # vêm da produção. Somente os DataFrames de entrada são sintéticos.
    corpo = trecho("# 2. PREPARAÇÃO DA CHAVE DA FATO", "# 2.1.")
    corpo += trecho("colunas_inmet_obrigatorias =", "# 15. GRAVAÇÃO")
    if not persistir:
        corpo = [n for n in corpo if not possui_chamada(n, "persist")]
    liberacao = trecho("fim_reutilizacao_inmet =", "# 16. VALIDAÇÃO PÓS-GRAVAÇÃO")
    return (compile(ast.Module(body=imports_helper + corpo, type_ignores=[]), str(ARQUIVO), "exec"),
            compile(ast.Module(body=liberacao, type_ignores=[]), str(ARQUIVO), "exec"))


class PersistenciaInmetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]")
                     .appName("Teste-Persistencia-INMET")
                     .config("spark.ui.enabled", "false").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.fluxos = {p: carregar_fluxo(p) for p in (False, True)}
        cls.schema_inmet = T.StructType(
            [T.StructField("data", T.StringType()),
             T.StructField("codigo_estacao", T.StringType())]
            + [T.StructField(c, T.LongType()) for c in
               ("qtd_observacoes", "qtd_obs_precipitacao", "qtd_obs_temperatura",
                "qtd_obs_umidade", "qtd_obs_vento")]
            + [T.StructField(c, T.DoubleType()) for c in
               ("precipitacao_dia_mm", "temperatura_media_dia_c", "temperatura_minima_dia_c",
                "temperatura_maxima_dia_c", "umidade_media_dia_pct", "umidade_minima_dia_pct",
                "umidade_maxima_dia_pct", "vento_medio_dia_ms", "vento_maximo_dia_ms")])
        cls.inmet = []
        for data in ("2023-01-01", "2024-06-15", "2025-12-31"):
            linha = {f.name: 20.0 for f in cls.schema_inmet.fields}
            linha.update(data=data, codigo_estacao=" a001 ", qtd_observacoes=24,
                         qtd_obs_precipitacao=24, qtd_obs_temperatura=24,
                         qtd_obs_umidade=24, qtd_obs_vento=24)
            cls.inmet.append(linha)
        cls.inmet[1].update(temperatura_media_dia_c=None, qtd_obs_temperatura=0)
        cls.fato = [(i, 2023, 1, data, "A001", "3550308", "São Paulo", "A000")
                    for i, data in enumerate(("2022-12-31", "2023-01-01", "2023-01-01",
                                              "2024-06-15", "2026-01-01"))]
        cls.schema_fato = "id long, ano int, mes int, data_internacao string, codigo_estacao string, CD_MUN string, NM_MUN string, codigo_cid string"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def executar(self, persistir, inmet, fato):
        df_fato = self.spark.createDataFrame(fato, self.schema_fato)
        namespace = dict(df_inmet=self.spark.createDataFrame(inmet, self.schema_inmet),
                         df_fato=df_fato, total_fato=len(fato), fato_fora_periodo=2,
                         DATA_INICIO="2023-01-01", DATA_FIM="2025-12-31")
        codigo, liberar = self.fluxos[persistir]
        logs = io.StringIO()
        try:
            with contextlib.redirect_stdout(logs):
                try:
                    exec(codigo, namespace)
                except ValueError as erro:
                    return {"erro": str(erro)}
                df = namespace["df_integrada"]
                # Última action sintética ocupa o lugar da escrita real.
                linhas = sorted(df.collect(), key=lambda r: r.id)
                self.assertFalse(namespace["df_fato"].is_cached)
                self.assertEqual(namespace["df_inmet"].is_cached, persistir)
                if persistir:
                    self.assertEqual(namespace["df_inmet"].storageLevel, StorageLevel.MEMORY_AND_DISK)
                    self.assertIn("InMemoryTableScan", df._jdf.queryExecution().executedPlan().toString())
                    exec(liberar, namespace)
                    self.assertFalse(namespace["df_inmet"].is_cached)
                metricas = {k: namespace[k] for k in
                            ("total_inmet", "total_estacoes_inmet", "chaves_inmet_invalidas",
                             "duplicidades_inmet", "datas_inmet", "qtd_fato_sem_inmet",
                             "qtd_inmet_sem_fato", "estatisticas_join", "dias_inmet_incompletos")}
                return dict(metricas=metricas, schema=df.schema.json(), linhas=linhas,
                            logs=[l for l in logs.getvalue().splitlines()
                                  if not l.startswith("[PERFORMANCE]")])
        finally:
            # Limpeza do teste inclusive nos cenários de reprovação esperada.
            namespace["df_inmet"].unpersist(blocking=True)

    def test_equivalencia_metricas_join_flags_e_granularidade(self):
        antes = self.executar(False, self.inmet, self.fato)
        depois = self.executar(True, self.inmet, self.fato)
        self.assertEqual(antes, depois)
        self.assertEqual(depois["metricas"]["total_inmet"], 3)
        self.assertEqual(depois["metricas"]["total_estacoes_inmet"], 1)
        self.assertEqual(depois["metricas"]["duplicidades_inmet"], 0)
        self.assertEqual(str(depois["metricas"]["datas_inmet"].data_min), "2023-01-01")
        self.assertEqual(str(depois["metricas"]["datas_inmet"].data_max), "2025-12-31")
        self.assertEqual(depois["metricas"]["estatisticas_join"].total, len(self.fato))
        self.assertEqual(depois["metricas"]["estatisticas_join"].com_match, 3)
        self.assertEqual(depois["metricas"]["estatisticas_join"].sem_match, 2)
        self.assertEqual(depois["linhas"][0].motivo_sem_meteorologia, "DATA_ANTERIOR_AO_PERIODO_INMET")
        self.assertEqual(depois["linhas"][-1].motivo_sem_meteorologia, "DATA_POSTERIOR_AO_PERIODO_INMET")
        self.assertEqual([r.meteorologia_disponivel for r in depois["linhas"]], [0, 1, 1, 1, 0])
        self.assertIsNone(depois["linhas"][3].temperatura_media_dia_c)

    def test_reprovacoes_preservadas(self):
        casos = []
        invalida = copy.deepcopy(self.inmet)
        invalida[0]["codigo_estacao"] = None
        casos.append((invalida, "chave inválida"))
        casos.append((self.inmet + [self.inmet[0]], "duplicidades"))
        casos.append((self.inmet[1:], "diferente de 2023"))
        casos.append(([self.inmet[0], self.inmet[2]], "sem correspondência estação + dia"))
        incompleta = copy.deepcopy(self.inmet)
        incompleta[0]["qtd_observacoes"] = 23
        casos.append((incompleta, "diferente de 24"))
        for inmet, mensagem in casos:
            with self.subTest(mensagem=mensagem):
                antes = self.executar(False, inmet, self.fato)
                depois = self.executar(True, inmet, self.fato)
                self.assertEqual(antes, depois)
                self.assertIn(mensagem, depois["erro"])

    def test_escopo_e_ordem_da_liberacao(self):
        arvore = ast.parse(ARQUIVO.read_text(encoding="utf-8"))
        chamadas = [n for n in ast.walk(arvore) if isinstance(n, ast.Call)
                    and isinstance(n.func, ast.Attribute)]
        persistencias = [n for n in chamadas if n.func.attr in ("persist", "cache")]
        liberacoes = [n for n in chamadas if n.func.attr == "unpersist"]
        self.assertEqual(len(persistencias), 1)
        self.assertEqual(len(liberacoes), 1)
        self.assertEqual(ast.unparse(persistencias[0]), "df_inmet.persist(StorageLevel.MEMORY_AND_DISK)")
        self.assertEqual(ast.unparse(liberacoes[0]), "df_inmet.unpersist()")
        escrita = next(n for n in chamadas if n.func.attr == "parquet" and possui_chamada(n, "partitionBy"))
        self.assertGreater(liberacoes[0].lineno, escrita.end_lineno)
        self.assertFalse(any(isinstance(n, ast.Name) and n.id in ("df_integrada", "df_inmet_join", "df_inmet")
                             and n.lineno > liberacoes[0].lineno for n in ast.walk(arvore)))


if __name__ == "__main__":
    unittest.main()
