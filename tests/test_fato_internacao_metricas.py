"""Equivalência do bloco 18, sem importar/executar o pipeline real.

spark-submit --master local[2] tests/test_fato_internacao_metricas.py -v
"""

import ast
import contextlib
import io
import unittest
from pathlib import Path

from pyspark.sql import SparkSession, functions as F, types as T


ARQUIVO = Path(__file__).resolve().parents[1] / "src/gold/fato_internacao.py"
CAMPOS = [
    "ano", "mes", "CD_MUN", "NM_MUN", "SIGLA_UF", "codigo_cid",
    "descricao_cid", "codigo_categoria", "descricao_categoria",
    "descricao_grupo", "codigo_capitulo", "descricao_capitulo",
]


def carregar_bloco():
    # O script executa I/O no topo. Compilamos somente seus imports e o
    # bloco real de validação, evitando duplicar a agregação de produção.
    fonte = ARQUIVO.read_text(encoding="utf-8")
    inicio = fonte.index("# 18. VALIDAÇÃO DE NULOS")
    fim = fonte.index("# 19. VALIDAÇÃO DE DISTÂNCIA", inicio)
    bloco = ast.parse(fonte[inicio:fim], filename=str(ARQUIVO))
    imports = [node for node in ast.parse(fonte).body
               if isinstance(node, (ast.Import, ast.ImportFrom))]
    modulo = ast.Module(body=imports + bloco.body, type_ignores=[])
    return compile(modulo, str(ARQUIVO), "exec"), bloco


class MetricasCamposImportantesTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("Teste-Metricas-Fato-Internacao")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.codigo, cls.bloco = carregar_bloco()
        cls.schema = T.StructType([
            T.StructField(campo, T.IntegerType() if campo == "ano"
                          else T.LongType() if campo == "mes"
                          else T.StringType(), True)
            for campo in CAMPOS
        ])
        cls.valida = [2023, 1] + ["valor válido"] * 10

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def comparar(self, linhas, schema=None, esperadas=None):
        df = self.spark.createDataFrame(linhas, schema or self.schema)
        # Referência independente: condição e ações do bloco anterior.
        antigas = {
            campo: df.filter(
                F.col(campo).isNull()
                | (F.trim(F.col(campo).cast("string")) == "")
            ).count()
            for campo in CAMPOS
        }
        if esperadas is not None:
            self.assertEqual(antigas, esperadas)
        namespace = {"df_gold": df}
        saida = io.StringIO()
        erro = None
        with contextlib.redirect_stdout(saida):
            try:
                exec(self.codigo, namespace)
            except ValueError as exc:
                erro = exc
        self.assertEqual(namespace["campos_importantes"], CAMPOS)
        self.assertEqual(namespace["metricas_campos_importantes"].asDict(), antigas)
        self.assertIs(namespace["df_gold"], df)
        self.assertEqual(df.schema, schema or self.schema)

        logs = []
        primeiro_invalido = None
        for campo in CAMPOS:
            logs.append(f"{campo}: {antigas[campo]} nulos/vazios")
            if antigas[campo] > 0:
                primeiro_invalido = campo
                break
        linhas_log = saida.getvalue().splitlines()
        self.assertEqual(linhas_log[:2], ["", "Validando campos importantes..."])
        self.assertEqual(linhas_log[2:2 + len(logs)], logs)
        if primeiro_invalido:
            self.assertIsInstance(erro, ValueError)
            self.assertEqual(str(erro),
                             f"O campo {primeiro_invalido} possui valores nulos/vazios.")
            self.assertEqual(len(linhas_log), 2 + len(logs))
        else:
            self.assertIsNone(erro)
            self.assertEqual(len(linhas_log), 3 + len(logs))
            self.assertRegex(linhas_log[-1],
                             r"^\[PERFORMANCE\] Validação consolidada de campos importantes: \d+\.\d{3} segundos$")

    def test_validos_e_trim_sem_ampliar_regra(self):
        # Zero, negativos, tab e newline não viram vazios pelo trim original.
        linhas = [self.valida, [0, -1] + ["  válido  "] * 10,
                  [2025, 12] + ["\t", "\n"] * 5]
        self.comparar(linhas, esperadas=dict.fromkeys(CAMPOS, 0))

    def test_nulos_vazios_espacos_e_multiplos_invalidos(self):
        linhas = [self.valida, [None] * 12,
                  [2024, 2] + [""] * 10, [2024, 3] + ["   "] * 10]
        # Contagens diferentes por coluna detectam aliases trocados.
        for indice in range(12):
            linha = self.valida.copy()
            linha[indice] = None
            linhas.extend([linha] * indice)
        esperadas = {campo: (1 if i < 2 else 3) + i
                     for i, campo in enumerate(CAMPOS)}
        self.comparar(linhas, esperadas=esperadas)

    def test_dataframe_vazio(self):
        self.comparar([], esperadas=dict.fromkeys(CAMPOS, 0))

    def test_primeiro_erro_apos_colunas_validas(self):
        linha = self.valida.copy()
        linha[3], linha[6] = "   ", None
        esperadas = dict.fromkeys(CAMPOS, 0)
        esperadas.update(NM_MUN=1, descricao_cid=1)
        self.comparar([linha], esperadas=esperadas)

    def test_cast_preservado_em_colunas_textuais(self):
        schema = T.StructType([T.StructField(c, T.StringType(), True) for c in CAMPOS])
        self.comparar([["2023"] * 12, [""] * 12, ["   "] * 12],
                      schema=schema, esperadas=dict.fromkeys(CAMPOS, 2))

    def test_uma_agregacao_e_uma_action(self):
        chamadas = [node.func.attr for node in ast.walk(self.bloco)
                    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)]
        self.assertEqual(chamadas.count("agg"), 1)
        self.assertEqual(chamadas.count("first"), 1)
        for proibida in ("filter", "count", "collect", "cache", "persist"):
            self.assertNotIn(proibida, chamadas)


if __name__ == "__main__":
    unittest.main()
