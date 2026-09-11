"""Checks leves: python -B -m unittest discover -s tests -p test_bronze_orchestration.py."""

import contextlib
import io
import runpy
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path
from unittest.mock import call, patch
from zipfile import ZipFile

from src.orchestration import bronze as wrappers
from src.quality.bronze.common import Report


class BronzeTests(unittest.TestCase):
    def test_periodos(self):
        with patch('src.bronze.sih.download_sih', return_value=Path('fake')) as fn:
            self.assertEqual(len(wrappers.bronze_sih()), 36)
            self.assertEqual(fn.call_args_list, [call(a, m) for a in range(2023, 2026) for m in range(1, 13)])
        with patch('src.bronze.inmet.download_inmet', return_value=Path('fake')) as fn:
            wrappers.bronze_inmet()
            self.assertEqual(fn.call_args_list, [call(2023), call(2024), call(2025)])
        with patch('src.bronze.ibge.download_malha_sp_2022', return_value=Path('fake')) as fn:
            wrappers.bronze_ibge()
            fn.assert_called_once_with()

    def test_gates(self):
        params = {
            'sih': dict(ano_inicio=2023, mes_inicio=1, ano_fim=2025, mes_fim=12),
            'inmet': dict(ano_inicio=2023, ano_fim=2025),
            'ibge': dict(ano=2022), 'cid10': {},
        }
        for fonte, kwargs in params.items():
            for aprovado in (False, True):
                report = Report(fonte)
                report.add('simulado', aprovado, 'teste')
                with self.subTest(fonte=fonte, aprovado=aprovado), patch(
                    f'src.quality.bronze.dq_{fonte}.validar_{fonte}', return_value=report
                ) as fn, contextlib.redirect_stdout(io.StringIO()):
                    if aprovado:
                        getattr(wrappers, f'dq_bronze_{fonte}')()
                    else:
                        with self.assertRaises(ValueError):
                            getattr(wrappers, f'dq_bronze_{fonte}')()
                    fn.assert_called_once_with(**kwargs)

    def test_cid10_preserva_existentes_e_extrai_ausentes(self):
        from src.bronze.cid10 import ARQUIVOS_ESPERADOS

        with tempfile.TemporaryDirectory() as tmp:
            raiz = Path(tmp)
            arquivo = raiz / 'CID10CSV.zip'
            with ZipFile(arquivo, 'w') as z:
                for nome in ARQUIVOS_ESPERADOS:
                    z.writestr(nome, 'codigo;descricao\n1;teste\n')
            existente = raiz / ARQUIVOS_ESPERADOS[0]
            existente.write_bytes(b'preservar mesmo se invalido')
            with patch('src.bronze.cid10.obter_caminho_zip', return_value=arquivo), patch(
                'src.bronze.cid10.obter_diretorio_bronze', return_value=raiz
            ), patch('urllib.request.urlopen', side_effect=AssertionError('rede proibida')), patch(
                'urllib.request.urlretrieve', side_effect=AssertionError('rede proibida')
            ), contextlib.redirect_stdout(io.StringIO()):
                wrappers.bronze_cid10()
                self.assertEqual(existente.read_bytes(), b'preservar mesmo se invalido')
                self.assertTrue(all((raiz / n).is_file() for n in ARQUIVOS_ESPERADOS))
                with patch('src.bronze.cid10.extrair_cid10', side_effect=AssertionError('nao reextrair')):
                    wrappers.bronze_cid10()
                with patch('src.bronze.cid10.obter_caminho_zip', return_value=raiz / 'ausente.zip'):
                    with self.assertRaisesRegex(FileNotFoundError, 'ZIP CID-10'):
                        wrappers.bronze_cid10()

    def test_acervo_existente_sem_rede_ou_escrita(self):
        root = Path(__file__).resolve().parents[1] / 'data/bronze'
        expected = [root / 'sih' / str(a) / f'RDSP{a % 100:02d}{m:02d}.dbc'
                    for a in range(2023, 2026) for m in range(1, 13)]
        expected += [root / 'inmet' / str(a) / f'{a}.zip' for a in range(2023, 2026)]
        expected += [root / 'ibge/2022/SP_Municipios_2022.zip']
        from src.bronze.cid10 import ARQUIVOS_ESPERADOS
        expected += [root / 'cid10' / n for n in ['CID10CSV.zip', *ARQUIVOS_ESPERADOS]]
        if not all(p.is_file() for p in expected):
            self.skipTest('Acervo completo indisponivel; nenhum download sera realizado')
        before = {p: (p.stat().st_size, p.stat().st_mtime_ns) for p in expected}
        with patch('src.bronze.sih.urlretrieve', side_effect=AssertionError('rede')), patch(
            'src.bronze.inmet.urlopen', side_effect=AssertionError('rede')
        ), patch('src.bronze.ibge.urlopen', side_effect=AssertionError('rede')), patch(
            'src.bronze.cid10.extrair_cid10', side_effect=AssertionError('extracao')
        ), contextlib.redirect_stdout(io.StringIO()):
            wrappers.bronze_sih()
            wrappers.bronze_inmet()
            wrappers.bronze_ibge()
            wrappers.bronze_cid10()
        self.assertEqual(before, {p: (p.stat().st_size, p.stat().st_mtime_ns) for p in expected})

    def test_dag(self):
        with contextlib.ExitStack() as stack:
            for prefix in ('bronze_', 'dq_bronze_'):
                for fonte in ('sih', 'ibge', 'inmet', 'cid10'):
                    stack.enter_context(patch.object(wrappers, prefix + fonte, side_effect=AssertionError('I/O no parse')))
            dag = runpy.run_path('/opt/airflow/dags/tcc_pipeline.py')['dag']
        self.assertEqual(set(dag.task_group.children), {'sih', 'ibge', 'inmet', 'cid10', 'gold'})
        self.assertEqual(len(dag.tasks), 24)
        self.assertEqual(len(dag.topological_sort()), 24)
        expected = {}
        for fonte, retries, delay in [('sih', 3, 2), ('inmet', 2, 2), ('ibge', 2, 1), ('cid10', 0, None)]:
            b, q, s = [f'{fonte}.{p}_{fonte}' for p in ('bronze', 'dq_bronze', 'silver')]
            expected[b] = set()
            expected[q] = {b}
            expected[s] = {q} | ({'ibge.dq_bronze_ibge'} if fonte == 'sih' else set())
            expected[f'{fonte}.dq_silver_{fonte}'] = {s}
            self.assertEqual(dag.get_task(b).retries, retries)
            if delay:
                self.assertEqual(dag.get_task(b).retry_delay, timedelta(minutes=delay))
            self.assertEqual(dag.get_task(q).retries, 0)
        expected.update({
            'inmet.silver_inmet_diario': {'inmet.dq_silver_inmet'},
            'inmet.dq_silver_inmet_diario': {'inmet.silver_inmet_diario'},
            'gold.gold_municipio_estacao': {'ibge.dq_silver_ibge', 'inmet.dq_silver_inmet'},
            'gold.dq_gold_municipio_estacao': {'gold.gold_municipio_estacao'},
            'gold.gold_fato_internacao': {'sih.dq_silver_sih', 'ibge.dq_silver_ibge', 'cid10.dq_silver_cid10', 'gold.dq_gold_municipio_estacao'},
            'gold.dq_gold_fato_internacao': {'gold.gold_fato_internacao'},
            'gold.gold_fato_internacao_meteorologia': {'gold.dq_gold_fato_internacao', 'inmet.dq_silver_inmet_diario'},
            'gold.dq_gold_fato_internacao_meteorologia': {'gold.gold_fato_internacao_meteorologia'},
        })
        for t in dag.tasks:
            self.assertEqual(t.upstream_task_ids, expected[t.task_id], t.task_id)
            self.assertEqual(t.downstream_task_ids, {n for n, parents in expected.items() if t.task_id in parents})
            self.assertEqual(t.trigger_rule, 'all_success')
            if 'bronze' not in t.task_id:
                self.assertEqual(t.retries, 0 if '.dq_' in t.task_id else 1)
                self.assertEqual(t._conn_id, 'spark_default')


if __name__ == '__main__':
    unittest.main()
