"""Casos sintéticos da Etapa 1; dados municipais, sem normalização populacional."""
import sys
import unittest
from datetime import date, timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession, functions as F
from src.consumption.dashboard_analytics import _adicionar_metricas, _contraste, _resumo, criar_cid_detalhe, criar_demografia_detalhe, criar_estratos_detalhe
from src.quality.consumption.fq_checks import validar_detalhe
from src.consumption.fq_bootstrap import intervalo_municipal, aplicar_bootstrap_recorte


class FrioQuenteRevisionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master('local[2]').appName('TCC-Revisao-Frio-Quente-Testes').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def metricas(self, valores):
        return _adicionar_metricas(self.spark.createDataFrame(
            [(f'M{i}',int(f*100),int(q*100),100,100,int((f+q)*100)) for i,(f,q) in enumerate(valores)],
            'codigo_municipio string, internacoes_frio long, internacoes_quente long, dias_frios long, dias_quentes long, internacoes_total long'))

    def caso(self,f,q,d,r,direcao):
        df = self.metricas([(f,q)]); v=df.first()
        self.assertAlmostEqual(v.diferenca_absoluta,d)
        self.assertAlmostEqual(v.diferenca_por_100_municipio_dias,100*d)
        if r is None:self.assertIsNone(v.diferenca_relativa)
        else:self.assertAlmostEqual(v.diferenca_relativa,r)
        self.assertEqual(v.direcao_diferenca,direcao)
        self.assertEqual(v.media_frio_zero,f==0)
        self.assertEqual(v.media_quente_zero,q==0)
        self.assertEqual(v.ambas_medias_zero,f==0 and q==0)
        validar_detalhe(df,['codigo_municipio'],'sintetico')

    def test_01_comum(self):self.caso(10,8,2,.25,'FRIO')
    def test_02_frio_zero(self):self.caso(0,2,-2,-1,'QUENTE')
    def test_03_quente_zero(self):self.caso(2,0,2,None,'FRIO')
    def test_04_ambos_zero(self):self.caso(0,0,0,None,'IGUAL')
    def test_05_percentual_grande_magnitude_pequena(self):self.caso(.02,.01,.01,1,'FRIO')

    def test_06_bases_separadas_sem_perder_zeros(self):
        v=_resumo(self.metricas([(0,0),(2,0),(0,2)]),[]).first()
        self.assertEqual(v.municipios_elegiveis,3)
        self.assertEqual(v.municipios_validos_absoluto,3)
        self.assertEqual(v.municipios_validos,1)
        self.assertEqual(v.mediana_diferenca_absoluta_elegiveis,0)
        self.assertEqual(v.mediana_diferenca_relativa,-1)
        self.assertEqual(v.municipios_quente_zero,2)
        self.assertEqual(v.municipios_ambas_medias_zero,1)

    def test_07_mediana_das_diferencas_nao_diferenca_das_medianas(self):
        v=_resumo(self.metricas([(0,5),(6,1),(10,20)]),[]).first()
        self.assertEqual(v.mediana_diferenca_absoluta_elegiveis,-5)
        self.assertEqual(v.mediana_media_frio_elegiveis-v.mediana_media_quente_elegiveis,1)

    def test_08_municipio_grande_nao_domina_mediana(self):
        v=_resumo(self.metricas([(2,1),(3,1),(4,1),(1000,1)]),[]).first()
        self.assertEqual(v.mediana_diferenca_absoluta_elegiveis,2)
        self.assertEqual(v.mediana_diferenca_por_100,200)

    def test_09_threshold_consistencia_inclusivo(self):
        df=self.metricas([(101,100),(99,100),(102,100),(98,100)])
        v=_resumo(df,[]).first()
        self.assertEqual(v.municipios_pequena_consistencia,2)
        self.assertEqual(v.municipios_frio_consistencia,1)
        self.assertEqual(v.municipios_quente_consistencia,1)
        self.assertEqual(v.percentual_pequena_consistencia,50)

    def fixture_esparsa(self):
        fato=self.spark.createDataFrame([(m,date(2024,1,1)+timedelta(days=i),10. if i<30 else 30.,0)
            for m in ('M1','M2') for i in range(60)],'codigo_municipio string, data date, temperatura_media_dia_c double, qtd_internacoes long')
        dim=self.spark.createDataFrame([('10','Capítulo X - Respiratórias','J09-J18','Pneumonia','J18','Pneumonia não especificada')],
            'codigo_capitulo string, descricao_capitulo string, codigo_grupo string, descricao_grupo string, codigo_categoria string, descricao_categoria string')
        eventos=self.spark.createDataFrame([('M1','10','J09-J18','J18','0-17','1','Masculino',10.,3),('M1','10','J09-J18','J18','0-17','1','Masculino',30.,1)],
            'codigo_municipio string, codigo_capitulo string, codigo_grupo string, codigo_categoria string, faixa_etaria string, sexo string, sexo_descricao string, temperatura_media_dia_c double, qtd_internacoes long')
        return fato,dim,eventos

    def test_10_cid_esparsa_preserva_dias_sem_evento_e_municipios_zero(self):
        fato,dim,eventos=self.fixture_esparsa()
        df=criar_cid_detalhe(fato,eventos,dim)
        v=df.filter("codigo_municipio='M1' AND chave_alvo='CATEGORIA_J18'").first()
        self.assertEqual((v.dias_frios,v.dias_quentes),(30,30))
        self.assertAlmostEqual(v.media_frio_municipio,3/30)
        z=df.filter("codigo_municipio='M2' AND chave_alvo='CATEGORIA_J18'").first()
        self.assertTrue(z.ambas_medias_zero)
        self.assertEqual(z.contraste_absoluto_valido,1)
        self.assertEqual(df.count(),6)

    def test_11_demografia_preserva_estratos_zero(self):
        fato,dim,eventos=self.fixture_esparsa()
        df=criar_demografia_detalhe(self.spark,fato,eventos,dim)
        self.assertEqual(df.count(),80)
        v=df.filter("codigo_municipio='M2' AND chave_alvo='CATEGORIA_J18' AND sexo='3' AND faixa_etaria='80+'").first()
        self.assertTrue(v.ambas_medias_zero)
        self.assertEqual((v.dias_frios,v.dias_quentes),(30,30))

    def test_12_marginais_nao_somam_medianas(self):
        fato,dim,eventos=self.fixture_esparsa()
        df=criar_estratos_detalhe(self.spark,fato,eventos,dim)
        self.assertEqual(df.count(),56)
        v=df.filter("codigo_municipio='M1' AND chave_alvo='TOTAL' AND nivel_demografico='SEXO' AND sexo='1'").first()
        self.assertAlmostEqual(v.media_frio_municipio,.1)
        self.assertAlmostEqual(v.media_quente_municipio,1/30)

    def test_13_dq_rejeita_dado_corrompido(self):
        df=self.metricas([(10,8)]).withColumn('diferenca_por_100_municipio_dias',F.lit(999.))
        with self.assertRaises(ValueError):validar_detalhe(df,['codigo_municipio'],'corrompido')

    def test_14_sem_dias_nao_e_media_zero(self):
        df=self.metricas([(0,2)]).withColumn('dias_frios',F.lit(0))
        v=_adicionar_metricas(df).first()
        self.assertIsNone(v.media_frio_municipio)
        self.assertIsNone(v.media_frio_zero)
        self.assertEqual(v.direcao_diferenca,'NAO_APLICAVEL')

    def test_15_bootstrap_reproduzivel_e_reamostra_municipios(self):
        a=intervalo_municipal([-2.,0.,1.,8.],'TESTE',replicas=1000)
        self.assertEqual(a,intervalo_municipal([-2.,0.,1.,8.],'TESTE',replicas=1000))
        self.assertLessEqual(a[0],a[1])
        self.assertEqual(intervalo_municipal([2.,2.,2.],'CONSTANTE'),(2.,2.))
        with self.assertRaises(ValueError):intervalo_municipal([float('nan')],'INVALIDO')

    def test_16_bootstrap_demografico_materializa_apenas_escopo_declarado(self):
        detalhe=self.metricas([(2,1),(2,1)]).withColumn('chave_alvo',F.lit('TOTAL'))
        fora=self.metricas([(2,1),(2,1)]).withColumn('chave_alvo',F.lit('CATEGORIA_RARA'))
        detalhe=detalhe.unionByName(fora)
        resumo=_resumo(detalhe,['chave_alvo'])
        rows={r.chave_alvo:r for r in aplicar_bootstrap_recorte(self.spark,'demografia',detalhe,resumo,['chave_alvo']).collect()}
        self.assertEqual(rows['TOTAL'].bootstrap_replicas,1000)
        self.assertEqual(rows['TOTAL'].bootstrap_seed,20261005)
        self.assertEqual(rows['TOTAL'].ic95_diferenca_por_100_inferior,100)
        self.assertEqual(rows['TOTAL'].ic95_diferenca_por_100_superior,100)
        self.assertIsNone(rows['CATEGORIA_RARA'].ic95_diferenca_por_100_inferior)
        self.assertEqual(rows['CATEGORIA_RARA'].bootstrap_status,'NAO_CALCULADO_FORA_ESCOPO')


if __name__=='__main__':unittest.main()
