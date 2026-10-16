"""Integração da regra térmica promovida, lags e semântica oficial."""
import sys,unittest
from pathlib import Path
from datetime import date,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pyspark.sql import SparkSession,functions as F
from src.consumption.metodologia_termica import calcular_thresholds,classificar
from src.consumption.promocao_metodologia import identificar,classificacao_oficial,anexar_classificacao_lag,contraste_classificado,criar_lags_principais
from src.consumption.dashboard_analytics import _resumo

class PromotionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=SparkSession.builder.master('local[2]').appName('Tests-Etapa1C').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def base(self):
        rows=[('M',date(2015,1,1)+timedelta(days=i),float(i%10),1,10 if i%10<2 else 1) for i in range(200)]
        rows+=[('M',date(2020,1,1)+timedelta(days=i),30.+i%10,1,3) for i in range(200)]
        return self.spark.createDataFrame(rows,'codigo_municipio string,data date,temperatura_media_dia_c double,meteorologia_disponivel int,qtd_internacoes long')
    def classified(self,df=None):
        d=self.base() if df is None else df
        return classificar(d,calcular_thresholds(d),calcular_thresholds(d,('codigo_municipio','mes')))
    def fixed(self):
        b=self.base();th=self.spark.createDataFrame([('M',2.,8.,False)],'codigo_municipio string,p20 double,p80 double,threshold_degenerado boolean')
        return classificar(b,th,calcular_thresholds(b,('codigo_municipio','mes')))
    def test_01_baseline_exclui_2014(self):
        b=self.base();extra=self.spark.createDataFrame([('M',date(2014,12,31),-999.,1,999)],b.schema)
        a=calcular_thresholds(b).first();x=calcular_thresholds(b.union(extra)).first()
        self.assertEqual((a.p20,a.p80),(x.p20,x.p80));self.assertEqual(x.dias_temperatura_valida,400)
    def test_02_filtrar_2020_nao_recalibra(self):
        th=calcular_thresholds(self.base()).first();cl=self.classified().filter('ano=2020').first()
        self.assertEqual((th.p20,th.p80),(cl.p20,cl.p80));self.assertGreater(calcular_thresholds(self.base().filter('year(data)=2020')).first().p20,th.p20)
    def test_03_p20_inclusivo(self):self.assertEqual(self.fixed().filter('temperatura_media_dia_c=2').first().classe_B,'FRIO')
    def test_04_p80_inclusivo(self):self.assertEqual(self.fixed().filter('temperatura_media_dia_c=8').first().classe_B,'QUENTE')
    def test_05_intermediario(self):self.assertEqual(self.fixed().filter('temperatura_media_dia_c=5').first().classe_B,'INTERMEDIARIO')
    def test_06_empates_deterministicos(self):
        d=self.fixed();self.assertEqual(d.filter('temperatura_media_dia_c=2 AND classe_B="FRIO"').count(),20)
        self.assertEqual({(r.data,r.classe_B) for r in d.collect()},{(r.data,r.classe_B) for r in d.repartition(3).collect()})
    def test_07_d100_apos_media_municipal(self):
        d=contraste_classificado(self.classified(),['codigo_municipio']).first()
        self.assertAlmostEqual(d.diferenca_por_100_municipio_dias,100*(d.internacoes_frio/d.dias_frios-d.internacoes_quente/d.dias_quentes))
    def test_08_mediana_entre_municipios(self):
        a=self.classified();b=a.withColumn('codigo_municipio',F.lit('N')).withColumn('qtd_internacoes',F.lit(999).cast('long'))
        d=contraste_classificado(a.unionByName(b),['codigo_municipio']);vals=sorted(r.diferenca_por_100_municipio_dias for r in d.collect())
        self.assertAlmostEqual(_resumo(d,[]).first().mediana_diferenca_por_100,vals[0])
    def test_09_A_sensibilidade_preservada(self):
        d=identificar(contraste_classificado(self.classified(),['codigo_municipio'],'classe_A'),'A','SENSIBILIDADE_ABSOLUTA').first()
        self.assertEqual(d.metodo_termico,'ABSOLUTO_20_25');self.assertEqual(d.papel_metodologico,'SENSIBILIDADE_ABSOLUTA')
    def test_10_C_somente_sensibilidade(self):
        d=identificar(self.classified(),'C','SENSIBILIDADE_SAZONAL').first()
        self.assertEqual(d.metodo_termico,'P20_P80_MUNICIPIO_MES');self.assertEqual(d.papel_metodologico,'SENSIBILIDADE_SAZONAL')
    def test_11_terminologia_oficial(self):
        d=classificacao_oficial(self.fixed()).filter('temperatura_media_dia_c=2').first()
        self.assertEqual(d.classificacao_termica,'RELATIVAMENTE_FRIO');self.assertEqual(d.metrica_principal,'D100')
    def lagfixture(self):
        return self.spark.createDataFrame([('M',date(2015,1,1),1.,True,2.,8.,'FRIO',9.),('M',date(2015,1,2),9.,True,2.,8.,'QUENTE',999.),('M',date(2015,1,4),5.,True,2.,8.,'INTERMEDIARIO',1.)],
            'codigo_municipio string,data date,temperatura_media_dia_c double,temperatura_valida boolean,p20 double,p80 double,classe_B string,temperatura_media_lag_1d double')
    def test_12_lag_data_exata_sem_linha_anterior(self):
        d={r.data:r for r in anexar_classificacao_lag(self.lagfixture(),1).collect()}
        self.assertEqual(d[date(2015,1,2)].classe_lag,'FRIO');self.assertEqual(d[date(2015,1,4)].classe_lag,'INVALIDO')
    def test_13_lag_tecnico_2014_so_como_exposicao(self):
        d=anexar_classificacao_lag(self.lagfixture(),1).filter("data='2015-01-01'").first()
        self.assertEqual(d.data_exposicao,date(2014,12,31));self.assertEqual(d.classe_lag,'QUENTE');self.assertEqual(d.p20,2.)
    def test_14_lag_zero_reutiliza_classe_principal(self):
        for r in anexar_classificacao_lag(self.lagfixture(),0).collect():self.assertEqual(r.classe_lag,r.classe_B)
    def test_15_degenerado_producao_intermediario(self):
        b=self.base().withColumn('temperatura_media_dia_c',F.lit(7.));d=classificacao_oficial(self.classified(b))
        self.assertEqual(d.filter("classificacao_termica!='INTERMEDIARIO'").count(),0)
    def test_16_base_magnitude_nao_usa_denominador_relativo(self):
        b=self.base().withColumn('qtd_internacoes',F.lit(0).cast('long'));d=contraste_classificado(self.classified(b),['codigo_municipio'])
        r=identificar(_resumo(d,[])).first();self.assertEqual(r.municipios_validos_magnitude,1);self.assertEqual(r.municipios_validos,0);self.assertEqual(r.mediana_diferenca_por_100,0.)
    def test_17_D0_respiratorio_reconcilia_media_e_zeros(self):
        rows=[('M',date(2015,1,1)+timedelta(days=i),1. if i<30 else 9.,1,5 if i<30 else 1) for i in range(60)]
        b=self.spark.createDataFrame(rows,'codigo_municipio string,data date,temperatura_media_dia_c double,meteorologia_disponivel int,qtd_internacoes long')
        cl=self.classified(b).withColumn('municipio_dia_id',F.concat(F.col('codigo_municipio'),F.col('data')))
        for n in [1,3,7]:cl=cl.withColumn('temperatura_media_lag_'+str(n)+'d',F.lit(None).cast('double'))
        ev=cl.select('codigo_municipio','municipio_dia_id','qtd_internacoes').withColumn('codigo_capitulo',F.lit('10')).withColumn('codigo_grupo',F.lit('J09-J18'))
        d0=criar_lags_principais(self.spark,cl,ev).filter("defasagem='D0'")
        expected=_resumo(contraste_classificado(cl,['codigo_municipio']),[]).first()
        for k in ['TOTAL','CAPITULO_10','GRUPO_J09-J18']:
            r=d0.filter(F.col('chave_alvo')==k).first()
            self.assertAlmostEqual(r.mediana_diferenca_por_100,expected.mediana_diferenca_por_100)
            self.assertEqual(r.municipios_validos_magnitude,1)
            self.assertEqual(r.internacoes_total,sum(x[4] for x in rows))
        z=d0.filter("chave_alvo='GRUPO_J20-J22'").first()
        self.assertEqual(z.mediana_diferenca_por_100,0.);self.assertEqual(z.municipios_validos,0)

if __name__=='__main__':unittest.main()
