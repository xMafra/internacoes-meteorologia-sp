"""Etapa 1B: percentis, empates, cobertura e comparação experimental."""
import unittest,sys
from pathlib import Path
from datetime import date,timedelta
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
sys.path.insert(0,'/home/jovyan/work')
from pyspark.sql import SparkSession,functions as F
from src.validation.auditoria_definicao_temperatura import (painel_periodo,calcular_thresholds,temperatura_valida,classificar,
    criar_exposicao,metricas_geral,detalhe_cid,comparar_resumos,ranks_cid,salvar,validar_thresholds,validar_classificacao,municipios_representativos,rotular_demografia)
from src.consumption.dashboard_analytics import _adicionar_metricas,_resumo


class TemperatureDefinitionAuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark=SparkSession.builder.master('local[2]').appName('TCC-Testes-Definicao-Temperatura').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','2').getOrCreate()
        cls.spark.sparkContext.setLogLevel('ERROR')
    @classmethod
    def tearDownClass(cls):cls.spark.stop()
    def painel(self,temps,mun='M1'):
        return self.spark.createDataFrame([(mun,date(2015,1,1)+timedelta(days=i),float(v) if v is not None else None,1,1)
            for i,v in enumerate(temps)],'codigo_municipio string, data date, temperatura_media_dia_c double, meteorologia_disponivel int, qtd_internacoes long')
    def classes(self,df):return classificar(df,calcular_thresholds(df),calcular_thresholds(df,('codigo_municipio','mes')))

    def test_01_exclui_periodo_tecnico_2014(self):
        df=self.painel(range(100)).unionByName(self.spark.createDataFrame([('M1',date(2014,12,25),999.,1,1)],self.painel([1]).schema))
        self.assertEqual(painel_periodo(df).count(),100)
        self.assertEqual(calcular_thresholds(df).first().temperatura_maxima,99)
    def test_02_percentil_exato_com_interpolacao(self):
        r=calcular_thresholds(self.painel(range(100))).first()
        self.assertAlmostEqual(r.p20,19.8);self.assertAlmostEqual(r.p80,79.2)
        self.assertEqual(r.dias_temperatura_valida,100)
    def test_03_percentis_ordenados_e_dentro_intervalo(self):
        df=calcular_thresholds(self.painel(range(100)));validar_thresholds(df,['codigo_municipio'])
        r=df.first();self.assertLessEqual(r.temperatura_minima,r.p05);self.assertLessEqual(r.p95,r.temperatura_maxima)
    def test_04_exclui_null_nan_infinito_e_flag_invalido(self):
        df=self.painel([None,float('nan'),float('inf'),float('-inf'),10.,20.])
        df=df.withColumn('meteorologia_disponivel',F.when(F.col('temperatura_media_dia_c')==20,0).otherwise(1))
        self.assertEqual(df.filter(temperatura_valida(df)).count(),1)
        self.assertEqual(calcular_thresholds(df).first().dias_temperatura_valida,1)
    def test_05_limites_absolutos_e_relativos_inclusivos(self):
        df=self.painel([18,20,25,28]);th=self.spark.createDataFrame([('M1',18.,28.,False)],'codigo_municipio string,p20 double,p80 double,threshold_degenerado boolean')
        mt=self.spark.createDataFrame([('M1',1,18.,28.,False)],'codigo_municipio string,mes int,p20 double,p80 double,threshold_degenerado boolean')
        rows={r.temperatura_media_dia_c:r for r in classificar(df,th,mt).collect()}
        self.assertEqual(rows[20].classe_A,'INTERMEDIARIO');self.assertEqual(rows[25].classe_A,'QUENTE')
        self.assertEqual(rows[18].classe_B,'FRIO');self.assertEqual(rows[28].classe_B,'QUENTE')
    def test_06_empates_nao_sao_removidos_para_forcar_20(self):
        df=self.painel([10]*5+[20,30,40,50,60]);cl=self.classes(df)
        self.assertEqual(cl.filter("classe_B='FRIO'").count(),5)
        self.assertEqual(cl.filter("classe_B='FRIO'").select('temperatura_media_dia_c').distinct().first()[0],10)
    def test_07_degenerado_nao_cria_duas_condicoes(self):
        df=self.painel([22]*200);th=calcular_thresholds(df);self.assertTrue(th.first().threshold_degenerado)
        cl=self.classes(df);self.assertEqual(cl.filter("classe_B='INTERMEDIARIO'").count(),200)
        e=criar_exposicao(cl).filter("metodo='B'").first()
        self.assertEqual((e.dias_frios,e.dias_quentes,e.elegivel_exposicao),(0,0,0))
    def test_08_todos_dias_validos_particionados(self):
        cl=self.classes(self.painel(list(range(100))+[None]));validar_classificacao(cl)
        for r in criar_exposicao(cl).collect():
            self.assertEqual(r.dias_frios+r.dias_quentes+r.dias_intermediarios,100)
            self.assertEqual(r.dias_sem_temperatura,1)
    def test_09_percentis_reproduziveis_apos_reordenacao(self):
        df=self.painel([v%27 for v in range(200)])
        a=calcular_thresholds(df).first();b=calcular_thresholds(df.orderBy(F.rand(20261005)).repartition(3)).first()
        for c in ['p05','p10','p20','p25','mediana','p75','p80','p90','p95']:self.assertEqual(a[c],b[c])
    def test_10_percentil_mensal_compara_mes_do_proprio_municipio(self):
        df=self.spark.createDataFrame([('M1',date(2015,mes,i+1),base+i/10,1,1) for mes,base,n in [(1,30.,10),(6,5.,20),(7,5.,20)] for i in range(n)],self.painel([1]).schema)
        r=self.classes(df).filter("data=DATE '2015-01-01'").first()
        self.assertEqual(r.classe_B,'QUENTE');self.assertEqual(r.classe_C,'FRIO')
        self.assertGreater(r.p20_mes,r.p20)
    def test_11_cid_esparsa_mantem_municipio_e_dias_zero_eventos(self):
        cl=self.classes(self.painel(range(200)).unionByName(self.painel(range(200),'M2')))
        expo=criar_exposicao(cl)
        nums=self.spark.createDataFrame([('M1','A','CATEGORIA_X',3,1,4),('M1','B','CATEGORIA_X',2,0,4)],
            'codigo_municipio string,metodo string,chave_alvo string,internacoes_frio long,internacoes_quente long,internacoes_total long')
        alvo=self.spark.createDataFrame([('CATEGORIA_X',)],'chave_alvo string')
        d=detalhe_cid(expo,nums,alvo);self.assertEqual(d.count(),4)
        r=d.filter("codigo_municipio='M2' AND metodo='B'").first()
        self.assertEqual((r.dias_frios,r.dias_quentes,r.internacoes_total,r.ambas_medias_zero),(40,40,0,True))
    def test_12_resumo_e_municipal_sem_peso_de_cidade_grande(self):
        df=self.spark.createDataFrame([(f'M{i}','A',f*100,q*100,100,100,(f+q)*100) for i,(f,q) in enumerate([(2,1),(3,1),(4,1),(1000,1)])],
            'codigo_municipio string,metodo string,internacoes_frio long,internacoes_quente long,dias_frios long,dias_quentes long,internacoes_total long')
        r=_resumo(_adicionar_metricas(df),['metodo']).first();self.assertEqual(r.mediana_diferenca_por_100,200)
    def test_13_spearman_trata_empates_por_rank_medio(self):
        d=self.spark.createDataFrame([(f'G{i}','Grupo',v,v,True) for i,v in enumerate([3.,2.,2.,0.])],
            'chave_alvo string,nivel_cid string,mediana_diferenca_por_100_A double,mediana_diferenca_por_100_B double,mesma_direcao boolean')
        ranks,stats=ranks_cid(d);self.assertAlmostEqual(stats.first().spearman,1)
        self.assertEqual(ranks.filter("chave_alvo='G1'").first().rank_A,2.5)
    def test_14_direcao_diferente_e_sensivel(self):
        d=self.spark.createDataFrame([('M1','A','TOTAL',1000,500,100,100,1500),('M1','B','TOTAL',500,1000,100,100,1500)],
            'codigo_municipio string,metodo string,chave_alvo string,internacoes_frio long,internacoes_quente long,dias_frios long,dias_quentes long,internacoes_total long')
        r=comparar_resumos(_resumo(_adicionar_metricas(d),['metodo','chave_alvo']),['chave_alvo']).first()
        self.assertEqual(r.robustez_descritiva,'SENSIVEL_AO_METODO')
    def test_15_bloqueia_gravacao_fora_da_auditoria(self):
        with self.assertRaises(ValueError):salvar(self.spark.range(1),'../../../../consumption/dashboard_analytics')
    def test_16_relativo_recupera_municipio_quente_sem_frio_absoluto(self):
        df=self.painel([21+8*i/199 for i in range(200)]);e={r.metodo:r for r in criar_exposicao(self.classes(df)).collect()}
        self.assertEqual(e['A'].elegivel_exposicao,0);self.assertEqual(e['B'].elegivel_exposicao,1)
        self.assertEqual((e['B'].dias_frios,e['B'].dias_quentes),(40,40))
    def test_17_anual_mantem_threshold_do_periodo_completo(self):
        df=self.spark.createDataFrame([('M1',date(ano,1,i+1),base+i,1,1) for ano,base in [(2015,0.),(2016,100.)] for i in range(10)],self.painel([1]).schema)
        cl=self.classes(df)
        self.assertEqual(cl.select('p20','p80').distinct().count(),1)
        self.assertEqual(criar_exposicao(cl,True).filter("metodo='B' AND elegivel_exposicao=1").count(),0)
    def test_18_duplicata_municipio_data_e_rejeitada(self):
        df=self.painel(range(10));cl=self.classes(df)
        with self.assertRaises(ValueError):validar_classificacao(cl.unionByName(cl.limit(1)))
    def test_19_exemplos_reproduziveis_sem_coluna_duplicada(self):
        df=self.painel(range(100),'M1').unionByName(self.painel(range(20,120),'M2')).unionByName(self.painel(range(40,140),'M3'))
        th=calcular_thresholds(df);expo=criar_exposicao(self.classes(df));d=municipios_representativos(th,expo)
        self.assertEqual(len(d.columns),len(set(d.columns)))
        escolhidos={r.codigo_municipio:r.perfil for r in d.select('codigo_municipio','perfil').distinct().collect()}
        self.assertEqual(escolhidos,{'M1':'MENOR_MEDIA','M2':'MEDIANA_DAS_MEDIAS','M3':'MAIOR_MEDIA'})
    def test_20_total_demografico_mantem_rotulos_oficiais(self):
        d=self.spark.createDataFrame([('TOTAL',None,None,'60-79','3')],
            'chave_alvo string,nivel_cid string,rotulo_alvo string,faixa_etaria string,sexo string')
        r=rotular_demografia(d).first()
        self.assertEqual(r.nivel_cid,'Total');self.assertEqual(r.rotulo_alvo,'Todas as internações')
        self.assertEqual(r.sexo_descricao,'Feminino');self.assertEqual(r.ordem_sexo,2);self.assertEqual(r.ordem_faixa_etaria,4)


if __name__=='__main__':unittest.main(verbosity=2)
