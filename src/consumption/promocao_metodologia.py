"""Etapa 1C: Consumption oficial B, D100 e sensibilidades A/C identificadas.

Reutiliza fontes materializadas e a regra exata promovida da Etapa 1B.
Valida todos os resultados antes da primeira substituição da produção.
"""
from __future__ import annotations
import json,sys,time,shutil
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from pyspark import StorageLevel
from pyspark.sql import SparkSession,functions as F
from src.consumption.dashboard_analytics import (BASE,SAIDA,_adicionar_metricas,_resumo,_dimensao_alvos_cid,
    criar_temperatura_faixa,criar_sazonalidade_mes,DEFASAGENS)
from src.consumption.metodologia_termica import (BASELINE_INICIO,BASELINE_FIM,METODOS,painel_periodo,
    temperatura_valida,calcular_thresholds,classe_relativa,classificar,criar_exposicao,metricas_geral,
    numeradores_cid,empilhar_numeradores,detalhe_cid,detalhe_demografico,rotular_demografia)
from src.consumption.fq_bootstrap import aplicar_bootstrap_recorte
from src.quality.consumption.fq_checks import conferir_chaves,conferir_condicoes,validar_detalhe,validar_resumo
from src.quality.consumption.thermal_checks import validar_thresholds,validar_classificacao,reconciliar_oficial
from src.common.spark_io import destino_parquet_atomico

EVIDENCIA=Path(BASE).parent/'validation/promocao_metodologia_final'
AUDITORIA=Path(BASE).parent/'validation/auditoria_definicao_temperatura'
VERSAO='P20_P80_MUNICIPIO_2015_2025_V1'
KEYS={'contraste_municipio':['codigo_municipio'],'geral_resumo':['chave_alvo'],'temporal_ano':['ano'],
    'cid_resumo':['chave_alvo'],'demografia_resumo':['chave_alvo','faixa_etaria','sexo'],
    'estratos_resumo':['chave_alvo','nivel_demografico','faixa_etaria','sexo'],
    'lag_resumo':['chave_alvo','defasagem']}
AUDIT_MAP={'contraste_municipio':'audit_general_results', 'geral_resumo':'audit_general_results',
    'temporal_ano':'audit_annual_results','cid_resumo':'audit_cid_results','demografia_resumo':'audit_demografia_results',
    'estratos_resumo':'audit_estratos_results'}


def identificar(df,metodo='B',papel='PRINCIPAL'):
    d=(df.withColumn('metodo_termico',F.lit(METODOS.get(metodo,metodo)))
       .withColumn('papel_metodologico',F.lit(papel)).withColumn('metrica_principal',F.lit('D100'))
       .withColumn('unidade_magnitude',F.lit('Diferenca de internacoes por 100 municipio-dias'))
       .withColumn('baseline_inicio',F.to_date(F.lit(BASELINE_INICIO)))
       .withColumn('baseline_fim',F.to_date(F.lit(BASELINE_FIM)))
       .withColumn('versao_metodologia',F.lit(VERSAO)))
    if 'municipios_validos_absoluto' in d.columns:
        d=d.withColumn('municipios_validos_magnitude',F.col('municipios_validos_absoluto'))
    if 'direcao_diferenca' in d.columns:
        direction=F.col('direcao_diferenca')
        if metodo in ('B','C'):
            direction=F.when(direction=='FRIO','RELATIVAMENTE_FRIO').when(direction=='QUENTE','RELATIVAMENTE_QUENTE').otherwise(direction)
        d=d.withColumn('direcao_termica',direction)
    return d


def classificacao_oficial(cl):
    c=F.col('classe_B')
    return identificar(cl.withColumn('classificacao_termica',F.when(c=='FRIO','RELATIVAMENTE_FRIO')
        .when(c=='QUENTE','RELATIVAMENTE_QUENTE').when(c=='INVALIDO','SEM_TEMPERATURA_VALIDA').otherwise('INTERMEDIARIO')))


def anexar_classificacao_lag(cl,ordem):
    """Lookup município + data civil, não posição de linha; baseline nunca varia.

    Nos dias-alvo técnicos de 2014 somente, usa o lag já materializado pela
    atribuição histórica da Consumption. Esses valores não formam thresholds.
    """
    if ordem==0:return cl.withColumn('classe_lag',F.col('classe_B'))
    campo=f'temperatura_media_lag_{ordem}d'
    lookup=cl.select(F.col('codigo_municipio').alias('_mun'),F.to_date('data').alias('_data'),
        F.col('temperatura_media_dia_c').alias('_temp'),F.col('temperatura_valida').alias('_valida'))
    d=cl.withColumn('data_exposicao',F.date_sub('data',ordem))
    d=d.join(lookup,(F.col('codigo_municipio')==F.col('_mun'))&(F.col('data_exposicao')==F.col('_data')),'left')
    tecnico=F.col('data_exposicao')<F.to_date(F.lit(BASELINE_INICIO))
    t=F.when(tecnico,F.col(campo)).otherwise(F.col('_temp'))
    finito=t.isNotNull()&~F.isnan(t)&(F.abs(t)!=float('inf'))
    v=F.coalesce(F.when(tecnico,finito).otherwise(F.col('_valida')&finito),F.lit(False))
    return d.withColumn('temperatura_exposicao',t).withColumn('classe_lag',classe_relativa(t,F.col('p20'),F.col('p80'),v)).drop('_mun','_data','_temp','_valida')


def contraste_classificado(d,grupos,classe='classe_B'):
    f=F.col(classe)=='FRIO';q=F.col(classe)=='QUENTE'
    return _adicionar_metricas(d.groupBy(*grupos).agg(
        F.sum(F.when(f,F.col('qtd_internacoes')).otherwise(0)).cast('long').alias('internacoes_frio'),
        F.sum(F.when(q,F.col('qtd_internacoes')).otherwise(0)).cast('long').alias('internacoes_quente'),
        F.sum(f.cast('long')).alias('dias_frios'),F.sum(q.cast('long')).alias('dias_quentes'),
        F.sum('qtd_internacoes').cast('long').alias('internacoes_total'),F.count('*').alias('municipio_dias_total')))


def criar_lags_principais(spark,cl,eventos):
    targets=spark.createDataFrame([('TOTAL','Todas as internações',1),('CAPITULO_10','Capítulo X — Doenças do aparelho respiratório',2),
        ('GRUPO_J09-J18','J09-J18 — Influenza e pneumonia',3),('GRUPO_J20-J22','J20-J22 — Outras infecções agudas das vias aéreas inferiores',4)],
        'chave_alvo string, alvo_analitico string, ordem_alvo int')
    # Materializa entradas mínimas independentes: o bloco de lags não reutiliza
    # a linhagem térmica/numeradores CID já agregada pelos demais recortes.
    cols=['municipio_dia_id','codigo_municipio','data','temperatura_media_dia_c',
          'temperatura_valida','p20','p80','classe_B','qtd_internacoes',
          *['temperatura_media_lag_'+str(n)+'d' for n in (1,3,7)]]
    cl=cl.select(*cols).localCheckpoint(eager=True)
    resp=eventos.select('municipio_dia_id','codigo_municipio','codigo_capitulo','codigo_grupo','qtd_internacoes')
    resp=resp.filter((F.col('codigo_capitulo')=='10')|F.col('codigo_grupo').isin('J09-J18','J20-J22')).localCheckpoint(eager=True)
    resp=resp.select('municipio_dia_id','codigo_municipio','qtd_internacoes',F.explode(F.array(
        F.when(F.col('codigo_capitulo')=='10',F.lit('CAPITULO_10')),
        F.when(F.col('codigo_grupo')=='J09-J18',F.lit('GRUPO_J09-J18')),
        F.when(F.col('codigo_grupo')=='J20-J22',F.lit('GRUPO_J20-J22')))).alias('chave_alvo')).filter('chave_alvo IS NOT NULL')
    parts=[]
    for rotulo,ordem,_ in DEFASAGENS:
        d=anexar_classificacao_lag(cl,ordem)
        total=contraste_classificado(d,['codigo_municipio'],'classe_lag').withColumn('chave_alvo',F.lit('TOTAL'))
        e=resp.join(d.select('municipio_dia_id','classe_lag'),'municipio_dia_id')
        nums=e.groupBy('codigo_municipio','chave_alvo').agg(
            F.sum(F.when(F.col('classe_lag')=='FRIO',F.col('qtd_internacoes')).otherwise(0)).alias('internacoes_frio'),
            F.sum(F.when(F.col('classe_lag')=='QUENTE',F.col('qtd_internacoes')).otherwise(0)).alias('internacoes_quente'),
            F.sum('qtd_internacoes').alias('internacoes_total'))
        det=total.select('codigo_municipio','dias_frios','dias_quentes','municipio_dias_total').crossJoin(targets.filter("chave_alvo!='TOTAL'").select('chave_alvo'))
        det=_adicionar_metricas(det.join(nums,['codigo_municipio','chave_alvo'],'left').fillna(0,subset=['internacoes_frio','internacoes_quente','internacoes_total']))
        allrows=total.unionByName(det.select(total.columns)).persist(StorageLevel.MEMORY_AND_DISK)
        validar_detalhe(allrows,['codigo_municipio','chave_alvo'],'lag_'+rotulo)
        res=_resumo(allrows,['chave_alvo']).join(targets,'chave_alvo').withColumn('defasagem',F.lit(rotulo)).withColumn('ordem_defasagem',F.lit(ordem))
        res=res.persist(StorageLevel.MEMORY_AND_DISK);res.count();allrows.unpersist();parts.append(res)
    return identificar(parts[0].unionByName(parts[1]).unionByName(parts[2]).unionByName(parts[3]))


def gerar_saidas_oficiais(spark,fato_dia,fato_cid,dim_cid,dim_municipio):
    t0=time.perf_counter();tempos={}
    def cache(d):d=d.persist(StorageLevel.MEMORY_AND_DISK);d.count();return d
    def marca(n,t):tempos[n]=round(time.perf_counter()-t,3);print('TEMPO',n,tempos[n],flush=True)
    t=time.perf_counter();painel=cache(painel_periodo(fato_dia))
    conferir_chaves(painel,['codigo_municipio','data'],'fonte_diaria')
    th=cache(calcular_thresholds(painel));tm=cache(calcular_thresholds(painel,('codigo_municipio','mes')))
    validar_thresholds(th,['codigo_municipio']);validar_thresholds(tm,['codigo_municipio','mes'])
    th=cache(th.join(dim_municipio.select('codigo_municipio','nome_municipio'),'codigo_municipio'))
    cl=cache(classificar(painel,th,tm));validar_classificacao(cl)
    expo=cache(criar_exposicao(cl));expo_ano=cache(criar_exposicao(cl,True))
    conferir_condicoes(expo,{'dias_nao_particionados':F.col('dias_frios')+F.col('dias_quentes')+F.col('dias_intermediarios')!=F.col('dias_temperatura_valida')},'particao_dias')
    out={'dim_threshold_temperatura_municipio':identificar(th),
        'sensibilidade_mes_thresholds':identificar(tm,'C','SENSIBILIDADE_SAZONAL'),
        'classificacao_termica_dia':classificacao_oficial(cl.select('municipio_dia_id','codigo_municipio','data','ano','mes',
            'temperatura_media_dia_c','temperatura_valida','p20','p80','p20_mes','p80_mes','threshold_degenerado','threshold_mes_degenerado','classe_A','classe_B','classe_C'))}
    geral=cache(metricas_geral(cl,expo));annual=cache(metricas_geral(cl,expo_ano,True))
    muni=geral.filter("metodo='B'").drop('metodo').join(th.select('codigo_municipio','p20','p80','threshold_degenerado'),'codigo_municipio')
    muni=muni.join(dim_municipio.select('codigo_municipio','nome_municipio','latitude_municipio','longitude_municipio'),'codigo_municipio')
    out['contraste_municipio']=identificar(muni)
    for method,prefix,role in [('B','','PRINCIPAL'),('C','sensibilidade_mes_','SENSIBILIDADE_SAZONAL')]:
        det=geral.filter(F.col('metodo')==method).drop('metodo')
        res=_resumo(det,['chave_alvo'])
        if method=='B':res=aplicar_bootstrap_recorte(spark,'geral',det,res,['chave_alvo'])
        out[prefix+'geral_resumo']=identificar(cache(res),method,role)
        out[prefix+'temporal_ano']=identificar(cache(_resumo(annual.filter(F.col('metodo')==method),['ano'])),method,role)
    marca('baseline_classificacao_geral_anual',t)
    t=time.perf_counter();targets=_dimensao_alvos_cid(dim_cid)
    total=spark.createDataFrame([('TOTAL','Total',0,None,None,None,None,None,None,'Todas as internações')],targets.schema)
    fulltargets=targets.unionByName(total)
    fields=['municipio_dia_id','codigo_municipio','codigo_categoria','faixa_etaria','sexo','sexo_descricao','qtd_internacoes']
    ev=cache(fato_cid.select(*fields).join(cl.select('municipio_dia_id','classe_A','classe_B','classe_C'),'municipio_dia_id')
        .join(dim_cid.select('codigo_categoria','codigo_capitulo','codigo_grupo'),'codigo_categoria','left'))
    raw=numeradores_cid(ev).persist(StorageLevel.DISK_ONLY);raw.count()
    nums=empilhar_numeradores(raw).persist(StorageLevel.DISK_ONLY);nums.count();marca('numeradores_compartilhados',t)
    for name,maker,keys in [('cid',lambda:detalhe_cid(expo,nums,targets),['chave_alvo']),
        ('demografia',lambda:detalhe_demografico(spark,expo,nums,targets),['chave_alvo','faixa_etaria','sexo']),
        ('estratos',lambda:detalhe_demografico(spark,expo,nums,targets,'IDADE').unionByName(detalhe_demografico(spark,expo,nums,targets,'SEXO')),['chave_alvo','nivel_demografico','faixa_etaria','sexo'])]:
        t=time.perf_counter();det=maker().persist(StorageLevel.DISK_ONLY);det.count()
        validar_detalhe(det,['codigo_municipio','metodo',*keys],name+'_municipal')
        allres=cache(_resumo(det,['metodo',*keys]).join(fulltargets,'chave_alvo'))
        if name!='cid':allres=cache(rotular_demografia(allres))
        # Recalcula A para comprovar a referência histórica; nunca altera seus números.
        reference=EVIDENCIA/'producao_antes'/(name+'_resumo')
        if reference.exists():reconciliar_oficial(spark.read.parquet(str(reference)),allres.filter("metodo='A'"),keys,'A_'+name)
        bdet=det.filter("metodo='B'").drop('metodo').join(fulltargets.select('chave_alvo','nivel_cid'),'chave_alvo')
        bres=allres.filter("metodo='B'").drop('metodo')
        bres=cache(aplicar_bootstrap_recorte(spark,name,bdet,bres,keys))
        out[name+'_resumo']=identificar(bres)
        det.unpersist();allres.unpersist();marca(name+'_dq_resumo_bootstrap',t)
    t=time.perf_counter()
    tc=targets.filter((F.col('nivel_cid')=='Capítulo')|F.col('chave_alvo').isin('GRUPO_J09-J18','GRUPO_J20-J22'))
    nc=empilhar_numeradores(numeradores_cid(ev,False,('C',),True),('C',)).join(tc.select('chave_alvo'),'chave_alvo')
    ec=expo.filter("metodo='C'").select('codigo_municipio','metodo','dias_frios','dias_quentes','municipio_dias_total')
    dc=_adicionar_metricas(ec.crossJoin(tc.select('chave_alvo')).join(nc,['codigo_municipio','metodo','chave_alvo'],'left').fillna(0,subset=['internacoes_frio','internacoes_quente','internacoes_total']))
    out['sensibilidade_mes_cid_resumo']=identificar(cache(_resumo(dc,['chave_alvo']).join(tc,'chave_alvo')),'C','SENSIBILIDADE_SAZONAL')
    eventos_lag=fato_cid.select('municipio_dia_id','codigo_municipio','codigo_categoria','qtd_internacoes').join(
        dim_cid.select('codigo_categoria','codigo_capitulo','codigo_grupo'),'codigo_categoria','left')
    out['lag_resumo']=cache(criar_lags_principais(spark,cl,eventos_lag))
    out['temperatura_faixa']=identificar(cache(criar_temperatura_faixa(painel)),'FAIXAS_FISICAS','COMPLEMENTAR')
    out['sazonalidade_mes']=identificar(cache(criar_sazonalidade_mes(painel)),'DESCRICAO_MENSAL','COMPLEMENTAR')
    marca('sensibilidade_mensal_lags_complementares',t)
    for name in KEYS:
        prior=EVIDENCIA/'producao_antes'/name
        if prior.exists():out['sensibilidade_absoluto_'+name]=identificar(spark.read.parquet(str(prior)),'A','SENSIBILIDADE_ABSOLUTA')
    tempos['calculo_total']=round(time.perf_counter()-t0,3)
    return out,tempos


def escrever(d,path):
    with destino_parquet_atomico(path) as stage:d.coalesce(12 if 'classificacao_termica_dia' in str(path) else 1).write.mode('overwrite').parquet(stage)


def main():
    start=time.perf_counter();EVIDENCIA.mkdir(parents=True,exist_ok=True)
    # Snapshot persistido antes da primeira promoção, nunca substituído em reexecuções.
    backup=EVIDENCIA/'producao_antes'
    if not backup.exists():shutil.copytree(SAIDA,backup)
    spark=SparkSession.builder.appName('TCC-Etapa1C-Metodologia-Final').getOrCreate();spark.sparkContext.setLogLevel('ERROR')
    try:
        read=lambda n:spark.read.parquet(BASE+'/'+n)
        out,tempos=gerar_saidas_oficiais(spark,read('fato_municipio_dia'),read('fato_municipio_dia_cid'),read('dim_cid'),read('dim_municipio'))
        from src.quality.consumption.dq_metodologia_final import validar_tabelas
        # Materializar quebra a linhagem compartilhada de joins CID/lag. A
        # reconciliação final usa arquivos independentes, que serão publicados.
        prepared=EVIDENCIA/'tabelas_preparadas'
        t=time.perf_counter()
        for name,d in out.items():escrever(d,prepared/name);print('STAGING_PREPARADO',name,flush=True)
        tempos['materializacao_validada']=round(time.perf_counter()-t,3)
        # Nenhum arquivo oficial é gravado antes do DQ destas saídas físicas.
        validated={n:spark.read.parquet(str(prepared/n)) for n in out}
        metadata=validar_tabelas(spark,validated)
        t=time.perf_counter();published=[]
        try:
            for name,d in validated.items():escrever(d,Path(SAIDA)/name);published.append(name);print('OFICIAL_GRAVADO',name,flush=True)
        except Exception:
            # Restaura toda tabela anterior já substituída; saídas novas ficam identificadas.
            for name in published:
                if (backup/name).exists():escrever(spark.read.parquet(str(backup/name)),Path(SAIDA)/name)
            raise
        tempos['publicacao']=round(time.perf_counter()-t,3)
        result={'status':'PASS','metodo_principal':METODOS['B'],'metrica_principal':'D100','baseline':[BASELINE_INICIO,BASELINE_FIM],
            'versao':VERSAO,'tabelas':metadata,'tempos':tempos,'tempo_total_segundos':round(time.perf_counter()-start,3)}
        (EVIDENCIA/'resultado_execucao.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        print('PROMOCAO_METODOLOGIA_FINAL=PASS',flush=True);return 0
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
