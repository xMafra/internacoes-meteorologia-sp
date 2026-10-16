"""DQ da promoção oficial e reconciliação integral com evidência histórica."""
from pathlib import Path
from pyspark.sql import functions as F
from src.consumption.dashboard_analytics import BASE,SAIDA
from src.consumption.metodologia_termica import BASELINE_INICIO,BASELINE_FIM,classe_relativa
from src.quality.consumption.thermal_checks import validar_thresholds,validar_classificacao,reconciliar_oficial
from src.quality.consumption.fq_checks import conferir_chaves,conferir_condicoes,validar_detalhe,validar_resumo

KEYS={'contraste_municipio':['codigo_municipio'],'geral_resumo':['chave_alvo'],'temporal_ano':['ano'],
    'cid_resumo':['chave_alvo'],'demografia_resumo':['chave_alvo','faixa_etaria','sexo'],
    'estratos_resumo':['chave_alvo','nivel_demografico','faixa_etaria','sexo'],'lag_resumo':['chave_alvo','defasagem']}


def validar_tabelas(spark,tabelas):
    audit=Path(BASE).parent/'validation/auditoria_definicao_temperatura'
    backup=Path(BASE).parent/'validation/promocao_metodologia_final/producao_antes'
    a=lambda n:spark.read.parquet(str(audit/n))
    th=tabelas['dim_threshold_temperatura_municipio'];cl=tabelas['classificacao_termica_dia']
    validar_thresholds(th,['codigo_municipio']);validar_thresholds(tabelas['sensibilidade_mes_thresholds'],['codigo_municipio','mes'])
    validar_classificacao(cl)
    conferir_condicoes(th,{'baseline_inicio':~F.col('baseline_inicio').eqNullSafe(F.to_date(F.lit(BASELINE_INICIO))),
        'baseline_fim':~F.col('baseline_fim').eqNullSafe(F.to_date(F.lit(BASELINE_FIM)))},'baseline_fixo')
    conferir_condicoes(cl,{'fora_baseline':~F.to_date('data').between(BASELINE_INICIO,BASELINE_FIM),
        'classe_incorreta':~F.col('classe_B').eqNullSafe(classe_relativa(F.col('temperatura_media_dia_c'),F.col('p20'),F.col('p80'),F.col('temperatura_valida')))},'classificacao_B')
    reconciliar_oficial(a('audit_relative_temperature_thresholds'),th,['codigo_municipio'],'thresholds_auditoria')
    n=spark.read.parquet(BASE+'/dim_municipio').select('codigo_municipio').distinct().count()
    if th.count()!=n:raise ValueError('Threshold ausente para município válido')
    for name,keys in KEYS.items():
        d=tabelas[name]
        if name=='contraste_municipio':validar_detalhe(d,keys,name)
        else:validar_resumo(d,keys,name)
        conferir_condicoes(d,{'metodo_principal':~F.col('metodo_termico').eqNullSafe(F.lit('P20_P80_MUNICIPIO')),
            'papel':~F.col('papel_metodologico').eqNullSafe(F.lit('PRINCIPAL')),
            'metrica':~F.col('metrica_principal').eqNullSafe(F.lit('D100'))},name+'_semantica')
        old=spark.read.parquet(str(backup/name))
        missing=set(old.columns)-set(d.columns)
        if missing:raise ValueError('Colunas de compatibilidade removidas: '+name+': '+str(missing))
        sens=tabelas['sensibilidade_absoluto_'+name]
        reconciliar_oficial(old,sens,keys,'A_preservado_'+name)
        if name not in ['contraste_municipio','lag_resumo']:
            expected=a({'geral_resumo':'audit_general_results','temporal_ano':'audit_annual_results',
                'cid_resumo':'audit_cid_results','demografia_resumo':'audit_demografia_results','estratos_resumo':'audit_estratos_results'}[name]).filter("metodo='B'")
            reconciliar_oficial(expected,d,keys,'B_auditoria_'+name)
    mun=tabelas['contraste_municipio']
    mc=a('audit_municipal_comparison')
    cols=[c for c in mc.columns if c.endswith('_B') and c[:-2] in mun.columns]
    reconciliar_oficial(mc.select('codigo_municipio',*[F.col(c).alias(c[:-2]) for c in cols]),mun,['codigo_municipio'],'B_municipal_auditoria')
    reference_eligible=mc.filter('elegivel_amostra_B=1').count()
    if mun.filter('elegivel_amostra=1').count()!=reference_eligible:raise ValueError('Elegibilidade divergente da auditoria')
    for name,ref,keys in [('sensibilidade_mes_geral_resumo','audit_general_results',['chave_alvo']),
        ('sensibilidade_mes_temporal_ano','audit_annual_results',['ano']),('sensibilidade_mes_cid_resumo','audit_seasonal_cid_results',['chave_alvo'])]:
        d=tabelas[name];validar_resumo(d,keys,name)
        reconciliar_oficial(a(ref).filter("metodo='C'"),d,keys,'C_preservado_'+name)
        conferir_condicoes(d,{'promovido_indevidamente':F.col('papel_metodologico')!='SENSIBILIDADE_SAZONAL'},name+'_papel')
    for name,keys in [('temperatura_faixa',['faixa_temperatura','ordem_faixa']),('sazonalidade_mes',['mes'])]:
        reconciliar_oficial(spark.read.parquet(str(backup/name)),tabelas[name],keys,name+'_complementar')
    cid=tabelas['cid_resumo'];g=tabelas['geral_resumo'];lag=tabelas['lag_resumo'].filter("defasagem='D0'")
    for key in ['TOTAL','CAPITULO_10','GRUPO_J09-J18','GRUPO_J20-J22']:
        origin=g if key=='TOTAL' else cid.filter(F.col('chave_alvo')==key)
        metricas=['chave_alvo','mediana_diferenca_por_100','q1_diferenca_por_100','q3_diferenca_por_100',
            'mediana_diferenca_relativa','municipios_validos','municipios_validos_absoluto','dias_frios_total','dias_quentes_total','internacoes_total',
            'internacoes_frio_validos_absoluto','internacoes_quente_validos_absoluto']
        reconciliar_oficial(origin.select(*metricas),lag.filter(F.col('chave_alvo')==key),['chave_alvo'],'lag_D0_'+key)
    alvos=spark.read.parquet(BASE+'/dim_cid')
    from src.consumption.dashboard_analytics import _dimensao_alvos_cid
    nc=_dimensao_alvos_cid(alvos).count()
    counts={'contraste_municipio':n,'temporal_ano':11,'geral_resumo':1,'temperatura_faixa':5,'sazonalidade_mes':12,
        'cid_resumo':nc,'demografia_resumo':(nc+1)*10,'estratos_resumo':(nc+1)*7,'lag_resumo':16,
        'dim_threshold_temperatura_municipio':n,'sensibilidade_mes_thresholds':n*12,'classificacao_termica_dia':cl.count()}
    metadata={}
    for name,d in tabelas.items():
        count=d.count()
        if name in counts and count!=counts[name]:raise ValueError('Cardinalidade incorreta: '+name)
        before=spark.read.parquet(str(backup/name)).columns if (backup/name).exists() else []
        metadata[name]={'linhas':count,'colunas':d.columns,'colunas_adicionadas':[c for c in d.columns if c not in before]}
    metadata['geral_resultado']=g.first().asDict()
    metadata['respiratorias_resultado']=[r.asDict() for r in cid.filter(F.col('chave_alvo').isin('CAPITULO_10','GRUPO_J09-J18','GRUPO_J20-J22')).collect()]
    metadata['demografia_total']=[r.asDict() for r in tabelas['demografia_resumo'].filter("chave_alvo='TOTAL'").collect()]
    print('DQ_METODOLOGIA_FINAL=PASS',flush=True)
    return metadata


def main():
    from pyspark.sql import SparkSession
    spark=SparkSession.builder.appName('DQ-Metodologia-Final').getOrCreate();spark.sparkContext.setLogLevel('ERROR')
    try:
        names=[p.name for p in Path(SAIDA).iterdir() if p.is_dir() and not '.__' in p.name]
        validar_tabelas(spark,{n:spark.read.parquet(str(Path(SAIDA)/n)) for n in names})
        print('DQ_DASHBOARD_ANALYTICS=PASS',flush=True);return 0
    finally:spark.stop()
