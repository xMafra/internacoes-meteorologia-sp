"""DQ compartilhado da definição térmica e reconciliação, sem corrigir valores."""
from functools import reduce
from pyspark.sql import functions as F
from src.consumption.metodologia_termica import NOMES_Q
from src.quality.consumption.fq_checks import conferir_chaves,conferir_condicoes

def validar_thresholds(d,chaves):
    qs=[F.col(q) for q in NOMES_Q]
    conferir_condicoes(d,{'threshold_nulo':reduce(lambda a,b:a|b,[q.isNull() for q in qs]),
        'fora_minmax':(qs[0]<F.col('temperatura_minima'))|(qs[-1]>F.col('temperatura_maxima')),
        'fora_ordem':reduce(lambda a,b:a|b,[qs[i]>qs[i+1] for i in range(len(qs)-1)]),
        'n_invalido':F.col('dias_temperatura_valida')<=0},'thresholds')
    conferir_chaves(d,chaves,'thresholds')

def validar_classificacao(d):
    conferir_chaves(d,['codigo_municipio','data'],'painel_classificado')
    cs={}
    for m in ['A','B','C']:
        cs['classe_valida_'+m]=F.col('temperatura_valida') & ~F.col('classe_'+m).isin('FRIO','INTERMEDIARIO','QUENTE')
        cs['invalido_classificado_'+m]=~F.col('temperatura_valida') & (F.col('classe_'+m)!='INVALIDO')
    conferir_condicoes(d,cs,'classificacao_diaria')

def reconciliar_oficial(antigo,novo,chaves,nome):
    conferir_chaves(novo,chaves,nome)
    assert antigo.select(*chaves).exceptAll(novo.select(*chaves)).count()==0
    assert novo.select(*chaves).exceptAll(antigo.select(*chaves)).count()==0
    j=antigo.alias('o').join(novo.alias('n'),chaves)
    checks={}
    for c,tipo in antigo.dtypes:
        if c not in novo.columns or c in chaves:continue
        x,y=F.col('o.`'+c+'`'),F.col('n.`'+c+'`')
        diff=~x.eqNullSafe(y)
        if tipo in ('double','float'):diff=diff & ((F.abs(x-y)>1e-10)|x.isNull()|y.isNull())
        checks[c]=diff
    conferir_condicoes(j,checks,'reconciliacao_'+nome)
