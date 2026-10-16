"""Etapa 1: materializa somente comparações, reconcilia histórico e gera auditoria.

Não lê nem grava arquivos do Power BI, Bronze, Silver ou Gold.
"""
from __future__ import annotations
import json, math, sys, time
from functools import reduce
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
from src.consumption.dashboard_analytics import (BASE, SAIDA, FAIXAS_ETARIAS, SEXOS, _numeradores_alvos,
    _contraste,_resumo,criar_cid_detalhe,criar_demografia_detalhe,criar_estratos_detalhe,
    criar_contraste_municipio,criar_temporal,criar_lag_resumo,publicar)
from src.consumption.fq_bootstrap import aplicar_bootstrap_recorte
from src.common.spark_io import destino_parquet_atomico
from src.quality.consumption.fq_checks import validar_detalhe,validar_resumo,conferir_chaves

AUDIT='/home/jovyan/work/data/validation/auditoria_metricas_extremas'
KEYS={
    'cid_resumo':['chave_alvo'],
    'demografia_resumo':['chave_alvo','faixa_etaria','sexo'],
    'estratos_resumo':['chave_alvo','nivel_demografico','faixa_etaria','sexo'],
    'geral_resumo':['chave_alvo'],
    'temporal_ano':['ano'],
    'lag_resumo':['chave_alvo','defasagem'],
    'contraste_municipio':['codigo_municipio'],
}


def escrever(df,destino,partes=1):
    with destino_parquet_atomico(destino) as staging:
        df.coalesce(partes).write.mode('overwrite').option('compression','snappy').parquet(staging)


def preservar_historico(spark,nome,novo):
    """Verifica o recálculo e mantém literalmente todas as colunas antigas."""
    destino=Path(SAIDA)/nome
    if not destino.exists():return novo,{'linhas':novo.count(),'colunas_historicas':0}
    b=Path(AUDIT)/'historico_antes'/nome
    if not b.exists():escrever(spark.read.parquet(str(destino)),str(b))
    antigo=spark.read.parquet(str(b)); chaves=KEYS[nome]
    conferir_chaves(novo,chaves,nome)
    if antigo.select(*chaves).exceptAll(novo.select(*chaves)).limit(1).count() or novo.select(*chaves).exceptAll(antigo.select(*chaves)).limit(1).count():
        raise ValueError('Cardinalidade/chaves históricas alteradas: '+nome)
    a=antigo.alias('a');n=novo.alias('n');j=a.join(n,chaves,'inner')
    conds=[]
    for field in antigo.schema:
        c=field.name
        if c in chaves:continue
        if c not in novo.columns:raise ValueError('Coluna histórica removida: '+c)
        x,y=F.col('a.`'+c+'`'),F.col('n.`'+c+'`')
        diferente=~x.eqNullSafe(y)
        if field.dataType.simpleString() in ('float','double'):
            diferente=diferente & ((F.abs(x-y)>1e-10) | x.isNull() | y.isNull())
        conds.append(diferente)
    if conds and j.filter(reduce(lambda x,y:x|y,conds)).limit(1).count():
        raise ValueError('Resultado histórico divergente: '+nome)
    adicionadas=[c for c in novo.columns if c not in antigo.columns]
    resultado=antigo.join(novo.select(*chaves,*adicionadas),chaves,'inner')
    linhas=antigo.count()
    print(f'HISTORICO PASS | {nome}: {linhas} linhas, {len(antigo.columns)} colunas antigas preservadas',flush=True)
    return resultado,{'linhas':linhas,'colunas_historicas':len(antigo.columns),'colunas_adicionadas':adicionadas}


def campos_resumo(detalhe):
    omit={'codigo_municipio','internacoes_frio','internacoes_quente','internacoes_total','dias_frios','dias_quentes',
          'media_frio_municipio','media_quente_municipio','diferenca_absoluta','diferenca_relativa',
          'municipio_dias_total','elegivel_amostra','contraste_relativo_valido','diferenca_por_100_municipio_dias',
          'media_frio_zero','media_quente_zero','ambas_medias_zero','direcao_diferenca','diferenca_zero',
          'contraste_absoluto_valido','classe_consistencia'}
    return [c for c in detalhe.columns if c not in omit]


def exportar_auditoria(spark,resumos,detalhes):
    cols=['chave_alvo','nivel_cid','codigo_capitulo','codigo_grupo','codigo_categoria','rotulo_alvo',
          'faixa_etaria','sexo','sexo_descricao','nivel_demografico','mediana_media_frio','mediana_media_quente',
          'mediana_media_frio_elegiveis','mediana_media_quente_elegiveis','mediana_diferenca_absoluta',
          'mediana_diferenca_absoluta_elegiveis','mediana_diferenca_por_100','mediana_diferenca_relativa',
          'direcao_diferenca','municipios_validos','municipios_validos_absoluto','internacoes_total',
          'dias_frios_total','dias_quentes_total','dias_frios_validos_relativo','dias_quentes_validos_relativo',
          'dias_frios_validos_absoluto','dias_quentes_validos_absoluto','media_frio_zero','media_quente_zero',
          'ambas_medias_zero','municipios_frio_zero_relativo','municipios_so_quente','municipios_so_frio',
          'municipios_ambas_medias_zero']
    partes=[]
    for nome in ('cid_resumo','demografia_resumo','estratos_resumo'):
        d=resumos[nome]
        partes.append(d.select(*[F.col(c) if c in d.columns else F.lit(None).alias(c) for c in cols])
                      .withColumn('tabela_origem',F.lit(nome)))
    todos=reduce(lambda a,b:a.unionByName(b),partes).withColumn('motivo_auditoria',F.concat_ws('; ',
        F.when(F.abs(F.col('mediana_diferenca_relativa')+1)<1e-10,'RELATIVO_MENOS_100'),
        F.when(F.abs(F.col('mediana_diferenca_relativa')-1)<1e-10,'RELATIVO_MAIS_100'),
        F.when(F.abs(F.abs(F.col('mediana_diferenca_relativa'))-1)<=.01,'RELATIVO_PROXIMO_100_ATE_1_PP'),
        F.when(F.col('municipios_so_quente')>0,'MUNICIPIOS_COM_FRIO_ZERO_QUENTE_POSITIVO'),
        F.when(F.col('municipios_so_frio')>0,'MUNICIPIOS_COM_QUENTE_ZERO_FRIO_POSITIVO'),
        F.when(F.col('municipios_ambas_medias_zero')>0,'MUNICIPIOS_COM_AMBAS_ZERO')))
    escrever(todos,AUDIT+'/resumos_auditoria')
    # CSV inclui TODAS as bases: não inventa limiar de poucos eventos/média pequena.
    with destino_parquet_atomico(AUDIT+'/resumos_csv') as staging:
        todos.coalesce(1).write.mode('overwrite').option('header',True).csv(staging)
    extremos=todos.filter(F.length('motivo_auditoria')>0)
    escrever(extremos,AUDIT+'/casos_extremos')
    caso=todos.filter((F.col('municipios_validos')==3)&(F.col('internacoes_total')==12))
    escrever(caso,AUDIT+'/caso_3_municipios_12_internacoes')
    rows=[r.asDict(recursive=True) for r in caso.collect()]  # somente os recortes solicitados
    Path(AUDIT+'/caso_3_12.json').write_text(json.dumps(rows,ensure_ascii=False,indent=2),encoding='utf-8')
    for nome,detail in detalhes.items():
        keys=KEYS[nome+'_resumo']
        casos=caso.filter(F.col('tabela_origem')==nome+'_resumo').select(*keys)
        municipal=detail.join(casos,keys,'inner')
        escrever(municipal,AUDIT+'/caso_3_12_municipal_'+nome,1)
        if rows:municipal.filter(F.col('internacoes_total')>0).orderBy(*keys,'codigo_municipio').show(30,False)
    return rows, todos.count(), extremos.count()


def main():
    if (Path(SAIDA)/'dim_threshold_temperatura_municipio').exists():
        raise RuntimeError('Etapa 1 é histórica: a produção usa B. Execute dashboard_analytics.py para regenerar a metodologia final.')
    inicio=time.perf_counter();tempos={};metadata={};pendentes={};detalhes={}
    spark=SparkSession.builder.appName('TCC-Etapa1-Metricas-Frio-Quente').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    def marca(nome,t):tempos[nome]=round(time.perf_counter()-t,3);print(f'TEMPO {nome}: {tempos[nome]} s',flush=True)
    try:
        ler=lambda nome:spark.read.option('mergeSchema','true').parquet(f'{BASE}/{nome}')
        t=time.perf_counter()
        fato=ler('fato_municipio_dia').persist(StorageLevel.MEMORY_AND_DISK)
        dim=ler('dim_cid');municipios=ler('dim_municipio')
        # Mesmo join/semântica da rotina original; fatos não são gravados.
        temperaturas=fato.select('municipio_dia_id','temperatura_media_dia_c','temperatura_media_lag_1d','temperatura_media_lag_3d','temperatura_media_lag_7d')
        eventos=(ler('fato_municipio_dia_cid').join(temperaturas,'municipio_dia_id','inner')
                 .join(dim,'codigo_categoria','left').persist(StorageLevel.DISK_ONLY))
        raw=_numeradores_alvos(eventos,demografia=True).persist(StorageLevel.MEMORY_AND_DISK)
        raw.count();marca('numeradores_compartilhados',t)
        controle=fato.agg(F.count('*').alias('municipio_dias'),F.sum('qtd_internacoes').alias('internacoes')).first().asDict()
        metadata['fontes_preservadas']=controle
        nums_cid=raw.filter(F.col('chave_alvo')!='TOTAL').groupBy('codigo_municipio','chave_alvo').agg(
            *[F.sum(c).cast('long').alias(c) for c in ('internacoes_frio','internacoes_quente','internacoes_total')])
        nums_demo=raw.filter(F.col('faixa_etaria').isin([f for f,_ in FAIXAS_ETARIAS]) & F.col('sexo').isin([s for s,_,_ in SEXOS]))
        criadores={
            'cid':lambda:criar_cid_detalhe(fato,eventos,dim,nums_cid),
            'demografia':lambda:criar_demografia_detalhe(spark,fato,eventos,dim,nums_demo),
            'estratos':lambda:criar_estratos_detalhe(spark,fato,eventos,dim,raw),
        }
        for nome,criar in criadores.items():
            t=time.perf_counter();detalhe=criar().persist(StorageLevel.DISK_ONLY)
            keys=KEYS[nome+'_resumo']; validar_detalhe(detalhe,['codigo_municipio',*keys],nome)
            # A auditoria municipal permite reproduzir os zeros e as distribuições sem reler a fato.
            colunas=[c for c in detalhe.columns if not c.startswith('descricao_') and c!='rotulo_alvo']
            escrever(detalhe.select(*colunas),AUDIT+'/municipal_'+nome,16)
            resumo=_resumo(detalhe,campos_resumo(detalhe))
            tb=time.perf_counter()
            resumo=aplicar_bootstrap_recorte(spark,nome,detalhe,resumo,keys)
            if nome in ('cid','demografia'):marca(nome+'_bootstrap',tb)
            # O resumo compacto é consultado pelo DQ, pelas chaves e pelo histórico.
            # Materializá-lo uma vez evita repetir a agregação municipal em cada ação.
            resumo=resumo.persist(StorageLevel.MEMORY_AND_DISK);resumo.count()
            validar_resumo(resumo,keys,nome)
            resumo,meta=preservar_historico(spark,nome+'_resumo',resumo)
            resumo=resumo.persist(StorageLevel.DISK_ONLY);resumo.count()
            pendentes[nome+'_resumo']=resumo;metadata[nome+'_resumo']=meta
            detalhes[nome]=detalhe
            marca(nome+'_metricas_dq_bootstrap',t)
        t=time.perf_counter()
        geral_detalhe=_contraste(fato,['codigo_municipio']).persist(StorageLevel.MEMORY_AND_DISK)
        geral=_resumo(geral_detalhe,[]).withColumn('chave_alvo',F.lit('TOTAL'))
        geral=aplicar_bootstrap_recorte(spark,'geral',geral_detalhe.withColumn('chave_alvo',F.lit('TOTAL')),geral,['chave_alvo'])
        adicionais={
            'geral_resumo':geral,
            'contraste_municipio':criar_contraste_municipio(fato,municipios),
            'temporal_ano':criar_temporal(fato),
            'lag_resumo':criar_lag_resumo(spark,fato,eventos),
        }
        for nome,novo in adicionais.items():
            novo=novo.persist(StorageLevel.MEMORY_AND_DISK);novo.count()
            if nome=='contraste_municipio':validar_detalhe(novo,KEYS[nome],nome)
            else:validar_resumo(novo,KEYS[nome],nome)
            novo,meta=preservar_historico(spark,nome,novo)
            novo=novo.persist(StorageLevel.DISK_ONLY);novo.count()
            pendentes[nome]=novo;metadata[nome]=meta
        marca('geral_temporal_lags',t)
        t=time.perf_counter();casos,total,extremos=exportar_auditoria(spark,pendentes,detalhes);marca('auditoria',t)
        # Publicação somente depois de todos os DQs e da reconciliação histórica passarem.
        t=time.perf_counter()
        for nome,df in pendentes.items():publicar(nome,df)
        marca('publicacao',t)
        tempos['total_segundos']=round(time.perf_counter()-inicio,3)
        resultado={'status':'PASS','tabelas':metadata,'tempos':tempos,'caso_3_12':casos,
                   'linhas_auditoria':total,'linhas_extremos':extremos,
                   'bootstrap':{'replicas':1000,'seed':20261005,'unidade':'municipio',
                       'cid':'capítulos e grupos','demografia':'TOTAL, CAPITULO_10, GRUPO_J09-J18, GRUPO_J20-J22','geral':True},
                   'powerbi_alterado':False}
        Path(AUDIT+'/resultado_execucao.json').write_text(json.dumps(resultado,ensure_ascii=False,indent=2),encoding='utf-8')
        print('REVISAO_FRIO_QUENTE=PASS',flush=True)
        return 0
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
