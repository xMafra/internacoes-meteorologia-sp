"""Regra térmica auditada compartilhada por Consumption e comparação experimental.
Baseline fixo 2015–2025; percentis exatos; empates inclusivos e degeneração explícita.
"""
from functools import reduce
from pyspark.sql import functions as F
from src.consumption.dashboard_analytics import _adicionar_metricas,FAIXAS_ETARIAS,SEXOS
BASELINE_INICIO='2015-01-01'
BASELINE_FIM='2025-12-31'
METODOS={'A':'ABSOLUTO_20_25','B':'P20_P80_MUNICIPIO','C':'P20_P80_MUNICIPIO_MES'}
QUANTIS=[.05,.10,.20,.25,.50,.75,.80,.90,.95]
NOMES_Q=['p05','p10','p20','p25','mediana','p75','p80','p90','p95']

def painel_periodo(df):
    return (df.filter(F.to_date('data').between(BASELINE_INICIO,BASELINE_FIM))
            .withColumn('ano',F.year('data')).withColumn('mes',F.month('data')))

def temperatura_valida(df):
    t=F.col('temperatura_media_dia_c')
    valido=t.isNotNull() & ~F.isnan(t) & (F.abs(t)!=float('inf'))
    if 'meteorologia_disponivel' in df.columns:valido=valido & (F.col('meteorologia_disponivel')==1)
    return F.coalesce(valido,F.lit(False))

def calcular_thresholds(df,grupos=('codigo_municipio',)):
    base=painel_periodo(df).filter(temperatura_valida(df))
    t='temperatura_media_dia_c'
    d=base.groupBy(*grupos).agg(F.count('*').alias('dias_temperatura_valida'),
        F.min(t).alias('temperatura_minima'),F.max(t).alias('temperatura_maxima'),
        F.avg(t).alias('temperatura_media'),F.stddev_samp(t).alias('temperatura_desvio_padrao'),
        F.percentile(t,QUANTIS).alias('_quantis'))
    for i,nome in enumerate(NOMES_Q):d=d.withColumn(nome,F.col('_quantis')[i])
    return d.drop('_quantis').withColumn('amplitude_p20_p80',F.col('p80')-F.col('p20')).withColumn('threshold_degenerado',F.col('p20')==F.col('p80'))

def classe_relativa(temperatura,p20,p80,valido):
    # Sem separação entre limites, retiramos todo o estrato do contraste.
    # Os dias válidos continuam contabilizados como intermediários e o flag é explícito.
    return (F.when(~valido,'INVALIDO').when(p20.isNull()|p80.isNull(),'INVALIDO')
            .when(p20==p80,'INTERMEDIARIO').when(temperatura<=p20,'FRIO')
            .when(temperatura>=p80,'QUENTE').otherwise('INTERMEDIARIO'))

def classificar(df,thresholds,thresholds_mes):
    d=painel_periodo(df).withColumn('temperatura_valida',temperatura_valida(df))
    d=d.join(F.broadcast(thresholds.select('codigo_municipio','p20','p80','threshold_degenerado')),'codigo_municipio','left')
    d=d.join(F.broadcast(thresholds_mes.select('codigo_municipio','mes',F.col('p20').alias('p20_mes'),F.col('p80').alias('p80_mes'),
        F.col('threshold_degenerado').alias('threshold_mes_degenerado'))),['codigo_municipio','mes'],'left')
    t=F.col('temperatura_media_dia_c');v=F.col('temperatura_valida')
    return (d.withColumn('classe_A',F.when(~v,'INVALIDO').when(t<20,'FRIO').when(t>=25,'QUENTE').otherwise('INTERMEDIARIO'))
            .withColumn('classe_B',classe_relativa(t,F.col('p20'),F.col('p80'),v))
            .withColumn('classe_C',classe_relativa(t,F.col('p20_mes'),F.col('p80_mes'),v)))

def empilhar_classes(df,metodos=('A','B','C')):
    manter=[c for c in df.columns if not c.startswith('classe_')]
    return (df.select(*manter,F.explode(F.array(*[F.struct(F.lit(m).alias('metodo'),F.col('classe_'+m).alias('classe')) for m in metodos])).alias('_m'))
            .select(*manter,'_m.*'))

def criar_exposicao(classificados,anual=False):
    d=empilhar_classes(classificados)
    keys=['codigo_municipio','metodo']+(['ano'] if anual else [])
    aggs=[F.count('*').alias('municipio_dias_total'),
        F.sum(F.col('temperatura_valida').cast('long')).alias('dias_temperatura_valida'),
        F.sum((~F.col('temperatura_valida')).cast('long')).alias('dias_sem_temperatura')]
    for classe,nome in [('FRIO','frios'),('QUENTE','quentes'),('INTERMEDIARIO','intermediarios')]:
        aggs.append(F.sum((F.col('classe')==classe).cast('long')).alias('dias_'+nome))
        if not anual:aggs.append(F.countDistinct(F.when(F.col('classe')==classe,F.col('ano'))).alias('anos_'+nome))
    if not anual:
        aggs += [F.sum((F.col('temperatura_valida') & (F.col('temperatura_media_dia_c')==F.when(F.col('metodo')=='B',F.col('p20')).when(F.col('metodo')=='C',F.col('p20_mes')))).cast('long')).alias('dias_empatados_p20'),
                 F.sum((F.col('temperatura_valida') & (F.col('temperatura_media_dia_c')==F.when(F.col('metodo')=='B',F.col('p80')).when(F.col('metodo')=='C',F.col('p80_mes')))).cast('long')).alias('dias_empatados_p80'),
                 F.max(F.when(F.col('metodo')=='B',F.col('threshold_degenerado')).when(F.col('metodo')=='C',F.col('threshold_mes_degenerado')).otherwise(False).cast('int')).alias('possui_threshold_degenerado')]
    out=d.groupBy(*keys).agg(*aggs)
    for nome in ['frios','quentes','intermediarios']:
        out=out.withColumn('percentual_dias_'+nome,F.when(F.col('dias_temperatura_valida')>0,100*F.col('dias_'+nome)/F.col('dias_temperatura_valida')))
    return (out.withColumn('elegivel_exposicao',((F.col('dias_frios')>=30)&(F.col('dias_quentes')>=30)).cast('int'))
        .withColumn('motivo_nao_elegivel',F.concat_ws('; ',F.when(F.col('dias_temperatura_valida')==0,'SEM_TEMPERATURA_VALIDA'),
                    F.when(F.col('dias_frios')==0,'SEM_DIAS_FRIOS').when(F.col('dias_frios')<30,'MENOS_30_DIAS_FRIOS'),
                    F.when(F.col('dias_quentes')==0,'SEM_DIAS_QUENTES').when(F.col('dias_quentes')<30,'MENOS_30_DIAS_QUENTES')))
        .withColumn('equilibrio_exposicao',F.when(F.greatest('dias_frios','dias_quentes')>0,F.least('dias_frios','dias_quentes')/F.greatest('dias_frios','dias_quentes'))))

def metricas_geral(classificados,exposicao,anual=False):
    d=empilhar_classes(classificados)
    keys=['codigo_municipio','metodo']+(['ano'] if anual else [])
    nums=d.groupBy(*keys).agg(F.sum(F.when(F.col('classe')=='FRIO',F.col('qtd_internacoes')).otherwise(0)).alias('internacoes_frio'),
        F.sum(F.when(F.col('classe')=='QUENTE',F.col('qtd_internacoes')).otherwise(0)).alias('internacoes_quente'),
        F.sum('qtd_internacoes').alias('internacoes_total'))
    return _adicionar_metricas(exposicao.join(nums,keys)).withColumn('chave_alvo',F.lit('TOTAL'))

def numeradores_cid(eventos,demografia=True,metodos=('A','B'),somente_capitulos_grupos=False):
    demo=['faixa_etaria','sexo','sexo_descricao'] if demografia else []
    dims=[('CAPITULO_','codigo_capitulo'),('GRUPO_','codigo_grupo'),('CATEGORIA_','codigo_categoria')]
    if somente_capitulos_grupos:dims=dims[:2]
    if demografia:dims=[('TOTAL',None),*dims]
    partes=[]
    for prefixo,coluna in dims:
        keys=['codigo_municipio',*demo]+([coluna] if coluna else [])
        aggs=[F.sum('qtd_internacoes').cast('long').alias('internacoes_total')]
        for m in metodos:
            for classe,suf in [('FRIO','frio'),('QUENTE','quente')]:
                aggs.append(F.sum(F.when(F.col('classe_'+m)==classe,F.col('qtd_internacoes')).otherwise(0)).cast('long').alias(suf+'_'+m))
        d=eventos.groupBy(*keys).agg(*aggs)
        d=d.withColumn('chave_alvo',F.concat(F.lit(prefixo),F.col(coluna)) if coluna else F.lit('TOTAL'))
        if coluna:d=d.drop(coluna)
        partes.append(d)
    return reduce(lambda a,b:a.unionByName(b),partes)

def empilhar_numeradores(raw,metodos=('A','B')):
    manter=[c for c in raw.columns if not c.startswith(('frio_','quente_'))]
    return (raw.select(*manter,F.explode(F.array(*[F.struct(F.lit(m).alias('metodo'),F.col('frio_'+m).alias('internacoes_frio'),
           F.col('quente_'+m).alias('internacoes_quente')) for m in metodos])).alias('_m')).select(*manter,'_m.*'))

def detalhe_cid(exposicao,numeradores,alvos):
    nums=numeradores.filter(F.col('chave_alvo')!='TOTAL').groupBy('codigo_municipio','metodo','chave_alvo').agg(
        *[F.sum(c).cast('long').alias(c) for c in ['internacoes_frio','internacoes_quente','internacoes_total']])
    d=exposicao.filter(F.col('metodo').isin('A','B')).select('codigo_municipio','metodo','dias_frios','dias_quentes','municipio_dias_total')
    d=d.crossJoin(alvos.select('chave_alvo')).join(nums,['codigo_municipio','metodo','chave_alvo'],'left')
    return _adicionar_metricas(d.fillna(0,subset=['internacoes_frio','internacoes_quente','internacoes_total']))

def detalhe_demografico(spark,exposicao,numeradores,alvos,nivel='CONJUNTO'):
    es=[(f,of,s,ds,os) for f,of in FAIXAS_ETARIAS for s,ds,os in SEXOS]
    if nivel=='IDADE':es=[(f,of,'TODOS','Todos os sexos',0) for f,of in FAIXAS_ETARIAS]
    if nivel=='SEXO':es=[('Todas as idades',0,s,ds,os) for s,ds,os in SEXOS]
    estratos=spark.createDataFrame(es,'faixa_etaria string, ordem_faixa_etaria int, sexo string, sexo_descricao string, ordem_sexo int')
    targets=alvos.select('chave_alvo').unionByName(spark.createDataFrame([('TOTAL',)],'chave_alvo string'))
    groupdemo=['faixa_etaria','sexo','sexo_descricao'] if nivel=='CONJUNTO' else ['faixa_etaria'] if nivel=='IDADE' else ['sexo','sexo_descricao']
    keys=['codigo_municipio','metodo','chave_alvo',*groupdemo]
    raw=numeradores
    if nivel=='CONJUNTO':raw=raw.filter(F.col('faixa_etaria').isin([f for f,_ in FAIXAS_ETARIAS])&F.col('sexo').isin([s for s,_,_ in SEXOS]))
    nums=raw.groupBy(*keys).agg(*[F.sum(c).cast('long').alias(c) for c in ['internacoes_frio','internacoes_quente','internacoes_total']])
    d=exposicao.filter(F.col('metodo').isin('A','B')).select('codigo_municipio','metodo','dias_frios','dias_quentes','municipio_dias_total')
    return _adicionar_metricas(d.crossJoin(targets).crossJoin(estratos).join(nums,keys,'left').fillna(0,subset=['internacoes_frio','internacoes_quente','internacoes_total'])).withColumn('nivel_demografico',F.lit(nivel))

def rotular_demografia(res):
    mapa_sexo=F.create_map(*[F.lit(v) for par in [('1','Masculino'),('3','Feminino'),('TODOS','Todos os sexos')] for v in par])
    mapa_idade=F.create_map(*[F.lit(v) for par in [*FAIXAS_ETARIAS,('Todas as idades',0)] for v in par])
    return (res.withColumn('nivel_cid',F.when(F.col('chave_alvo')=='TOTAL','Total').otherwise(F.col('nivel_cid')))
        .withColumn('rotulo_alvo',F.when(F.col('chave_alvo')=='TOTAL','Todas as internações').otherwise(F.col('rotulo_alvo')))
        .withColumn('sexo_descricao',F.element_at(mapa_sexo,F.col('sexo')))
        .withColumn('ordem_sexo',F.when(F.col('sexo')=='1',1).when(F.col('sexo')=='3',2).otherwise(0))
        .withColumn('ordem_faixa_etaria',F.element_at(mapa_idade,F.col('faixa_etaria'))))
