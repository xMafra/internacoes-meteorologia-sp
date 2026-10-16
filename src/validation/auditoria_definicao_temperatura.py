"""Etapa 1B: comparação experimental; nunca publica na Consumption oficial."""
from __future__ import annotations
import json,sys,time,hashlib
from pathlib import Path
from functools import reduce
sys.path.insert(0,str(Path(__file__).resolve().parents[2]))
from pyspark import StorageLevel
from pyspark.sql import SparkSession,DataFrame,Window,functions as F
from src.consumption.dashboard_analytics import (_adicionar_metricas,_resumo,_dimensao_alvos_cid,FAIXAS_ETARIAS,SEXOS)
from src.quality.consumption.fq_checks import conferir_condicoes,conferir_chaves,validar_detalhe,validar_resumo
from src.common.spark_io import destino_parquet_atomico

BASE='/home/jovyan/work/data/consumption'
OUT=Path('/home/jovyan/work/data/validation/auditoria_definicao_temperatura')
DIST_Q=[.05,.10,.25,.50,.75,.90,.95]
STATUS='AUDITORIA_EXPERIMENTAL_NAO_OFICIAL'
from src.consumption.metodologia_termica import (METODOS,QUANTIS,NOMES_Q,painel_periodo,temperatura_valida,calcular_thresholds,classe_relativa,classificar,empilhar_classes,criar_exposicao,metricas_geral,numeradores_cid,empilhar_numeradores,detalhe_cid,detalhe_demografico,rotular_demografia)
from src.quality.consumption.thermal_checks import (validar_thresholds,validar_classificacao,reconciliar_oficial)



























def comparar_resumos(df,chaves,metodos=('A','B')):
    metricas=['mediana_media_frio_elegiveis','mediana_media_quente_elegiveis','mediana_diferenca_absoluta_elegiveis',
        'mediana_diferenca_por_100','q1_diferenca_por_100','q3_diferenca_por_100','mediana_diferenca_relativa','direcao_diferenca',
        'municipios_validos','municipios_validos_absoluto','internacoes_total','dias_frios_total','dias_quentes_total',
        'percentual_frio_consistencia','percentual_quente_consistencia','percentual_pequena_consistencia']
    partes=[df.filter(F.col('metodo')==m).select(*chaves,*[F.col(c).alias(c+'_'+m) for c in metricas]) for m in metodos]
    out=reduce(lambda a,b:a.join(b,chaves,'full'),partes)
    a,b=metodos[:2];va=F.col('mediana_diferenca_por_100_'+a);vb=F.col('mediana_diferenca_por_100_'+b)
    same=F.col('direcao_diferenca_'+a)==F.col('direcao_diferenca_'+b)
    iqr_ambiguo=(F.col('q1_diferenca_por_100_'+a)==0)&(F.col('q3_diferenca_por_100_'+a)==0)&(F.col('q1_diferenca_por_100_'+b)==0)&(F.col('q3_diferenca_por_100_'+b)==0)
    insuf=(F.col('municipios_validos_absoluto_'+a)==0)|(F.col('municipios_validos_absoluto_'+b)==0)|va.isNull()|vb.isNull()|iqr_ambiguo
    parecido=vb.between(F.col('q1_diferenca_por_100_'+a),F.col('q3_diferenca_por_100_'+a)) & va.between(F.col('q1_diferenca_por_100_'+b),F.col('q3_diferenca_por_100_'+b))
    return (out.withColumn('delta_mediana_por_100',vb-va).withColumn('mesma_direcao',same)
        .withColumn('robustez_descritiva',F.when(insuf,'INCONCLUSIVO').when(~same,'SENSIVEL_AO_METODO').when(parecido,'ROBUSTO_DESCRITIVO').otherwise('PARCIALMENTE_ROBUSTO'))
        .withColumn('criterio_robustez',F.lit('Mesma direção e medianas dentro dos IQRs recíprocos; não é teste estatístico')))


def ranks_cid(comparacao):
    d=comparacao.filter(F.col('nivel_cid').isin('Capítulo','Grupo')).filter(F.col('mediana_diferenca_por_100_A').isNotNull()&F.col('mediana_diferenca_por_100_B').isNotNull())
    for m in ['A','B']:
        valor='mediana_diferenca_por_100_'+m
        w=Window.partitionBy('nivel_cid').orderBy(F.col(valor).desc())
        tie=Window.partitionBy('nivel_cid',valor)
        d=d.withColumn('rank_'+m,F.rank().over(w)+(F.count('*').over(tie)-1)/2.0)
    d=d.withColumn('deslocamento_rank',F.abs(F.col('rank_A')-F.col('rank_B')))
    stats=d.groupBy('nivel_cid').agg(F.count('*').alias('itens'),F.corr('rank_A','rank_B').alias('spearman'),
        F.sum(F.col('mesma_direcao').cast('long')).alias('mesma_direcao'),F.sum((~F.col('mesma_direcao')).cast('long')).alias('direcao_diferente'),
        F.sum((F.col('deslocamento_rank')>0).cast('long')).alias('posicao_diferente'),F.percentile('deslocamento_rank',[.5,.9,1.]).alias('deslocamento_p50_p90_max'))
    return d,stats


def municipios_representativos(th,expo):
    n=th.count()
    ordenado=th.withColumn('_ord',F.row_number().over(Window.orderBy('temperatura_media','codigo_municipio')))
    escolhidos=ordenado.filter(F.col('_ord').isin(1,(n+1)//2,n)).withColumn('perfil',
        F.when(F.col('_ord')==1,'MENOR_MEDIA').when(F.col('_ord')==n,'MAIOR_MEDIA').otherwise('MEDIANA_DAS_MEDIAS'))
    return escolhidos.drop('dias_temperatura_valida').join(expo,['codigo_municipio'])




def salvar(df,nome,csv=True,partes=1):
    caminho=(OUT/nome).resolve()
    if OUT.resolve() not in caminho.parents:raise ValueError('Destino fora da auditoria')
    d=df.withColumn('status_uso',F.lit(STATUS))
    with destino_parquet_atomico(caminho) as staging:d.coalesce(partes).write.mode('overwrite').parquet(staging)
    if csv:
        # CSV aceita apenas colunas escalares; arrays ficam em Parquet e no resumo JSON.
        escalares=[c for c,t in d.dtypes if not t.startswith(('array','struct','map'))]
        with destino_parquet_atomico(OUT/(nome+'_csv')) as staging:d.select(*escalares).coalesce(1).write.mode('overwrite').option('header',True).csv(staging)
    print('AUDITORIA GRAVADA | '+nome,flush=True)


def snapshot_producao():
    root=Path(BASE)/'dashboard_analytics'
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}






def resumo_distribuicoes(df,colunas,grupos=()):
    parts=[]
    for c in colunas:
        d=df.groupBy(*grupos).agg(F.min(c).alias('minimo'),F.percentile(c,DIST_Q).alias('_q'),F.max(c).alias('maximo'))
        for i,q in enumerate(['p05','p10','p25','mediana','p75','p90','p95']):d=d.withColumn(q,F.col('_q')[i])
        parts.append(d.drop('_q').withColumn('variavel',F.lit(c)))
    return reduce(lambda a,b:a.unionByName(b),parts)




def main():
    inicio=time.perf_counter();tempos={};result={};antes=snapshot_producao()
    spark=SparkSession.builder.appName('TCC-Etapa1B-Auditoria-Definicao-Temperatura').getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')
    def marca(nome,t):tempos[nome]=round(time.perf_counter()-t,3);print(f'TEMPO {nome}: {tempos[nome]} s',flush=True)
    def rows(df):return [r.asDict(recursive=True) for r in df.collect()]  # somente saídas compactas explicitamente selecionadas
    def cache(df):d=df.persist(StorageLevel.MEMORY_AND_DISK);d.count();return d
    try:
        t=time.perf_counter();ler=lambda n:spark.read.parquet(BASE+'/'+n)
        fonte=ler('fato_municipio_dia')
        cols=['municipio_dia_id','data','codigo_municipio','codigo_estacao','temperatura_media_dia_c','meteorologia_disponivel','qtd_internacoes']
        painel=cache(painel_periodo(fonte.select(*cols)))
        conferir_chaves(painel,['codigo_municipio','data'],'fonte_diaria')
        conferir_condicoes(painel,{'municipio_sem_codigo':F.col('codigo_municipio').isNull(),
            'estacao_ausente_com_temperatura':temperatura_valida(painel)&F.col('codigo_estacao').isNull(),
            'internacoes_invalidas':F.col('qtd_internacoes').isNull()|(F.col('qtd_internacoes')<0)},'fonte')
        dim=ler('dim_municipio');conferir_chaves(dim,['codigo_municipio'],'dim_municipio')
        th=cache(calcular_thresholds(painel));thmes=cache(calcular_thresholds(painel,('codigo_municipio','mes')))
        validar_thresholds(th,['codigo_municipio']);validar_thresholds(thmes,['codigo_municipio','mes'])
        stations=painel.filter(temperatura_valida(painel)).groupBy('codigo_municipio').agg(F.countDistinct('codigo_estacao').alias('estacoes_distintas_validas'))
        th=cache(th.join(dim.select('codigo_municipio','nome_municipio','latitude_municipio','longitude_municipio'),'codigo_municipio').join(stations,'codigo_municipio'))
        salvar(th,'audit_relative_temperature_thresholds');salvar(thmes,'audit_monthly_temperature_thresholds')
        cl=cache(classificar(painel,th,thmes));validar_classificacao(cl)
        expo=cache(criar_exposicao(cl));expo_ano=cache(criar_exposicao(cl,True))
        conferir_condicoes(expo,{'particao_dias':F.col('dias_frios')+F.col('dias_quentes')+F.col('dias_intermediarios')!=F.col('dias_temperatura_valida'),
            'particao_validade':F.col('dias_temperatura_valida')+F.col('dias_sem_temperatura')!=F.col('municipio_dias_total')},'cobertura')
        salvar(expo.join(th.select('codigo_municipio','nome_municipio'),'codigo_municipio'),'audit_exposure_absolute_vs_relative')
        salvar(expo_ano,'audit_exposure_annual')
        dist=cache(resumo_distribuicoes(expo,['percentual_dias_frios','percentual_dias_quentes','equilibrio_exposicao','dias_frios','dias_quentes'],['metodo']))
        dth=cache(resumo_distribuicoes(th,['p20','p80','amplitude_p20_p80','temperatura_media','dias_temperatura_valida']))
        salvar(dist,'audit_exposure_distributions');salvar(dth,'audit_threshold_distributions')
        result['distribuicoes_exposicao']=rows(dist);result['distribuicoes_thresholds']=rows(dth)
        result['fonte']=painel.agg(F.count('*').alias('municipio_dias'),F.sum('qtd_internacoes').alias('internacoes_total'),F.min('data').alias('inicio'),F.max('data').alias('fim'),F.sum(temperatura_valida(painel).cast('long')).alias('dias_temperatura_valida')).first().asDict()
        result['thresholds_degenerados']={'municipio':th.filter('threshold_degenerado').count(),'municipio_mes':thmes.filter('threshold_degenerado').count()}
        result['cobertura']=rows(expo.groupBy('metodo').agg(F.count('*').alias('municipios'),F.sum('elegivel_exposicao').alias('elegiveis'),
            F.sum((F.col('dias_frios')==0).cast('long')).alias('sem_frio'),F.sum((F.col('dias_quentes')==0).cast('long')).alias('sem_quente'),
            F.sum((F.col('dias_frios')<30).cast('long')).alias('frio_menos30'),F.sum((F.col('dias_quentes')<30).cast('long')).alias('quente_menos30'),
            F.sum('dias_frios').alias('dias_frios'),F.sum('dias_quentes').alias('dias_quentes'),F.sum('dias_intermediarios').alias('dias_intermediarios')))
        elig=reduce(lambda a,b:a.join(b,'codigo_municipio'),[expo.filter(F.col('metodo')==m).select('codigo_municipio',F.col('elegivel_exposicao').alias('elegivel_'+m),F.col('motivo_nao_elegivel').alias('motivo_'+m)) for m in METODOS])
        salvar(elig.join(dim.select('codigo_municipio','nome_municipio'),'codigo_municipio'),'audit_eligibility_comparison')
        result['municipios_recuperados']=rows(elig.filter((F.col('elegivel_A')==0)&(F.col('elegivel_B')==1)).join(dim.select('codigo_municipio','nome_municipio'),'codigo_municipio'))
        result['municipios_perdidos']=rows(elig.filter((F.col('elegivel_A')==1)&(F.col('elegivel_B')==0)))
        exemplos=municipios_representativos(th,expo);salvar(exemplos,'audit_representative_municipalities');result['exemplos_municipios']=rows(exemplos)
        matrix=cl.filter('temperatura_valida').groupBy('classe_A','classe_B').count()
        salvar(matrix,'audit_daily_agreement_matrix');salvar(cl.filter('temperatura_valida').groupBy('codigo_municipio','classe_A','classe_B').count(),'audit_daily_agreement_municipal')
        result['matriz_concordancia']=rows(matrix)
        desacordos=[]
        for nome,cond,alvo in [('INTERMEDIARIO_ABSOLUTO_FRIO_RELATIVO',(F.col('classe_A')=='INTERMEDIARIO')&(F.col('classe_B')=='FRIO'),21.),
             ('INTERMEDIARIO_ABSOLUTO_QUENTE_RELATIVO',(F.col('classe_A')=='INTERMEDIARIO')&(F.col('classe_B')=='QUENTE'),24.),
             ('FRIO_ABSOLUTO_COMUM_OU_QUENTE_RELATIVO',(F.col('classe_A')=='FRIO')&(F.col('classe_B').isin('INTERMEDIARIO','QUENTE')),19.)]:
            d=cl.filter(cond).join(dim.select('codigo_municipio','nome_municipio'),'codigo_municipio').orderBy(F.abs(F.col('temperatura_media_dia_c')-alvo),'codigo_municipio','data').limit(3)
            desacordos.append(d.select('codigo_municipio','nome_municipio','data','codigo_estacao','temperatura_media_dia_c','p20','p80','classe_A','classe_B').withColumn('caso',F.lit(nome)))
        discord=reduce(lambda a,b:a.unionByName(b),desacordos);salvar(discord,'audit_disagreement_examples');result['discordancias']=rows(discord)
        salvar(cl.select('codigo_municipio','data','codigo_estacao','temperatura_media_dia_c','temperatura_valida','p20','p80','p20_mes','p80_mes','classe_A','classe_B','classe_C'),'audit_daily_classifications',False,12)
        marca('thresholds_cobertura_classificacao',t)
        t=time.perf_counter()
        geral=cache(metricas_geral(cl,expo));anual=cache(metricas_geral(cl,expo_ano,True))
        validar_detalhe(geral,['codigo_municipio','metodo'],'geral_municipal');validar_detalhe(anual,['codigo_municipio','metodo','ano'],'anual_municipal')
        geral_res=cache(_resumo(geral,['metodo','chave_alvo']));anual_res=cache(_resumo(anual,['metodo','ano','chave_alvo']))
        validar_resumo(geral_res,['metodo'],'geral');validar_resumo(anual_res,['metodo','ano'],'anual')
        salvar(geral_res,'audit_general_results');salvar(anual_res,'audit_annual_results');salvar(comparar_resumos(anual_res,['ano','chave_alvo']),'audit_annual_comparison')
        result['geral']=rows(geral_res);result['anual']=rows(comparar_resumos(anual_res,['ano','chave_alvo']))
        bases_ano=anual.filter(F.col('metodo').isin('A','B')).groupBy('codigo_municipio','ano').agg(F.min('elegivel_amostra').alias('_comum')).filter('_comum=1').select('codigo_municipio','ano')
        anual_comum=cache(_resumo(anual.filter(F.col('metodo').isin('A','B')).join(bases_ano,['codigo_municipio','ano']),['metodo','ano','chave_alvo']))
        salvar(anual_comum,'audit_annual_common_cohort');result['anual_base_comum']=rows(comparar_resumos(anual_comum,['ano','chave_alvo']))
        comums=elig.filter((F.col('elegivel_A')==1)&(F.col('elegivel_B')==1)).select('codigo_municipio')
        geral_comum=cache(_resumo(geral.filter(F.col('metodo').isin('A','B')).join(comums,'codigo_municipio'),['metodo','chave_alvo']))
        salvar(geral_comum,'audit_general_common_cohort');result['geral_base_comum']=rows(geral_comum)
        muni=th
        campos=['dias_frios','dias_quentes','percentual_dias_frios','percentual_dias_quentes','elegivel_amostra','media_frio_municipio','media_quente_municipio','diferenca_por_100_municipio_dias','direcao_diferenca']
        for m in METODOS:
            muni=muni.join(geral.filter(F.col('metodo')==m).select('codigo_municipio',*[F.col(c).alias(c+'_'+m) for c in campos]),'codigo_municipio')
        muni=muni.withColumn('delta_B_A',F.col('diferenca_por_100_municipio_dias_B')-F.col('diferenca_por_100_municipio_dias_A'))
        salvar(muni,'audit_municipal_comparison');result['maiores_mudancas_municipais']=rows(muni.orderBy(F.abs(F.col('delta_B_A')).desc(),'codigo_municipio').limit(10))
        quartis=th.agg(F.percentile('temperatura_media',[.25,.75]).alias('q')).first().q
        territor=muni.withColumn('perfil_termico',F.when(F.col('temperatura_media')<=quartis[0],'QUARTIL_MAIS_FRIO').when(F.col('temperatura_media')>=quartis[1],'QUARTIL_MAIS_QUENTE').otherwise('CENTRAL'))
        territor=territor.groupBy('perfil_termico').agg(F.count('*').alias('municipios'),F.percentile('delta_B_A',[.25,.5,.75]).alias('delta_q1_mediana_q3'),
            F.sum((F.col('direcao_diferenca_A')!=F.col('direcao_diferenca_B')).cast('long')).alias('direcao_diferente'))
        salvar(territor,'audit_thermal_territorial_comparison');result['territorial']=rows(territor)
        # O método A experimental precisa reproduzir todas as métricas oficiais comuns.
        old=ler('dashboard_analytics/'+('sensibilidade_absoluto_contraste_municipio' if (Path(BASE)/'dashboard_analytics/sensibilidade_absoluto_contraste_municipio').exists() else 'contraste_municipio'));nov=geral.filter(F.col('metodo')=='A')
        reconciliar_oficial(old,nov,['codigo_municipio'],'A_municipal_oficial');result['reconciliacao_A_oficial']='PASS'
        marca('resultados_gerais_anuais_territoriais',t)
        t=time.perf_counter()
        dimcid=ler('dim_cid');alvos=_dimensao_alvos_cid(dimcid)
        temp=cl.select('municipio_dia_id','classe_A','classe_B','classe_C')
        campos_eventos=['municipio_dia_id','codigo_municipio','codigo_categoria','faixa_etaria','sexo','sexo_descricao','qtd_internacoes']
        eventos=(ler('fato_municipio_dia_cid').select(*campos_eventos).join(temp,'municipio_dia_id')
                 .join(dimcid.select('codigo_categoria','codigo_capitulo','codigo_grupo'),'codigo_categoria','left').persist(StorageLevel.DISK_ONLY))
        raw=numeradores_cid(eventos).persist(StorageLevel.DISK_ONLY);raw.count()
        nums=empilhar_numeradores(raw).persist(StorageLevel.DISK_ONLY);nums.count()
        marca('numeradores_cid_demografia_compartilhados',t)
        resumo_tabelas=[]
        for nome,criar,keys in [
            ('cid',lambda:detalhe_cid(expo,nums,alvos),['metodo','chave_alvo']),
            ('demografia',lambda:detalhe_demografico(spark,expo,nums,alvos),['metodo','chave_alvo','faixa_etaria','sexo']),
            ('estratos',lambda:detalhe_demografico(spark,expo,nums,alvos,'IDADE').unionByName(detalhe_demografico(spark,expo,nums,alvos,'SEXO')),['metodo','chave_alvo','nivel_demografico','faixa_etaria','sexo'])]:
            t=time.perf_counter();det=criar().persist(StorageLevel.DISK_ONLY);det.count()
            validar_detalhe(det,['codigo_municipio',*keys],nome+'_municipal')
            res=cache(_resumo(det,keys));validar_resumo(res,keys,nome)
            if nome=='cid':
                res=cache(res.join(alvos,'chave_alvo'));cidres=res
                comum=cache(_resumo(det.join(comums,'codigo_municipio'),keys).join(alvos,'chave_alvo'))
                salvar(comum,'audit_cid_common_cohort')
                cmp=cache(comparar_resumos(res,['chave_alvo']).join(alvos,'chave_alvo'));salvar(cmp,'audit_cid_comparison')
                result['cid_principais']=rows(cmp.filter(F.col('chave_alvo').isin('CAPITULO_10','CAPITULO_9','CAPITULO_15','GRUPO_J09-J18','GRUPO_J20-J22','GRUPO_O80-O84')))
                result['cid_robustez']=rows(cmp.groupBy('nivel_cid','robustez_descritiva').count())
                ranking,stats=ranks_cid(cmp);salvar(ranking,'audit_cid_ranking');salvar(stats,'audit_cid_ranking_stats')
                result['ranking']=rows(stats);result['maiores_mudancas_ranking']=rows(ranking.orderBy(F.col('deslocamento_rank').desc(),'chave_alvo').limit(12))
                tops=cidres.filter((F.col('metodo')=='A')&(F.col('nivel_cid')=='Categoria')).orderBy(F.col('internacoes_total').desc(),'chave_alvo').limit(20).select('chave_alvo')
                result['categorias_principais']=rows(cmp.join(tops,'chave_alvo'))
            else:
                if nome=='demografia':res=res.withColumn('nivel_demografico',F.lit('CONJUNTO'))
                res=cache(rotular_demografia(res.join(alvos.select('chave_alvo','nivel_cid','rotulo_alvo'),'chave_alvo','left')))
                demo_cmp=comparar_resumos(res,keys[1:])
                salvar(demo_cmp,'audit_'+nome+'_comparison')
                result[nome+'_total']=rows(demo_cmp.filter(F.col('chave_alvo')=='TOTAL'))
            salvar(res,'audit_'+nome+'_results')
            oficiais={'cid':('cid_resumo',['chave_alvo']),'demografia':('demografia_resumo',['chave_alvo','faixa_etaria','sexo']),
                      'estratos':('estratos_resumo',['chave_alvo','nivel_demografico','faixa_etaria','sexo'])}
            tabela,chaves_oficiais=oficiais[nome]
            reconciliar_oficial(ler('dashboard_analytics/'+('sensibilidade_absoluto_'+tabela if (Path(BASE)/('dashboard_analytics/sensibilidade_absoluto_'+tabela)).exists() else tabela)),res.filter("metodo='A'"),chaves_oficiais,'A_'+nome+'_oficial')
            resumo_tabelas.append(res.withColumn('tabela_origem',F.lit(nome)))
            # Reproduzir exatamente os 38 recortes 3/12 da Etapa 1 em ambos os métodos.
            raros=spark.read.parquet('/home/jovyan/work/data/validation/auditoria_metricas_extremas/caso_3_municipios_12_internacoes')
            orig={'cid':'cid_resumo','demografia':'demografia_resumo','estratos':'estratos_resumo'}[nome]
            rkeys=[k for k in keys if k!='metodo']
            escolhido=raros.filter(F.col('tabela_origem')==orig).select(*rkeys).distinct()
            detalhes_raros=det.join(escolhido,rkeys,'inner')
            salvar(detalhes_raros,'audit_rare_municipal_'+nome,False,2)
            salvar(res.join(escolhido,rkeys,'inner'),'audit_rare_results_'+nome)
            if nome=='demografia':result['caso_A43']=rows(res.filter("chave_alvo='CATEGORIA_A43' AND faixa_etaria='60-79' AND sexo='3'"))
            det.unpersist();marca(nome+'_resultados_dq_auditoria',t)
        t=time.perf_counter()
        todos=cache(reduce(lambda a,b:a.unionByName(b,allowMissingColumns=True),resumo_tabelas))
        zeros=todos.groupBy('metodo').agg(F.count('*').alias('recortes'),
            F.sum((F.col('mediana_media_frio_elegiveis')==0).cast('long')).alias('mediana_frio_zero'),
            F.sum((F.col('mediana_media_quente_elegiveis')==0).cast('long')).alias('mediana_quente_zero'),
            F.sum((F.abs(F.col('mediana_diferenca_relativa')+1)<1e-10).cast('long')).alias('mediana_relativa_menos100'),
            F.sum((F.abs(F.col('mediana_diferenca_relativa')-1)<1e-10).cast('long')).alias('mediana_relativa_mais100'),
            F.sum((F.col('municipios_quente_zero')>0).cast('long')).alias('algum_denominador_zero'),
            F.sum((F.col('municipios_validos')==0).cast('long')).alias('sem_municipio_relativo_valido'),
            F.sum((F.col('municipios_validos').between(1,3)).cast('long')).alias('N_relativo_1_a_3_descritivo'))
        salvar(zeros,'audit_extremes_comparison');result['extremos']=rows(zeros)
        salvar(todos.groupBy('metodo','municipios_validos').count(),'audit_relative_sample_size_distribution')
        salvar(todos.groupBy('metodo','internacoes_total').count(),'audit_event_count_distribution')
        # Método C: capítulos e grupos respiratórios, sem CID × demografia completo.
        keysC=alvos.filter((F.col('nivel_cid')=='Capítulo')|F.col('chave_alvo').isin('GRUPO_J09-J18','GRUPO_J20-J22'))
        rawC=empilhar_numeradores(numeradores_cid(eventos,False,('C',),True),('C',)).join(keysC.select('chave_alvo'),'chave_alvo')
        expC=expo.filter("metodo='C'").select('codigo_municipio','metodo','dias_frios','dias_quentes','municipio_dias_total')
        detC=_adicionar_metricas(expC.crossJoin(keysC.select('chave_alvo')).join(rawC,['codigo_municipio','metodo','chave_alvo'],'left').fillna(0,subset=['internacoes_frio','internacoes_quente','internacoes_total']))
        resC=cache(_resumo(detC,['metodo','chave_alvo']).join(keysC,'chave_alvo'));validar_resumo(resC,['metodo','chave_alvo'],'cid_sazonal')
        salvar(resC,'audit_seasonal_cid_results')
        result['cid_sazonal']=rows(resC)
        result['cid_B_C']=rows(comparar_resumos(cidres.unionByName(resC),['chave_alvo'],('B','C')).join(keysC,'chave_alvo'))
        # Distribuições por mês, estação meteorológica e ano para os três métodos.
        long=empilhar_classes(cl).filter('temperatura_valida')
        meses=long.groupBy('metodo','mes','classe').count().withColumnRenamed('count','dias')
        meses=meses.withColumn('percentual_dentro_mes',100*F.col('dias')/F.sum('dias').over(Window.partitionBy('metodo','mes')))
        meses=meses.withColumn('percentual_exposicao_no_mes',100*F.col('dias')/F.sum('dias').over(Window.partitionBy('metodo','classe')))
        salvar(meses,'audit_seasonality_month');result['sazonalidade_mensal']=rows(meses)
        seasons=long.withColumn('estacao_ano',F.when(F.col('mes').isin(12,1,2),'VERAO').when(F.col('mes').isin(3,4,5),'OUTONO').when(F.col('mes').isin(6,7,8),'INVERNO').otherwise('PRIMAVERA'))
        seasons=seasons.groupBy('metodo','estacao_ano','classe').count().withColumnRenamed('count','dias')
        seasons=seasons.withColumn('percentual_exposicao_na_estacao',100*F.col('dias')/F.sum('dias').over(Window.partitionBy('metodo','classe')))
        salvar(seasons,'audit_seasonality_season');result['sazonalidade_estacoes']=rows(seasons)
        porano=long.groupBy('metodo','ano','classe').count().withColumnRenamed('count','dias');salvar(porano,'audit_seasonality_year');result['sazonalidade_anual']=rows(porano)
        # Verifica cardinalidades esperadas e preservação byte a byte da produção.
        assert cidres.count()==2*alvos.count()
        assert todos.count()==2*(alvos.count()+(alvos.count()+1)*17)
        depois=snapshot_producao();assert antes==depois,'PRODUCAO ALTERADA'
        result['producao_sha256_preservada']={'arquivos':len(antes),'alterados':[]}
        (OUT/'production_hashes.json').write_text(json.dumps(antes,ensure_ascii=False,indent=2),encoding='utf-8')
        marca('extremos_sazonalidade_verificacao',t)
        result['status']='PASS';result['status_uso']=STATUS;result['metodos']=METODOS;result['tempos']=tempos
        result['tempo_total_segundos']=round(time.perf_counter()-inicio,3)
        (OUT/'resultados_auditoria.json').write_text(json.dumps(result,ensure_ascii=False,indent=2,default=str),encoding='utf-8')
        print('AUDITORIA_DEFINICAO_TEMPERATURA=PASS',flush=True)
        return 0
    finally:spark.stop()


if __name__=='__main__':raise SystemExit(main())
