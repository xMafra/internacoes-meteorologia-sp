"""Validações das diferenças municipais e de seus resumos; não corrigem dados."""
from pyspark.sql import functions as F

TOL = 1e-10  # tolerância de ponto flutuante, não limiar analítico


def conferir_condicoes(df, condicoes, nome):
    linha = df.agg(*[F.sum(F.when(F.coalesce(c, F.lit(False)), 1).otherwise(0)).alias(k)
                    for k, c in condicoes.items()]).first()
    falhas = {k: int(linha[k] or 0) for k in condicoes if linha[k]}
    print(f"DQ | {nome}: {falhas or 'PASS'}", flush=True)
    if falhas:
        raise ValueError(f"DQ {nome}: {falhas}")


def conferir_chaves(df, chaves, nome):
    if df.groupBy(*chaves).count().filter(F.col('count') != 1).limit(1).count():
        raise ValueError(f"DQ {nome}: chaves duplicadas {chaves}")


def validar_detalhe(df, chaves, nome='municipal'):
    f, q, d = [F.col(x) for x in ['media_frio_municipio','media_quente_municipio','diferenca_absoluta']]
    r = F.col('diferenca_relativa')
    esperado = F.when(d.isNull(), 'NAO_APLICAVEL').when(d > 0, 'FRIO').when(d < 0, 'QUENTE').otherwise('IGUAL')
    c = {
        'municipio_invalido': F.col('codigo_municipio').isNull() | (F.length(F.trim(F.col('codigo_municipio'))) == 0),
        'media_negativa': (f < 0) | (q < 0),
        'absoluta_inconsistente': ~d.eqNullSafe(f-q) & (F.abs(d-(f-q)) > TOL),
        'reescalada_inconsistente': F.abs(F.col('diferenca_por_100_municipio_dias')-100*d) > TOL,
        'relativa_com_denominador_zero': (q == 0) & r.isNotNull(),
        'relativa_inconsistente': (q > 0) & (F.abs(r-d/q) > TOL),
        'frio_zero_inconsistente': ~F.col('media_frio_zero').eqNullSafe(f == 0),
        'quente_zero_inconsistente': ~F.col('media_quente_zero').eqNullSafe(q == 0),
        'ambas_zero_inconsistente': ~F.col('ambas_medias_zero').eqNullSafe((f == 0) & (q == 0)),
        'direcao_inconsistente': ~F.col('direcao_diferenca').eqNullSafe(esperado),
        'elegibilidade_alterada': F.col('elegivel_amostra') != ((F.col('dias_frios') >= 30) & (F.col('dias_quentes') >= 30)).cast('int'),
        'absoluto_valido_inconsistente': F.col('contraste_absoluto_valido') != ((F.col('elegivel_amostra') == 1) & f.isNotNull() & q.isNotNull()).cast('int'),
        'relativo_valido_inconsistente': F.col('contraste_relativo_valido') != ((F.col('elegivel_amostra') == 1) & r.isNotNull()).cast('int'),
        'volume_termico_maior_total': F.col('internacoes_frio')+F.col('internacoes_quente') > F.col('internacoes_total'),
        'media_sem_dias_frio': (F.col('dias_frios') == 0) & f.isNotNull(),
        'media_sem_dias_quente': (F.col('dias_quentes') == 0) & q.isNotNull(),
    }
    for x in ['internacoes_frio','internacoes_quente','internacoes_total','dias_frios','dias_quentes']:
        c[x+'_invalido'] = F.col(x).isNull() | (F.col(x) < 0)
    for x in ['media_frio_municipio','media_quente_municipio','diferenca_absoluta','diferenca_por_100_municipio_dias','diferenca_relativa']:
        c[x+'_nao_finito'] = F.isnan(x) | (F.abs(F.col(x)) == float('inf'))
    # NULL indevido não pode passar apenas porque uma subtração retorna NULL.
    c['absoluta_ausente'] = f.isNotNull() & q.isNotNull() & d.isNull()
    c['reescalada_ausente'] = d.isNotNull() & F.col('diferenca_por_100_municipio_dias').isNull()
    c['relativa_ausente'] = (q > 0) & f.isNotNull() & r.isNull()
    c['media_frio_inconsistente'] = (F.col('dias_frios') > 0) & (f.isNull() | (F.abs(f-F.col('internacoes_frio')/F.col('dias_frios')) > TOL))
    c['media_quente_inconsistente'] = (F.col('dias_quentes') > 0) & (q.isNull() | (F.abs(q-F.col('internacoes_quente')/F.col('dias_quentes')) > TOL))
    conferir_condicoes(df, c, nome)
    conferir_chaves(df, chaves, nome)


def validar_resumo(df, chaves, nome='resumo'):
    n, nr = F.col('municipios_validos_absoluto'), F.col('municipios_validos')
    d = F.col('mediana_diferenca_absoluta_elegiveis')
    esperado = F.when(d.isNull(), 'NAO_APLICAVEL').when(d > 0, 'FRIO').when(d < 0, 'QUENTE').otherwise('IGUAL')
    c = {
        'municipios_invalidos': (n < 0) | (nr < 0) | (nr > n) | (n > F.col('municipios_total')),
        'quartis_fora_ordem': (F.col('q1_diferenca_absoluta_elegiveis') > d) | (d > F.col('q3_diferenca_absoluta_elegiveis')),
        'direcao_inconsistente': ~F.col('direcao_diferenca').eqNullSafe(esperado),
        'medias_negativas': (F.col('mediana_media_frio_elegiveis') < 0) | (F.col('mediana_media_quente_elegiveis') < 0),
        'classes_consistencia_inconsistentes': (F.col('municipios_frio_consistencia')+F.col('municipios_quente_consistencia')+F.col('municipios_pequena_consistencia')) != nr,
        'classes_absolutas_inconsistentes': (F.col('municipios_maior_frio_absoluto')+F.col('municipios_maior_quente_absoluto')+F.col('municipios_medias_iguais_absoluto')) != n,
        'percentuais_consistencia_inconsistentes': (nr > 0) & (F.abs(F.col('percentual_frio_consistencia')+F.col('percentual_quente_consistencia')+F.col('percentual_pequena_consistencia')-100) > TOL),
        'zero_frio_inconsistente': ~F.col('media_frio_zero').eqNullSafe(F.col('mediana_media_frio_elegiveis') == 0),
        'zero_quente_inconsistente': ~F.col('media_quente_zero').eqNullSafe(F.col('mediana_media_quente_elegiveis') == 0),
        'ambas_zero_inconsistente': ~F.col('ambas_medias_zero').eqNullSafe(F.col('media_frio_zero') & F.col('media_quente_zero')),
        'zeros_demais': (F.col('municipios_frio_zero') > n) | (F.col('municipios_quente_zero') > n) | (F.col('municipios_ambas_medias_zero') > n),
    }
    for a,b in [('q1_diferenca_absoluta_elegiveis','q1_diferenca_por_100'),('mediana_diferenca_absoluta_elegiveis','mediana_diferenca_por_100'),('q3_diferenca_absoluta_elegiveis','q3_diferenca_por_100')]:
        c[b+'_inconsistente'] = (F.abs(F.col(b)-100*F.col(a)) > TOL) | (F.col(a).isNotNull() & F.col(b).isNull())
    for x,typ in df.dtypes:
        if typ in ('float','double'):
            c[x+'_nao_finito'] = F.isnan(x) | (F.abs(F.col(x)) == float('inf'))
        if x.startswith(('dias_','internacoes_')):
            c[x+'_negativo_ou_ausente'] = F.col(x).isNull() | (F.col(x) < 0)
    for base in ['absoluto','relativo']:
        c['volume_'+base+'_inconsistente'] = F.col('internacoes_frio_validos_'+base)+F.col('internacoes_quente_validos_'+base) > F.col('internacoes_total_validos_'+base)
    for x in ['municipios_total','municipios_validos','municipios_validos_absoluto']:
        c[x+'_ausente'] = F.col(x).isNull()
    for x in ['q1_diferenca_absoluta_elegiveis','mediana_diferenca_absoluta_elegiveis','q3_diferenca_absoluta_elegiveis','mediana_media_frio_elegiveis','mediana_media_quente_elegiveis']:
        c[x+'_ausente'] = (n > 0) & F.col(x).isNull()
    if 'bootstrap_replicas' in df.columns:
        c['ic_fora_ordem'] = F.col('ic95_diferenca_por_100_inferior') > F.col('ic95_diferenca_por_100_superior')
        c['ic_ausente'] = F.col('bootstrap_replicas').isNotNull() & (F.col('ic95_diferenca_por_100_inferior').isNull() | F.col('ic95_diferenca_por_100_superior').isNull())
    conferir_condicoes(df, c, nome)
    conferir_chaves(df, chaves, nome)


def validar_publicadas(spark, base):
    for nome,chaves in [('cid_resumo',['chave_alvo']),('demografia_resumo',['chave_alvo','faixa_etaria','sexo']),('estratos_resumo',['chave_alvo','nivel_demografico','faixa_etaria','sexo']),('geral_resumo',['chave_alvo']),('temporal_ano',['ano']),('lag_resumo',['chave_alvo','defasagem'])]:
        validar_resumo(spark.read.parquet(f'{base}/{nome}'), chaves, nome)
    validar_detalhe(spark.read.parquet(f'{base}/contraste_municipio'), ['codigo_municipio'], 'contraste_municipio')
