"""IC exploratório por reamostragem de municípios, somente recortes declarados."""
import hashlib
import numpy as np

SEED = 20261005
REPLICAS = 1000


def intervalo_municipal(valores, chave, replicas=REPLICAS, seed=SEED):
    x = np.asarray(valores, dtype=float)
    if not len(x) or not np.isfinite(x).all():
        raise ValueError('Distribuição municipal vazia ou não finita')
    # Subseed derivada da chave: resultado independente da ordem/partição Spark.
    subseed = int.from_bytes(hashlib.sha256((str(seed)+'|'+chave).encode()).digest()[:8], 'big')
    rng = np.random.default_rng(subseed)
    pos = (len(x)-1)//2  # mediana empírica inferior, compatível com o resumo Spark
    estimativas = np.empty(replicas, dtype=float)
    for inicio in range(0, replicas, 64):
        fim = min(inicio+64, replicas)
        amostras = x[rng.integers(0, len(x), size=(fim-inicio, len(x)))]
        estimativas[inicio:fim] = np.partition(amostras, pos, axis=1)[:,pos]
    baixo, alto = np.quantile(estimativas, [0.025,0.975])
    return float(baixo), float(alto)


def anexar_bootstrap(spark, detalhe, resumo, chaves, escopo):
    from pyspark.sql import functions as F, types as T
    grupos = (detalhe.filter(escopo & (F.col('contraste_absoluto_valido') == 1))
              .groupBy(*chaves).agg(F.sort_array(F.collect_list(F.struct('codigo_municipio','diferenca_por_100_municipio_dias'))).alias('municipios')))
    linhas = []
    for row in grupos.toLocalIterator():
        chave = '|'.join(str(row[k]) for k in chaves)
        baixo,alto = intervalo_municipal([v.diferenca_por_100_municipio_dias for v in row.municipios], chave)
        linhas.append(tuple(row[k] for k in chaves)+(baixo,alto,REPLICAS,SEED))
    schema = T.StructType([resumo.schema[k] for k in chaves]+[
        T.StructField('ic95_diferenca_por_100_inferior',T.DoubleType()),
        T.StructField('ic95_diferenca_por_100_superior',T.DoubleType()),
        T.StructField('bootstrap_replicas',T.IntegerType()),
        T.StructField('bootstrap_seed',T.LongType())])
    print(f'Bootstrap: {len(linhas)} recortes × {REPLICAS} réplicas; unidade = município', flush=True)
    return (resumo.join(spark.createDataFrame(linhas, schema),chaves,'left')
            .withColumn('bootstrap_status',F.when(F.col('bootstrap_replicas').isNotNull(),'CALCULADO_EXPLORATORIO').otherwise('NAO_CALCULADO_FORA_ESCOPO')))


def aplicar_bootstrap_recorte(spark, nome, detalhe, resumo, chaves):
    """Escopo único, compartilhado pelo runner e por verificações de integração."""
    from pyspark.sql import functions as F
    if nome == 'cid':
        escopo = F.col('nivel_cid').isin('Capítulo','Grupo')
    elif nome == 'demografia':
        escopo = F.col('chave_alvo').isin('TOTAL','CAPITULO_10','GRUPO_J09-J18','GRUPO_J20-J22')
    elif nome == 'geral':
        escopo = F.lit(True)
    else:
        return resumo
    return anexar_bootstrap(spark, detalhe, resumo, chaves, escopo)
