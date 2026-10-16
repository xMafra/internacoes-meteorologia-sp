"""Gráficos estáticos e relatório da Etapa 1B; lê somente saídas de auditoria."""
from pathlib import Path
import json,math
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter

ROOT=Path(__file__).resolve().parents[2]
AUDIT=ROOT/'data/validation/auditoria_definicao_temperatura'
FIG=AUDIT/'figuras'
DOC=ROOT/'docs/Relatorio_Auditoria_Definicao_Temperatura.md'
COLORS={'A':'#E68A32','B':'#327EC2','C':'#07858C'}
LABELS={'A':'Absoluto <20 / ≥25 °C','B':'P20/P80 municipal','C':'P20/P80 município × mês'}
MESES=['jan','fev','mar','abr','mai','jun','jul','ago','set','out','nov','dez']
KEYS=['CAPITULO_10','CAPITULO_9','GRUPO_J09-J18','GRUPO_J20-J22','CAPITULO_15','GRUPO_O80-O84']
CID_LABELS=['Cap. X — Respiratórias','Cap. IX — Circulatórias','J09–J18 — Influenza e pneumonia',
            'J20–J22 — Infecções inferiores','Cap. XV — Gravidez, parto e puerpério','O80–O84 — Parto']


def csv(nome):
    files=sorted((AUDIT/(nome+'_csv')).glob('part-*.csv'))
    if not files:raise FileNotFoundError(nome)
    return pd.concat([pd.read_csv(p,dtype={'codigo_municipio':str,'sexo':str}) for p in files],ignore_index=True)


def fmt(x,n=2):
    if x is None or (isinstance(x,(float,np.floating)) and not math.isfinite(x)):return 'não aplicável'
    if isinstance(x,(bool,np.bool_)):return 'sim' if x else 'não'
    if isinstance(x,(int,np.integer)):return f'{x:,}'.replace(',','.')
    if isinstance(x,(float,np.floating)):return f'{x:,.{n}f}'.replace(',','X').replace('.',',').replace('X','.')
    return str(x)


def tabela(headers,linhas):
    return '\n'.join(['| '+' | '.join(headers)+' |','|'+'|'.join(['---']*len(headers))+'|',
        *['| '+' | '.join(str(x).replace('|','/') for x in linha)+' |' for linha in linhas]])


def finish(fig,nome,rodape='Auditoria experimental • SP • 2015–2025 • comparação descritiva'):
    fig.text(.015,.015,rodape,fontsize=9,color='#596F84')
    fig.tight_layout(rect=(0,.045,1,.99))
    fig.savefig(FIG/(nome+'.png'),dpi=160,facecolor='white')
    fig.savefig(FIG/(nome+'.svg'),facecolor='white')
    plt.close(fig)


def graficos():
    FIG.mkdir(parents=True,exist_ok=True)
    plt.rcParams.update({'font.family':'DejaVu Sans','font.size':11,'axes.spines.top':False,'axes.spines.right':False,
        'axes.titleweight':'bold','axes.titlecolor':'#18334A','axes.labelcolor':'#18334A','xtick.color':'#435D73','ytick.color':'#435D73'})
    expo=csv('audit_exposure_absolute_vs_relative');a=expo[expo.metodo=='A']
    for campo,nome,title,expr,color in [('percentual_dias_frios','01_dias_frios_absolutos','Dias frios absolutos: cobertura entre municípios','T <20 °C',COLORS['B']),
        ('percentual_dias_quentes','02_dias_quentes_absolutos','Dias quentes absolutos: cobertura entre municípios','T ≥25 °C',COLORS['A'])]:
        v=a[campo].dropna();fig,ax=plt.subplots(figsize=(10,5.5));ax.hist(v,bins='sturges',color=color,alpha=.88,edgecolor='white')
        med=float(v.median());ax.axvline(med,color='#18334A',ls='--',lw=1.5,label='Mediana: '+fmt(med)+'%')
        ax.set(title=title,xlabel='Porcentagem dos dias válidos com '+expr,ylabel='Número de municípios',xlim=(0,100))
        ax.legend(frameon=False);ax.grid(axis='y',alpha=.16)
        finish(fig,nome,'Denominador: dias com temperatura válida de cada município • N='+str(len(v)))
    mun=csv('audit_municipal_comparison');comum=mun[(mun.elegivel_amostra_A==1)&(mun.elegivel_amostra_B==1)]
    x=comum.diferenca_por_100_municipio_dias_A;y=comum.diferenca_por_100_municipio_dias_B
    fig,axs=plt.subplots(1,2,figsize=(12,5.4));sc=None
    for ax,title in zip(axs,['Todos os municípios elegíveis em ambos','Ampliação da faixa central P05–P95']):
        sc=ax.scatter(x,y,c=comum.temperatura_media,cmap='viridis',s=18,alpha=.72,edgecolors='none')
        ax.axhline(0,color='#8EA0B0',lw=.7);ax.axvline(0,color='#8EA0B0',lw=.7);ax.grid(alpha=.12)
        ax.set(title=title,xlabel='Diferença por 100 — absoluto',ylabel='Diferença por 100 — P20/P80')
    lo=min(x.min(),y.min());hi=max(x.max(),y.max());axs[0].plot([lo,hi],[lo,hi],color='#8EA0B0',ls='--',lw=1)
    axs[1].set_xlim(*np.quantile(x,[.05,.95]));axs[1].set_ylim(*np.quantile(y,[.05,.95]))
    fig.colorbar(sc,ax=axs[0],label='Média térmica municipal (°C)',fraction=.045,pad=.025)
    finish(fig,'03_comparacao_municipal',f'Cada ponto representa um município • N={len(comum)} • a ampliação à direita não exclui dados da análise')
    th=csv('audit_relative_temperature_thresholds').sort_values(['mediana','codigo_municipio']);ordem=np.arange(1,len(th)+1)
    fig,ax=plt.subplots(figsize=(11,5.5));ax.fill_between(ordem,th.p20,th.p80,color='#DCECF0',alpha=.85)
    ax.plot(ordem,th.p20,color=COLORS['B'],label='P20 municipal');ax.plot(ordem,th.p80,color=COLORS['A'],label='P80 municipal')
    ax.plot(ordem,th.mediana,color='#18334A',lw=1,label='Mediana municipal')
    ax.axhline(20,color='#8294A4',ls='--',lw=1,label='20 °C');ax.axhline(25,color='#8294A4',ls=':',lw=1,label='25 °C')
    ax.set(title='Limites relativos variam com o padrão térmico municipal',xlabel='Municípios ordenados pela temperatura mediana',ylabel='Temperatura média diária (°C)')
    ax.legend(ncol=3,frameon=False);ax.grid(axis='y',alpha=.15);finish(fig,'04_percentis_municipais')
    cid=csv('audit_cid_results');cid=pd.concat([cid,csv('audit_seasonal_cid_results')],ignore_index=True)
    fig,ax=plt.subplots(figsize=(11,7));ys=np.arange(len(KEYS));offset={'A':-.23,'B':0,'C':.23}
    for m in ['A','B','C']:
        dm=cid[cid.metodo==m].set_index('chave_alvo');primeiro=True
        for i,k in enumerate(KEYS):
            if k not in dm.index:continue
            row=dm.loc[k];v=row.mediana_diferenca_por_100;low=row.q1_diferenca_por_100;high=row.q3_diferenca_por_100
            yy=i+offset[m];ax.errorbar(v,yy,xerr=[[v-low],[high-v]],fmt='o',ms=6,capsize=3,lw=1.6,color=COLORS[m],label=LABELS[m] if primeiro else None)
            ax.annotate(fmt(v,4 if 0<abs(v)<.005 else 2),xy=(v,yy),xytext=(7,4),textcoords='offset points',fontsize=8,color=COLORS[m]);primeiro=False
    ax.axvline(0,color='#8EA0B0',lw=1);ax.set_yticks(ys,CID_LABELS);ax.invert_yaxis()
    ax.set(title='Diagnósticos principais: magnitude e distribuição municipal',xlabel='Diferença de internações por 100 município-dias')
    ax.legend(frameon=False,loc='lower right');ax.grid(axis='x',alpha=.12)
    finish(fig,'05_diagnosticos_principais','Pontos: medianas • linhas: Q1–Q3 dos municípios • intervalos não são IC • C somente no escopo declarado')
    season=csv('audit_seasonality_month');fig,axs=plt.subplots(1,2,figsize=(12,5.4),sharey=True)
    for ax,classe,title in zip(axs,['FRIO','QUENTE'],['Quando ocorrem os dias classificados como frios?','Quando ocorrem os dias classificados como quentes?']):
        for m in ['A','B','C']:
            d=season[(season.metodo==m)&(season.classe==classe)].sort_values('mes')
            ax.plot(d.mes,d.percentual_exposicao_no_mes,'o-',color=COLORS[m],label=LABELS[m],ms=4)
        ax.set(title=title,xlabel='Mês',xticks=range(1,13),xticklabels=MESES);ax.set_ylim(bottom=0);ax.grid(alpha=.14)
    axs[0].set_ylabel('% dos dias daquela condição no mês');axs[1].legend(frameon=False,fontsize=9)
    finish(fig,'06_sazonalidade','Cada linha soma 100% ao longo dos 12 meses • distribuição de exposição, não de internações')
    ano=csv('audit_annual_results');fig,ax=plt.subplots(figsize=(11,5.5))
    for m in ['A','B','C']:
        d=ano[ano.metodo==m].sort_values('ano');ax.plot(d.ano,d.mediana_diferenca_por_100,'o-',color=COLORS[m],label=LABELS[m])
    ax.axhline(0,color='#8EA0B0',lw=1);ax.set(title='Contrastes anuais dependem da definição térmica',xlabel='Ano',ylabel='Mediana da diferença por 100 município-dias',xticks=range(2015,2026))
    ax.legend(frameon=False);ax.grid(alpha=.14);finish(fig,'07_resultados_anuais','Percentis fixos em 2015–2025 • elegibilidade de 30 dias por condição avaliada em cada ano')


def relatorio():
    r=json.loads((AUDIT/'resultados_auditoria.json').read_text(encoding='utf-8'))
    assert r['status']=='PASS'
    gerais={v['metodo']:v for v in r['geral']};cob={v['metodo']:v for v in r['cobertura']}
    dist={(d.get('metodo'),d['variavel']):d for d in r['distribuicoes_exposicao']}
    thdist={d['variavel']:d for d in r['distribuicoes_thresholds']}
    cid=csv('audit_cid_results');cidC=csv('audit_seasonal_cid_results');cid3=pd.concat([cid,cidC],ignore_index=True)
    methods=['A','B','C'];secs=[]
    def add(s):secs.append(s)
    def img(nome):return f'![{nome}](../data/validation/auditoria_definicao_temperatura/figuras/{nome}.png)'
    add('# Auditoria da definição térmica — limites absolutos, percentis municipais e percentis mensais\n\nEtapa 1B • 05/10/2026 • evidência local • 2015–2025 • todas as saídas novas são experimentais. A metodologia oficial e o Power BI foram preservados.\n')
    add('## 1. Motivação\n\nA comparação intramunicipal evita que uma grande cidade domine a mediana estadual, mas não torna os limites 20/25 °C equivalentes climaticamente. A mesma temperatura pode ocupar posições diferentes nas distribuições locais. Esta auditoria compara cobertura, distribuição de exposições e sensibilidade dos contrastes, sem procurar um método que produza um resultado previamente desejado.\n')
    add('## 2. Método absoluto atual\n\nA: frio T<20 °C; quente T≥25 °C; intermediário 20≤T<25 °C. O intervalo é contínuo: 24,99 °C continua intermediário. Elegibilidade: ≥30 dias em cada condição. O método descreve exposições em uma escala física comum; não garante que os dias tenham a mesma excepcionalidade local.\n')
    add(f"Não há município sem dias frios nem sem dias quentes no período completo. Há {cob['A']['frio_menos30']} com menos de 30 dias frios e {cob['A']['quente_menos30']} com menos de 30 dias quentes. 'Poucos' é mostrado pela distribuição e pelo critério já existente de 30 dias; não foi inventado novo corte.\n")
    add(tabela(['Distribuição entre municípios','Mínimo','P05','P10','P25','Mediana','P75','P90','P95','Máximo'],
        [[label,*[fmt(dist[('A',c)][v]) for v in ['minimo','p05','p10','p25','mediana','p75','p90','p95','maximo']]]
         for label,c in [('% dias frios','percentual_dias_frios'),('% dias quentes','percentual_dias_quentes'),('Dias frios','dias_frios'),('Dias quentes','dias_quentes'),('Equilíbrio min(F,Q)/max(F,Q)','equilibrio_exposicao')]]))
    add(img('01_dias_frios_absolutos'));add(img('02_dias_quentes_absolutos'))
    add('## 3. Método P20/P80 municipal e implementação\n\nB: percentis da temperatura média diária válida de cada município no período 01/01/2015–31/12/2025. Frio T≤P20; quente T≥P80. Empates são incluídos integralmente, sem amostragem ou exclusão aleatória. A faixa restante é intermediária. A temperatura provém da série estação–município já atribuída no painel; nenhuma temperatura foi inferida das internações. Os sete dias técnicos de 2014 não entram nos percentis.\n')
    add('Os thresholds usam `pyspark.sql.functions.percentile`, exato e nativo Spark 3.5, com interpolação linear na posição (N−1)p. Não foi usado `percentile_approx` para os thresholds. O número limitado de dias por município torna o cálculo exato viável. Os resumos de internações reutilizam `percentile_approx(...,10000)` da Etapa 1 para preservar sua convenção empírica; erro de rank declarado de 1/10000, sem adicionar nada ao numerador ou denominador. Threshold térmico interpolado e mediana empírica das diferenças são operações distintas. [Documentação Spark: percentil exato](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile.html), [implementação da interpolação](https://github.com/apache/spark/blob/v3.5.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/percentiles.scala), [percentil aproximado](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html).\n')
    add(f"Foram observados {fmt(r['thresholds_degenerados']['municipio'])} municípios e {fmt(r['thresholds_degenerados']['municipio_mes'])} estratos município×mês com P20=P80. A política explícita para um estrato degenerado é classificá-lo como intermediário, marcar o flag e não formar contrastes frio/quente nesse estrato. Os dias continuam contabilizados na cobertura; não se permite dupla classificação. O teste sintético de ausência de variação cobre essa exceção. Nos dados reais ela não foi necessária.\n")
    fonte=r['fonte'];invalid=fonte['municipio_dias']-fonte['dias_temperatura_valida']
    add(f"Fonte: {fmt(fonte['municipio_dias'])} município-dias; {fmt(fonte['dias_temperatura_valida'])} com temperatura válida; {fmt(invalid)} sem temperatura válida ({fmt(100*invalid/fonte['municipio_dias'])}%). Validade exige temperatura conhecida e finita e registro meteorológico disponível no painel. Não foi criada exigência de 24 observações horárias. Os totais de {fmt(fonte['internacoes_total'])} internações abrangem todo o período, inclusive dias sem meteorologia; os numeradores térmicos incluem somente dias da respectiva condição. Ausência de temperatura nunca vira zero internação.\n")
    add('## 4. Cobertura e elegibilidade\n')
    add(tabela(['Método','Municípios elegíveis','Não elegíveis','Dias frios','Dias quentes','Intermediários'],
        [[LABELS[m],fmt(cob[m]['elegiveis']),fmt(cob[m]['municipios']-cob[m]['elegiveis']),fmt(cob[m]['dias_frios']),fmt(cob[m]['dias_quentes']),fmt(cob[m]['dias_intermediarios'])] for m in methods]))
    add(f"B recupera {len(r['municipios_recuperados'])} municípios e perde {len(r['municipios_perdidos'])}. A cobertura absoluta já é {fmt(100*cob['A']['elegiveis']/cob['A']['municipios'])}% dos municípios; o benefício de cobertura de B é pequeno em número, embora a comparabilidade da fração de exposição mude. Os recuperados são:\n")
    add(tabela(['Município','Motivo no método A'],[[v['nome_municipio'],v['motivo_A']] for v in r['municipios_recuperados']]))
    add(tabela(['Método','% frio: mínimo/mediana/máximo','% quente: mínimo/mediana/máximo','Equilíbrio: mínimo/mediana/máximo'],
        [[LABELS[m],*[ ' / '.join(fmt(dist[(m,c)][v]) for v in ['minimo','mediana','maximo']) for c in ['percentual_dias_frios','percentual_dias_quentes','equilibrio_exposicao']]] for m in methods]))
    exp=csv('audit_exposure_absolute_vs_relative')
    add('A diferença em relação a exatamente 20% está publicada por município. Em B, os intervalos observados para frio e quente na tabela acima incluem os empates nos limites. C também inclui os empates por mês; pequenas diferenças na quantidade de dias de cada mês e no tamanho amostral impedem exigir exatamente 20%. `dias_empatados_p20` e `dias_empatados_p80` permitem reproduzir os casos, sem eliminar observações.\n')
    add('## 5. Distribuição térmica municipal\n')
    add(tabela(['Variável','Mínimo','P05','P10','P25','Mediana','P75','P90','P95','Máximo'],
        [[c,*[fmt(thdist[c][v]) for v in ['minimo','p05','p10','p25','mediana','p75','p90','p95','maximo']]] for c in ['p20','p80','amplitude_p20_p80','temperatura_media','dias_temperatura_valida']]))
    add('A tabela municipal completa também fornece mínimo, P05/P10/P20/P25, mediana, P75/P80/P90/P95, máximo, média, desvio-padrão amostral, nome, coordenadas e número de estações distintas válidas. Nenhuma estação foi reassociada usando a referência de 2025 para anos anteriores. Mudanças históricas de estação continuam nas séries e limitam sua interpretação como climatologia medida exatamente em cada município. Municípios atendidos pela mesma estação podem compartilhar a série meteorológica.\n')
    add(img('04_percentis_municipais'))
    add('Exemplos escolhidos por ordenação crescente da média térmica: primeira posição, posição central e última posição; desempate pelo código IBGE. A escolha não usa internações ou a direção do resultado.\n')
    exemplos=[v for v in r['exemplos_municipios'] if v['metodo']=='A']
    mapa_exp={(v['codigo_municipio'],v['metodo']):v for v in r['exemplos_municipios']}
    add(tabela(['Perfil','Município','Média °C','P20','P80','% frio A','% quente A','Dias F/Q A','Dias F/Q B'],
        [[v['perfil'],v['nome_municipio'],fmt(v['temperatura_media']),fmt(v['p20']),fmt(v['p80']),fmt(v['percentual_dias_frios']),fmt(v['percentual_dias_quentes']),
          f"{fmt(v['dias_frios'])}/{fmt(v['dias_quentes'])}",f"{fmt(mapa_exp[(v['codigo_municipio'],'B')]['dias_frios'])}/{fmt(mapa_exp[(v['codigo_municipio'],'B')]['dias_quentes'])}"] for v in exemplos]))
    add('## 6. Concordância entre classificações\n')
    matriz={(v['classe_A'],v['classe_B']):v['count'] for v in r['matriz_concordancia']};cls=['FRIO','INTERMEDIARIO','QUENTE']
    add(tabela(['A \\ B','Frio relativo','Intermediário relativo','Quente relativo'],[[a,*[fmt(matriz.get((a,b),0)) for b in cls]] for a in cls]))
    concord=sum(matriz.get((c,c),0) for c in cls);total=fonte['dias_temperatura_valida']
    add(f"Concordância de rótulos: {fmt(concord)} de {fmt(total)} dias válidos ({fmt(100*concord/total)}%). Dos intermediários absolutos, {fmt(matriz.get(('INTERMEDIARIO','FRIO'),0))} tornam-se relativamente frios e {fmt(matriz.get(('INTERMEDIARIO','QUENTE'),0))} relativamente quentes. Concordância não é acurácia: nenhum dos métodos é uma classificação verdadeira usada como referência.\n")
    add(tabela(['Caso','Município','Data','Temperatura °C','P20/P80','A','B'],
        [[v['caso'],v['nome_municipio'],v['data'],fmt(v['temperatura_media_dia_c']),f"{fmt(v['p20'])}/{fmt(v['p80'])}",v['classe_A'],v['classe_B']] for v in r['discordancias']]))
    add('Os exemplos são as observações existentes mais próximas de 21, 24 e 19 °C nos três tipos de discordância pedidos, com desempate por município/data. Temperaturas mostradas são reais; não são valores substituídos para se ajustar ao exemplo.\n')
    add('## 7. Resultados gerais\n\nA unidade continua municipal: Fm=eventos frios/dias frios; Qm=eventos quentes/dias quentes; Dm=Fm−Qm; D100m=100Dm. Somente depois são calculadas medianas e quartis dos municípios elegíveis. A reescala não é taxa populacional. Rm=Dm/Qm se Qm>0, senão NULL. Fm=Qm=0 produz D=0, com flags, e não um percentual artificial.\n')
    campos=[('Municípios válidos absolutos','municipios_validos_absoluto',0),('Municípios válidos relativos','municipios_validos',0),
        ('Mediana da média fria','mediana_media_frio_elegiveis',4),('Mediana da média quente','mediana_media_quente_elegiveis',4),
        ('Mediana D por município-dia','mediana_diferenca_absoluta_elegiveis',6),('Mediana D100','mediana_diferenca_por_100',4),
        ('Q1 de D100','q1_diferenca_por_100',4),('Q3 de D100','q3_diferenca_por_100',4),('Mediana R (%)','mediana_diferenca_relativa',4),
        ('% municípios frio — consistência','percentual_frio_consistencia',2),('% municípios quente — consistência','percentual_quente_consistencia',2),
        ('% diferença pequena — consistência','percentual_pequena_consistencia',2),('Internações totais no período','internacoes_total',0)]
    add(tabela(['Métrica','A — absoluto','B — municipal','C — municipal × mês'],
        [[label,*[fmt(gerais[m][c]*(100 if c=='mediana_diferenca_relativa' else 1),n) for m in methods]] for label,c,n in campos]))
    add('A direção da mediana usa D absoluto. A consistência conserva as classes exclusivas FRIO (>+1%), QUENTE (<−1%) e PEQUENA (−1% a +1%, inclusive) na base com R definido. Há também contagens de sinais absolutos estritos. As bases estão separadas; a mediana das diferenças não é a diferença das medianas das médias.\n')
    comum={v['metodo']:v for v in r['geral_base_comum']}
    add(f"Na mesma base de municípios elegíveis em A e B (N={fmt(comum['A']['municipios_validos_absoluto'])}), D100 é {fmt(comum['A']['mediana_diferenca_por_100'],4)} em A e {fmt(comum['B']['mediana_diferenca_por_100'],4)} em B. Isso permite avaliar a classificação sem atribuir toda a diferença aos cinco municípios recuperados.\n")
    add(f"A conclusão de contraste mediano geral pequeno em A/B permanece descritivamente compatível com os dados: R mediano passa de {fmt(100*gerais['A']['mediana_diferenca_relativa'])}% para {fmt(100*gerais['B']['mediana_diferenca_relativa'])}%, e as medianas D100 permanecem dentro dos IQRs recíprocos, atravessados por zero. Não foi definido limiar clínico ou de relevância substantiva. Em C, R é {fmt(100*gerais['C']['mediana_diferenca_relativa'])}% e a magnitude D100 é {fmt(abs(gerais['C']['mediana_diferenca_por_100']/gerais['A']['mediana_diferenca_por_100']))} vezes a de A: a direção permanece, mas não cabe estender a mesma magnitude às três perguntas.\n")
    add(img('03_comparacao_municipal'))
    add('## 8. Resultados anuais\n\nOs percentis permanecem fixos no período 2015–2025; não são recalibrados a cada ano. A elegibilidade de 30 dias em cada condição é reavaliada anualmente. Foram publicados os resumos, suas bases anuais e a comparação usando somente municípios elegíveis em ambos no respectivo ano.\n')
    ar=csv('audit_annual_results');amap={(int(v.ano),v.metodo):v for _,v in ar.iterrows()}
    add(tabela(['Ano','D100 A','D100 B','D100 C','Direção A/B','N absoluto A/B/C','Robustez A/B'],
        [[v['ano'],fmt(amap[(v['ano'],'A')].mediana_diferenca_por_100,3),fmt(amap[(v['ano'],'B')].mediana_diferenca_por_100,3),fmt(amap[(v['ano'],'C')].mediana_diferenca_por_100,3),
          v['direcao_diferenca_A']+'/'+v['direcao_diferenca_B'],'/'.join(fmt(amap[(v['ano'],m)].municipios_validos_absoluto,0) for m in methods),v['robustez_descritiva']] for v in sorted(r['anual'],key=lambda x:x['ano'])]))
    mud=[str(v['ano']) for v in r['anual'] if v['mesma_direcao'] is False]
    add('Anos com direção diferente entre A e B: '+(', '.join(mud) if mud else 'nenhum')+'. Magnitudes, consistência e elegibilidade completas estão nas saídas anuais; o gráfico não substitui essas bases.\n')
    add(img('07_resultados_anuais'))
    expano=csv('audit_exposure_annual');anoexp=expano.groupby(['metodo','ano'])[['dias_temperatura_valida','dias_frios','dias_quentes','elegivel_exposicao']].sum().reset_index()
    b=anoexp[anoexp.metodo=='B'];b=b.assign(pct_frio=100*b.dias_frios/b.dias_temperatura_valida,pct_quente=100*b.dias_quentes/b.dias_temperatura_valida)
    add('Estabilidade da exposição de B ao longo dos anos, usando o mesmo baseline:')
    add(tabela(['Ano','% frio B','% quente B','Municípios elegíveis B'],[[int(v.ano),fmt(v.pct_frio),fmt(v.pct_quente),fmt(v.elegivel_exposicao,0)] for _,v in b.iterrows()]))
    add('Extremos fixos do período completo podem variar de frequência entre anos. A comparação não separa tendências temporais, mudanças na cobertura meteorológica ou a pandemia de outros padrões associados às temperaturas.\n')
    add('## 9. Diagnósticos CID e ranking\n\nA e B foram calculados para todos os 2.077 capítulos/grupos/categorias da dimensão, sem excluir raros. As 20 categorias com mais internações no período foram selecionadas para leitura detalhada por contagem e desempate pelo código, independentemente do resultado térmico. C cobre todos os capítulos e os dois grupos respiratórios prioritários.\n')
    cmap={(row.metodo,row.chave_alvo):row for _,row in cid3.iterrows()}
    add(tabela(['Diagnóstico','D100 A','D100 B','D100 C','R A (%)','R B (%)','% frio A/B','N absoluto A/B','Robustez A/B'],
        [[v['rotulo_alvo'],fmt(v['mediana_diferenca_por_100_A'],4),fmt(v['mediana_diferenca_por_100_B'],4),fmt(cmap[('C',v['chave_alvo'])].mediana_diferenca_por_100,4) if ('C',v['chave_alvo']) in cmap else 'fora do escopo',
          fmt(100*v['mediana_diferenca_relativa_A'],3) if v['mediana_diferenca_relativa_A'] is not None else 'NULL',fmt(100*v['mediana_diferenca_relativa_B'],3) if v['mediana_diferenca_relativa_B'] is not None else 'NULL',
          fmt(v['percentual_frio_consistencia_A'])+'/'+fmt(v['percentual_frio_consistencia_B']),fmt(v['municipios_validos_absoluto_A'])+'/'+fmt(v['municipios_validos_absoluto_B']),v['robustez_descritiva']] for v in r['cid_principais']]))
    add(img('05_diagnosticos_principais'))
    resp=['CAPITULO_10','GRUPO_J09-J18','GRUPO_J20-J22']
    add('### Respiratórias: comparação explícita\n')
    add(tabela(['Recorte','Método','Direção','D100','R (%)','% frio','% quente','% pequena','N absoluto','N relativo'],
        [[cmap[(m,k)].rotulo_alvo,m,cmap[(m,k)].direcao_diferenca,fmt(cmap[(m,k)].mediana_diferenca_por_100,4),fmt(100*cmap[(m,k)].mediana_diferenca_relativa,3) if pd.notna(cmap[(m,k)].mediana_diferenca_relativa) else 'NULL',
          fmt(cmap[(m,k)].percentual_frio_consistencia),fmt(cmap[(m,k)].percentual_quente_consistencia),fmt(cmap[(m,k)].percentual_pequena_consistencia),fmt(cmap[(m,k)].municipios_validos_absoluto,0),fmt(cmap[(m,k)].municipios_validos,0)] for k in resp for m in methods]))
    add('**O padrão respiratório persiste em A/B nos três recortes prioritários, mas não em C.** Capítulo X, J09–J18 e J20–J22 têm medianas positivas em A/B e negativas em C. Em J20–J22, D100 de C é −0,000929, quase zero; a classificação de direção usa o valor sem arredondamento. A consistência no frio do Capítulo X passa de 83,13% em A para 85,27% em B e 35,50% em C. Portanto, o achado é robusto descritivamente entre A/B e sensível quando a comparação se restringe à excepcionalidade dentro do mês. Isso não demonstra que a estação explica toda a diferença nem identifica associação causal.\n')
    add(tabela(['Nível','Itens','Spearman de ranks','Mesma direção','Direção diferente','Posição diferente','Deslocamento P50/P90/máximo'],
        [[v['nivel_cid'],fmt(v['itens']),fmt(v['spearman'],4),fmt(v['mesma_direcao']),fmt(v['direcao_diferente']),fmt(v['posicao_diferente']),'/'.join(fmt(x,1) for x in v['deslocamento_p50_p90_max'])] for v in r['ranking']]))
    add('O ranking usa D100, do maior contraste no frio para o maior no quente. Empates recebem rank médio. Spearman é a correlação de Pearson desses ranks. Rank pode mudar mesmo com a direção preservada; zeros frequentes e empates também influenciam a correlação. Não foi criado corte arbitrário de deslocamento substancial: são apresentados a distribuição e os maiores deslocamentos observados.\n')
    add(tabela(['CID','Rank A','Rank B','Deslocamento','D100 A/B','Direção A/B'],
        [[v['rotulo_alvo'],fmt(v['rank_A'],1),fmt(v['rank_B'],1),fmt(v['deslocamento_rank'],1),fmt(v['mediana_diferenca_por_100_A'],4)+'/'+fmt(v['mediana_diferenca_por_100_B'],4),v['direcao_diferenca_A']+'/'+v['direcao_diferenca_B']] for v in r['maiores_mudancas_ranking']]))
    add('### Categorias principais selecionadas por volume\n')
    add(tabela(['Categoria','Internações','D100 A','D100 B','Direção A/B','Robustez'],
        [[v['rotulo_alvo'],fmt(v['internacoes_total_A']),fmt(v['mediana_diferenca_por_100_A'],4),fmt(v['mediana_diferenca_por_100_B'],4),v['direcao_diferenca_A']+'/'+v['direcao_diferenca_B'],v['robustez_descritiva']] for v in sorted(r['categorias_principais'],key=lambda x:(-x['internacoes_total_A'],x['chave_alvo']))]))
    add('## 10. Demografia e distribuição territorial\n\nForam preservadas as cinco faixas etárias e os códigos SIH 1/3. Idade, sexo e idade×sexo têm comparações próprias; marginais são calculadas por contagens municipais antes das médias, sem somar medianas. Os resultados completos incluem todos os diagnósticos e suas combinações da Etapa 1.\n')
    sexo={'1':'Masculino','3':'Feminino','TODOS':'Todos os sexos'}
    add(tabela(['Marginal','Idade','Sexo','D100 A','D100 B','Direção A/B','N absoluto A/B'],
        [[v['nivel_demografico'],v['faixa_etaria'],sexo.get(str(v['sexo']),str(v['sexo'])),fmt(v['mediana_diferenca_por_100_A'],4),fmt(v['mediana_diferenca_por_100_B'],4),v['direcao_diferenca_A']+'/'+v['direcao_diferenca_B'],fmt(v['municipios_validos_absoluto_A'])+'/'+fmt(v['municipios_validos_absoluto_B'])] for v in sorted(r['estratos_total'],key=lambda x:(x['nivel_demografico'],x['faixa_etaria'],x['sexo']))]))
    add(tabela(['Idade','Sexo','D100 A','D100 B','Direção A/B','N absoluto A/B','Robustez'],
        [[v['faixa_etaria'],sexo.get(str(v['sexo']),str(v['sexo'])),fmt(v['mediana_diferenca_por_100_A'],4),fmt(v['mediana_diferenca_por_100_B'],4),v['direcao_diferenca_A']+'/'+v['direcao_diferenca_B'],fmt(v['municipios_validos_absoluto_A'])+'/'+fmt(v['municipios_validos_absoluto_B']),v['robustez_descritiva']] for v in sorted(r['demografia_total'],key=lambda x:(x['faixa_etaria'],x['sexo']))]))
    add('Nas marginais de sexo, a direção permanece QUENTE em ambos. As quatro faixas até 79 anos também mantêm essa direção; 80+ muda de −0,0311 para +0,2379 por 100 município-dias. No cruzamento, mulheres 80+ passam de −0,0591 para zero; os outros nove cruzamentos conservam a direção. Esses contrastes próximos de zero exigem leitura da dispersão, sem descrever a mudança de sinal como diferença grande.\n')
    add('A comparação territorial usa quartis empíricos da média térmica municipal, incluindo empates, sem inventar grupos geográficos. Interior, litoral e regiões metropolitanas não receberam rótulos novos: os exemplos têm nomes/coordenadas e a análise prioriza o padrão térmico solicitado.\n')
    add(tabela(['Perfil térmico','Municípios','Mudança D100 B−A: Q1/mediana/Q3','Direção diferente'],
        [[v['perfil_termico'],fmt(v['municipios']),'/'.join(fmt(x,4) for x in v['delta_q1_mediana_q3']),fmt(v['direcao_diferente'])] for v in r['territorial']]))
    add(tabela(['Município','Média térmica','D100 A','D100 B','B−A','Direção A/B'],
        [[v['nome_municipio'],fmt(v['temperatura_media']),fmt(v['diferenca_por_100_municipio_dias_A'],4),fmt(v['diferenca_por_100_municipio_dias_B'],4),fmt(v['delta_B_A'],4),v['direcao_diferenca_A']+'/'+v['direcao_diferenca_B']] for v in r['maiores_mudancas_municipais']]))
    add('## 11. Diagnósticos raros, zeros e percentuais extremos\n')
    extremes={v['metodo']:v for v in r['extremos']}
    add(tabela(['Característica nos mesmos recortes','A','B','B−A'],[[label,fmt(extremes['A'][k]),fmt(extremes['B'][k]),fmt(extremes['B'][k]-extremes['A'][k])] for label,k in [
        ('Recortes auditados','recortes'),('Mediana da média fria zero','mediana_frio_zero'),('Mediana da média quente zero','mediana_quente_zero'),
        ('Mediana relativa −100%','mediana_relativa_menos100'),('Mediana relativa +100%','mediana_relativa_mais100'),('Algum município com denominador zero','algum_denominador_zero'),
        ('Nenhum município relativo válido','sem_municipio_relativo_valido'),('N relativo 1–3, descrição do caso pedido','N_relativo_1_a_3_descritivo')]]))
    add('As contagens de mediana zero não significam que todos os municípios têm média zero. Os detalhes e os contadores municipais mantêm essa distinção. A faixa N=1–3 apenas reproduz a questão levantada pelo exemplo de três municípios; não é regra de elegibilidade, exclusão ou significância. Histogramas completos de N relativo e total de eventos foram publicados, sem limiares de raridade. O volume total por recorte não muda ao reclassificar dias; a disponibilidade do denominador e a amostra percentual podem mudar. P20/P80 não cria eventos e não corrige automaticamente percentuais instáveis.\n')
    add('### Reavaliação do exemplo A43 — mulheres 60–79 anos\n')
    add(tabela(['Método','Eventos no período','N absoluto','N relativo','Mediana D100 absoluta','Mediana R (%)','Municípios frio zero','Municípios quente zero','Ambas zero'],
        [[v['metodo'],fmt(v['internacoes_total']),fmt(v['municipios_validos_absoluto']),fmt(v['municipios_validos']),fmt(v['mediana_diferenca_por_100'],6),fmt(100*v['mediana_diferenca_relativa'],2) if v['mediana_diferenca_relativa'] is not None else 'NULL',fmt(v['municipios_frio_zero']),fmt(v['municipios_quente_zero']),fmt(v['municipios_ambas_medias_zero'])] for v in r['caso_A43']]))
    add('Os 38 recortes 3/12 da Etapa 1 foram reavaliados nos mesmos CIDs/idades/sexos. Suas saídas e detalhes municipais estão em `audit_rare_results_*` e `audit_rare_municipal_*`. O resultado relativo continua complementar, com NULL quando quente=0, flags e base explícita. Mediana e IQR zero sobre a grande população com zeros não são evidência de ausência de associação em um diagnóstico raro.\n')
    add('## 12. Sazonalidade\n\nEstações meteorológicas do hemisfério sul: verão DJF; outono MAM; inverno JJA; primavera SON. Não são datas astronômicas de equinócios. As proporções abaixo usam os dias válidos de cada condição; não são proporções de internações. Também foram publicadas distribuições por mês e ano e a proporção de cada condição dentro de cada mês.\n')
    season={(v['metodo'],v['classe'],v['estacao_ano']):v for v in r['sazonalidade_estacoes']}
    add(tabela(['Método','% frio no inverno','% frio no verão','% quente no verão','% quente no inverno'],
        [[LABELS[m],*[fmt(season.get((m,c,s),{}).get('percentual_exposicao_na_estacao',0)) for c,s in [('FRIO','INVERNO'),('FRIO','VERAO'),('QUENTE','VERAO'),('QUENTE','INVERNO')]]] for m in methods]))
    add(img('06_sazonalidade'))
    add('A distribuição observada permite quantificar a concentração de B em estações distintas. Não foi isolada a contribuição de vírus, feriados, dia da semana ou tendências. Estudos observacionais sobre temperatura e mortalidade usam ajustes temporais específicos; essa literatura contextualiza a limitação e não valida os limites 20/80 como ótimos para internações. [Estudo original: Temperature Variability and Mortality, seção de métodos](https://pmc.ncbi.nlm.nih.gov/articles/PMC5047764/).\n')
    add('## 13. Exploração município × mês\n\nC calcula P20/P80 por município e mês do ano sobre os onze anos, comparando janeiros com janeiros, julhos com julhos. Reutiliza as mesmas métricas e a mesma elegibilidade por dias. Foram calculados cobertura, geral, anual, todos os capítulos e os dois grupos respiratórios. Não foi reconstruído CID×demografia completo para C.\n')
    add('C redistribui as exposições entre meses e responde a outra pergunta: anomalias relativamente frias/quentes para a época do ano. Isso reduz a separação inverno/verão visível na tabela, mas não corresponde a um ajuste estatístico completo de sazonalidade. Tendência entre anos, epidemias, dia da semana, diferenças dentro do mês, cobertura e seleção de estação permanecem possíveis componentes do contraste. Uma mudança nos resultados de C não identifica qual componente explica a mudança.\n')
    add(tabela(['Diagnóstico','D100 B','D100 C','Direção B/C','Robustez descritiva B/C'],
        [[v['rotulo_alvo'],fmt(v['mediana_diferenca_por_100_B'],4),fmt(v['mediana_diferenca_por_100_C'],4),v['direcao_diferenca_B']+'/'+v['direcao_diferenca_C'],v['robustez_descritiva']] for v in r['cid_B_C'] if v['chave_alvo'] in KEYS]))
    add('## 14. Sensibilidade e robustez\n\nRegra descritiva declarada, sem margem percentual arbitrária: mesma direção e cada mediana dentro do IQR do outro método → ROBUSTO_DESCRITIVO; mesma direção sem essa compatibilidade central → PARCIALMENTE_ROBUSTO; direção diferente → SENSIVEL_AO_METODO. Ausência de base válida ou IQRs ambos inteiramente zero → INCONCLUSIVO para uma afirmação discriminante. O último caso evita chamar diagnósticos esparsos de robustos apenas porque suas medianas são zero.\n')
    add('Q1/Q3 são a distribuição municipal, não IC. O critério usa a dispersão observada solicitada pelo usuário; não é teste de equivalência, significância, ou garantia de magnitudes numericamente iguais. Sempre são apresentadas as magnitudes e diferenças reais. Em distribuições muito largas, compatibilidade entre IQRs é uma regra permissiva. Não foi imposto novo mínimo de eventos, população ou municípios.\n')
    add(tabela(['Achado prioritário','Avaliação documentada'],[
        ['Internações totais A/B','Robusto descritivo: mesma direção; −4,4000/−3,8619; compatibilidade com IQRs recíprocos.'],
        ['Internações totais incluindo C','Parcialmente robusto na interpretação conjunta: mesma direção, porém −10,3190 em C, 2,35 vezes A; perguntas térmicas distintas. A regra automática de IQR é permissiva nesse caso.'],
        ['Capítulo X, J09–J18 e J20–J22 A/B','Robustos descritivos, conforme regra e magnitudes nas tabelas.'],
        ['Respiratórias B/C e circulatórias B/C','Sensíveis ao método: medianas mudam de sinal.'],
        ['Gravidez/parto e puerpério A/B','Robusto pela regra descritiva; ambas medianas negativas e IQRs abrangem zero.'],
        ['Sexo e idades até 79 anos A/B','Robustos pela regra; direção preservada, magnitudes próprias na seção 10.'],
        ['80+ e mulheres 80+ A/B','Sensíveis ao método, mas contrastes muito pequenos e dispersão atravessando zero.'],
        ['Diagnósticos com medianas/IQRs inteiramente zero','Inconclusivos para uma afirmação discriminante; base percentual pode ser pequena.']]))
    add(tabela(['Nível CID','Classificação A/B','Recortes'],[[v['nivel_cid'],v['robustez_descritiva'],fmt(v['count'])] for v in r['cid_robustez']]))
    generalcmp=csv('audit_general_results')
    add('Os resultados gerais, os diagnósticos respiratórios e os demais achados devem ser avaliados separadamente. As tabelas das seções 7–10 e 13 identificam quais direções persistem e quais magnitudes mudam. Nenhuma conclusão sobre aumento de internações deve ser sustentada só pela preservação de um percentual ou de um rank.\n')
    add('## 15. Vantagens e limitações\n')
    add(tabela(['Método','Pergunta descritiva','Vantagem','Limitação'],[
        ['A — absoluto','Como diferem as médias abaixo de 20 e a partir de 25 °C?','Escala física comum e continuidade histórica','Frações de exposição desiguais, pouca condição quente em cinco municípios e composição sazonal'],
        ['B — municipal','Como diferem as médias nos extremos relativamente frios e quentes locais?','Equilibra frações de exposição e recupera cinco municípios','Temperaturas físicas diferentes; concentração sazonal; percentuais raros continuam frágeis'],
        ['C — município×mês','Como diferem as médias em dias excepcionais para o clima local e para o mês?','Reduz separação de exposições entre estações','Outro contraste; não elimina confundimento temporal e não foi auditado em toda CID×demografia']]))
    add('Os métodos não fornecem classificação clínica universal de frio/quente nem limiar de conforto ou de internação. P20/P80 é uma escolha descritiva solicitada, não estimada por uma curva ótima. A série histórica tem dados faltantes e estações compartilhadas; não representa temperatura individual. Nenhuma normalização populacional, pseudocount, regressão ou linguagem de atribuição causal foi incorporada.\n')
    add('## 16. Recomendação metodológica\n\n**Recomendação: cenário D — abordagens complementares, com P20/P80 municipal como referência quando a pergunta do TCC for explicitamente o contraste entre extremos térmicos locais.** O método B é mais alinhado a essa pergunta por oferecer frações comparáveis dentro de cada município. Os limites absolutos devem permanecer como análise de sensibilidade e como resultado principal quando a pergunta for literalmente <20/≥25 °C. A cobertura já elevada de A impede justificar a substituição apenas pelos cinco municípios recuperados.\n')
    add('A escolha de B não deve ser vendida como solução para diagnósticos raros ou para confundimento sazonal. Seus números de extremos e zeros estão publicados, incluindo resultados desfavoráveis. C deve acompanhar a interpretação como sensibilidade sazonal, porque sua pergunta e seus contrastes são diferentes; não há justificativa nesta auditoria descritiva para promovê-lo automaticamente como método único ou dizer que controla todos os fatores temporais.\n')
    add('A evidência decisiva para manter abordagens complementares é a mudança de sinal nos três recortes respiratórios em C, junto da concentração de 65,07% dos dias frios de B no inverno. A semelhança A/B sustenta a comparação de extremos locais, mas não uma afirmação geral de que dias frios em qualquer época tenham médias respiratórias maiores. A melhora de exposição não elimina essa limitação interpretativa.\n')
    add('Antes de fechar a redação do TCC, definir qual das três perguntas da seção 15 será a pergunta principal e explicitar a sensibilidade dos achados nas tabelas. A recomendação é local e fundamentada nas coberturas, distribuições e contrastes desta auditoria; a decisão final continua com o autor do TCC. Nenhuma produção foi substituída.\n')
    add('## 17. Alterações que seriam necessárias no dashboard — não executadas\n\nUma adoção posterior de B exigiria thresholds municipais versionados por período e definição explícita do baseline. Filtros de Ano não devem recalibrar automaticamente os percentis; recálculos por seleção responderiam a outra pergunta. Seria preciso incluir o método térmico, P20/P80 em °C, dias/exposição, N absoluto e N relativo, flags de zero, magnitude D100, quartis e consistência; títulos teriam de dizer “dias relativamente frios/quentes”. C exigiria ainda o mês do baseline. As tabelas experimentais não foram conectadas ao Power BI; DAX, páginas, visuais, PBIP/TMDL, bookmarks e filtros não foram alterados.\n')
    add('## Respostas objetivas às 14 perguntas\n')
    add(tabela(['Pergunta','Resposta baseada nas saídas'],[
        ['1. Poucos ou nenhum dia frio absoluto?',f"Zero sem dias frios; {cob['A']['frio_menos30']} abaixo do critério de 30 dias. Distribuição completa na seção 2."],
        ['2. Poucos ou nenhum dia quente absoluto?',f"Zero sem dias quentes; {cob['A']['quente_menos30']} abaixo de 30 dias; nomes e motivos na seção 4."],
        ['3. Variação de P20/P80?',f"P20: {fmt(thdist['p20']['minimo'])}–{fmt(thdist['p20']['maximo'])} °C; P80: {fmt(thdist['p80']['minimo'])}–{fmt(thdist['p80']['maximo'])} °C."],
        ['4. Mais municípios comparáveis?',f"{cob['A']['elegiveis']} → {cob['B']['elegiveis']}; recuperados {len(r['municipios_recuperados'])}, perdidos {len(r['municipios_perdidos'])}."],
        ['5. Geral muda?',f"D100 A/B/C: {fmt(gerais['A']['mediana_diferenca_por_100'],4)}/{fmt(gerais['B']['mediana_diferenca_por_100'],4)}/{fmt(gerais['C']['mediana_diferenca_por_100'],4)}; bases e dispersão na seção 7."],
        ['6. Anual muda?','Sim, as magnitudes e as bases variam. Anos com direção A/B diferente: '+(', '.join(mud) if mud else 'nenhum')+'. Tabela completa na seção 8.'],
        ['7. Respiratórias mudam?','A/B mantêm medianas positivas nos três recortes. C muda para negativas: Cap. X −0,9330; J09–J18 −0,6305; J20–J22 −0,000929 por 100 município-dias. Sensível à definição dentro do mês.'],
        ['8. Rankings CID mudam?','Sim; Spearman, direções, deslocamentos e exemplos nas tabelas da seção 9.'],
        ['9. Idade e sexo mudam?','Sexo e faixas até 79 mantêm direção. 80+ passa de −0,0311 para +0,2379; mulheres 80+ de −0,0591 para zero. Magnitudes muito próximas de zero.'],
        ['10. ±100% ficam mais/menos frequentes?',f"−100%: {extremes['A']['mediana_relativa_menos100']} → {extremes['B']['mediana_relativa_menos100']}; +100%: {extremes['A']['mediana_relativa_mais100']} → {extremes['B']['mediana_relativa_mais100']}."],
        ['11. B fica confundido com sazonalidade?','65,07% dos dias frios estão no inverno; 41,93% dos quentes, no verão. Há forte concentração, sem identificar ou ajustar todo confundimento.'],
        ['12. C resolve parte do problema?','Redistribui exposições por meses e reduz a separação inverno/verão; não é controle estatístico completo.'],
        ['13. Método mais defensável?','Para extremos térmicos locais, B. Para temperaturas físicas 20/25, A. Recomenda-se a análise complementar e a sensibilidade C.'],
        ['14. Manter A como sensibilidade?','Sim. Preserva a pergunta física e a continuidade; a metodologia oficial segue A até decisão do autor.']]))
    add('## Testes, Data Quality, preservação e performance\n\n44 testes executados: 44 aprovados, 0 falhas, 0 erros. Foram preservados os 24 anteriores e acrescentados 20: período técnico excluído, percentis exatos/ordenados, limites válidos, ausência/NaN/infinito/flag meteorológico, inclusividade, empates, degeneração, partição dos dias, reprodutibilidade com repartição, mês próprio, CID esparsa, peso igual, Spearman com empates, direção sensível, bloqueio de escrita fora da auditoria, recuperação sintética, baseline anual fixo, rejeição de duplicatas, exemplos reproduzíveis e rótulos demográficos oficiais.\n')
    add('DQ final PASS para fontes, thresholds, classificação diária, partição dos dias, chaves, fórmulas, NULL/NaN/infinito, zeros, direção e resumos. A comparação do método A com a produção passou no geral municipal e nos resumos CID, demografia e marginais. As duas falhas de integração encontradas durante desenvolvimento eram de colunas/rótulos nas novas saídas, foram corrigidas e cobertas por testes; nenhum número de produção foi reescrito.\n')
    add(tabela(['Etapa final medida','Segundos'],[[k,fmt(v,3)] for k,v in r['tempos'].items()]+[['Total da execução final',fmt(r['tempo_total_segundos'],3)]]))
    add('Os tempos acima são da execução final bem-sucedida, sem somar tentativas anteriores de desenvolvimento, suíte de testes ou renderização. Spark usa funções nativas; não há UDF nem loops Python nas fatos. Numeradores A/B e grãos demográficos são compartilhados, os resumos compactos são persistidos antes das verificações e Python recebe somente saídas pequenas selecionadas. O método C tem escopo limitado e não calcula o cruzamento demográfico inteiro.\n')
    add(f"Preservação da produção: {fmt(r['producao_sha256_preservada']['arquivos'])} arquivos da área oficial de analytics tiveram hashes SHA-256 idênticos antes/depois. A verificação externa adicional de Power BI e código oficial está em `verificacao_etapa1b.json` e `protected_hashes.json`.\n")
    add('## Arquivos e reprodução\n\nArquivos novos de implementação:\n\n- `src/validation/auditoria_definicao_temperatura.py` — runner Spark, testes de DQ e saídas experimentais.\n- `src/validation/relatorio_definicao_temperatura.py` — relatório e gráficos a partir das saídas compactas.\n- `tests/test_temperature_definition_audit.py` — 20 testes novos.\n- `docs/Relatorio_Auditoria_Definicao_Temperatura.md` — este relatório.\n\nDados: `data/validation/auditoria_definicao_temperatura`, com Parquets e CSVs de thresholds, exposições, elegibilidade, exemplos, classificação diária, concordância, geral, anual, base comum, CID, rankings, demografia, marginais, raros, extremos, sazonalidade e a exploração mensal. `resultados_auditoria.json` reúne os resultados numéricos compactos. Logs de execução e testes, hashes e `manifesto_arquivos.json` registram os arquivos físicos.\n\nGráficos: sete PNGs e sete SVGs em `figuras/`, todos estáticos e fora do Power BI. Para repetir: executar o runner com `spark-submit` no Spark existente; depois executar o gerador de relatório no Python do mesmo ambiente, que já contém NumPy, Pandas e Matplotlib. Nenhum download, rebuild de imagens, Bronze/Silver/Gold ou publicação foi necessário.\n')
    doc='\n\n'.join(secs)
    DOC.write_text(doc,encoding='utf-8')
    print('RELATORIO_GRAVADO',str(DOC),flush=True)


if __name__=='__main__':
    graficos()
    relatorio()
