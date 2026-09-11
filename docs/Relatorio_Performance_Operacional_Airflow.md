---
title: "Análise de performance operacional"
subtitle: "DAG tcc_pipeline — execução completa de 41m46s"
date: "10 de setembro de 2026"
lang: pt-BR
geometry: margin=2cm
fontsize: 10pt
toc-title: "Sumário"
colorlinks: true
header-includes:
  - \usepackage{pdflscape}
  - \usepackage{longtable}
  - \usepackage{booktabs}
  - \usepackage{array}
  - \usepackage{fvextra}
  - \DefineVerbatimEnvironment{Highlighting}{Verbatim}{breaklines,commandchars=\\\{\}}
  - \setlength{\emergencystretch}{3em}
---

# Resultado e escopo

A execução analisada durou **41m46,422s**. Três tasks sequenciais concentraram **90,84% desse tempo**: silver_sih, gold_fato_internacao e gold_fato_internacao_meteorologia.

O primeiro experimento recomendado é consolidar as contagens de nulos da fato de internação: esse bloco consumiu **4m33s**, segundo os logs. A maior task, Silver SIH, gastou aproximadamente **13m57s na conversão DBF para Parquet**, antes do processamento Spark.

A análise foi exclusivamente observacional. Não foram alterados código, DAG, configurações Spark/Airflow, pools, concorrência, permissões ou datasets; não houve commit, push ou reexecução do pipeline. Este Markdown e o PDF foram criados posteriormente, mediante solicitação de documentação. Nenhuma otimização proposta foi implementada.

# Identificação da execução e fontes

| Campo | Valor |
|:--|:--|
| DAG | tcc_pipeline |
| Run ID | manual__2026-09-10T01:37:19.578492+00:00 |
| Estado | success |
| Início UTC | 10/09/2026 01:37:20,237862 |
| Fim UTC | 10/09/2026 02:19:06,659535 |
| Horário de São Paulo | 09/09/2026, 22:37:20 a 23:19:06 |
| Duração precisa | 2.506,421673 segundos |
| Tasks / TaskGroups | 24 / 5 |
| Tentativas | Todas com try_number = 1 |
| Versão da DAG | 01a088dc-8205-7d14-bb61-cb94cc782a7b |

Foram consultados os registros de **dag_run**, **task_instance** e o código histórico em **dag_code**, no PostgreSQL de metadados do Airflow. Os horários internos dos scripts vieram dos logs locais da execução. As dependências foram confirmadas na versão histórica da DAG, não apenas no arquivo atual do workspace.

As durações das tasks incluem inicialização, execução e encerramento dos processos. Uma task SparkSubmitOperator pode conter conversões Python e I/O fora do Spark. Duração de task não equivale a tempo de CPU nem a duração exclusiva de estágios Spark.

A run selecionada era a mais recente em sucesso e correspondia aos aproximadamente 41m46s informados. Foram confirmadas 24 tasks em sucesso, todas na primeira tentativa.

\begin{landscape}
\section{Tabela completa das 24 tasks}
Horários em UTC, em 10/09/2026. Início e fim truncados a milissegundos; durações arredondadas a milissegundos. Todas as tasks tiveram uma tentativa. Bronze e DQ Bronze usam PythonOperator; Silver, Gold e seus DQs usam SparkSubmitOperator.
\par\medskip
\scriptsize
\setlength{\tabcolsep}{3pt}
\renewcommand{\arraystretch}{1.3}
\begin{longtable}{p{81mm}p{12mm}p{17mm}p{23mm}p{23mm}r p{25mm}p{13mm}}
\toprule
task\_id & Grupo & Tipo & start\_time & end\_time & Segundos & Duração & Estado\\
\midrule
\endfirsthead
\toprule
task\_id & Grupo & Tipo & start\_time & end\_time & Segundos & Duração & Estado\\
\midrule
\endhead
inmet.bronze\_inmet & inmet & bronze & 01:37:20.413 & 01:37:20.900 & 0,487 & 00:00:00,487 & success \\
inmet.dq\_bronze\_inmet & inmet & DQ bronze & 01:37:21.531 & 01:37:30.848 & 9,317 & 00:00:09,317 & success \\
inmet.silver\_inmet & inmet & silver & 01:37:31.593 & 01:39:29.760 & 118,167 & 00:01:58,167 & success \\
inmet.dq\_silver\_inmet & inmet & DQ silver & 01:39:30.386 & 01:40:26.911 & 56,525 & 00:00:56,525 & success \\
inmet.silver\_inmet\_diario & inmet & silver & 01:40:27.511 & 01:49:02.502 & 514,990 & 00:08:34,990 & success \\
inmet.dq\_silver\_inmet\_diario & inmet & DQ silver & 01:49:03.179 & 01:50:32.379 & 89,201 & 00:01:29,201 & success \\
ibge.bronze\_ibge & ibge & bronze & 01:37:20.409 & 01:37:20.889 & 0,480 & 00:00:00,480 & success \\
ibge.dq\_bronze\_ibge & ibge & DQ bronze & 01:37:21.531 & 01:37:22.060 & 0,529 & 00:00:00,529 & success \\
ibge.silver\_ibge & ibge & silver & 01:37:22.695 & 01:37:30.925 & 8,230 & 00:00:08,230 & success \\
ibge.dq\_silver\_ibge & ibge & DQ silver & 01:37:31.587 & 01:37:41.743 & 10,157 & 00:00:10,157 & success \\
cid10.bronze\_cid10 & cid10 & bronze & 01:37:20.410 & 01:37:20.890 & 0,480 & 00:00:00,480 & success \\
cid10.dq\_bronze\_cid10 & cid10 & DQ bronze & 01:37:21.531 & 01:37:21.974 & 0,443 & 00:00:00,443 & success \\
cid10.silver\_cid10 & cid10 & silver & 01:37:22.695 & 01:37:32.152 & 9,457 & 00:00:09,457 & success \\
cid10.dq\_silver\_cid10 & cid10 & DQ silver & 01:37:32.753 & 01:37:42.318 & 9,565 & 00:00:09,565 & success \\
sih.bronze\_sih & sih & bronze & 01:37:20.396 & 01:37:21.057 & 0,661 & 00:00:00,661 & success \\
sih.dq\_bronze\_sih & sih & DQ bronze & 01:37:21.531 & 01:38:26.977 & 65,446 & 00:01:05,446 & success \\
sih.silver\_sih & sih & silver & 01:38:27.750 & 01:57:21.261 & 1.133,510 & 00:18:53,510 & success \\
sih.dq\_silver\_sih & sih & DQ silver & 01:57:22.139 & 01:58:09.405 & 47,266 & 00:00:47,266 & success \\
gold.gold\_municipio\_estacao & gold & gold & 01:40:27.514 & 01:40:53.978 & 26,465 & 00:00:26,465 & success \\
gold.dq\_gold\_municipio\_estacao & gold & DQ gold & 01:40:54.485 & 01:41:03.051 & 8,567 & 00:00:08,567 & success \\
gold.gold\_fato\_internacao & gold & gold & 01:58:09.615 & 02:08:27.561 & 617,946 & 00:10:17,946 & success \\
gold.dq\_gold\_fato\_internacao & gold & DQ gold & 02:08:28.545 & 02:08:48.087 & 19,542 & 00:00:19,542 & success \\
gold.gold\_fato\_internacao\_meteorologia & gold & gold & 02:08:48.209 & 02:17:33.699 & 525,490 & 00:08:45,490 & success \\
gold.dq\_gold\_fato\_internacao\_meteorologia & gold & DQ gold & 02:17:34.733 & 02:19:06.311 & 91,577 & 00:01:31,577 & success \\
\bottomrule
\end{longtable}
\normalsize
\end{landscape}

# Ranking das 10 tasks mais demoradas

A porcentagem compara a duração individual com o wall-clock da DAG. Como existem sobreposições, essas porcentagens não devem ser somadas indiscriminadamente. Os nomes abaixo são os IDs dentro dos respectivos grupos.

| Posição | Grupo / task | Duração | % da DAG | Crítica |
|--:|:--|--:|--:|:--|
| 1 | sih / silver_sih | 18m53,510s | 45,22% | Sim |
| 2 | gold / gold_fato_internacao | 10m17,946s | 24,65% | Sim |
| 3 | gold / gold_fato_internacao_meteorologia | 8m45,490s | 20,97% | Sim |
| 4 | inmet / silver_inmet_diario | 8m34,990s | 20,55% | Não |
| 5 | inmet / silver_inmet | 1m58,167s | 4,71% | Não |
| 6 | gold / dq_gold_fato_internacao_meteorologia | 1m31,577s | 3,65% | Sim |
| 7 | inmet / dq_silver_inmet_diario | 1m29,201s | 3,56% | Não |
| 8 | sih / dq_bronze_sih | 1m05,446s | 2,61% | Sim |
| 9 | inmet / dq_silver_inmet | 56,525s | 2,26% | Não |
| 10 | sih / dq_silver_sih | 47,266s | 1,89% | Sim |

O INMET diário é caro, mas terminou com bastante antecedência em relação ao momento em que sua saída foi necessária. Reduzir sua duração isoladamente pode não reduzir o tempo total.

# Durações por camada

| Tipo | Quantidade | Soma das durações |
|:--|--:|--:|
| Bronze | 4 | 00:00:02,108 |
| DQ Bronze | 4 | 00:01:15,734 |
| Silver | 5 | 00:29:44,355 |
| DQ Silver | 5 | 00:03:32,714 |
| Gold | 3 | 00:19:29,901 |
| DQ Gold | 3 | 00:01:59,686 |
| **Total** | **24** | **00:56:04,498** |

A soma representa tempo acumulado de tasks, não CPU consumida nem duração da DAG. O paralelismo explica por que ela supera os 41m46s.

As Silver de SIH e INMET diário dominam o tempo acumulado Silver. Nas Gold, as duas fatos dominam; município-estação consumiu apenas 26,465s. Entre os DQs Spark, os mais caros são DQ Gold meteorologia (91,577s) e DQ Silver INMET diário (89,201s). Apenas o primeiro pertence ao caminho crítico desta execução.

# Bronze e DQ Bronze: custo e impacto

| Fonte | Aquisição Bronze | DQ Bronze |
|:--|--:|--:|
| SIH | 0,661s | 65,446s |
| INMET | 0,487s | 9,317s |
| IBGE | 0,480s | 0,529s |
| CID10 | 0,480s | 0,443s |
| **Soma** | **2,108s** | **75,734s** |

As quatro aquisições se sobrepuseram. A janela entre a primeira começar e a última terminar foi de aproximadamente **0,661s**. Os logs registram arquivos já existentes para SIH, INMET e IBGE; CID10 retornou os quatro CSVs locais. Esses tempos **não representam uma aquisição completa pela rede**.

Os quatro DQs Bronze começaram praticamente juntos. Sua janela conjunta foi de **65,446s**, determinada pelo SIH.

- O DQ Bronze SIH ocupou **65,446s do caminho crítico**.
- Os demais DQs Bronze foram absorvidos pelos ramos paralelos nesta execução.
- Bronze SIH e DQ Bronze SIH somaram **66,107s de execução** no caminho crítico.
- Silver SIH iniciou **67,513s após o início da DAG**, incluindo transições.

Em um cenário hipotético que mantivesse as demais durações e esperas, retirar apenas os DQs Bronze reduziria o caminho dominante em aproximadamente **65,45s**, não em 75,73s. Isso é uma projeção pelas dependências, **não uma medição causal do acréscimo real**. O ganho observado entre as duas runs não mede isoladamente o custo da integração Bronze.

# Paralelismo observado

Horários UTC, em 10/09/2026, aproximados até segundos.

| Intervalo | Execuções simultâneas |
|:--|:--|
| 01:37:20 a 01:37:21 | Quatro aquisições Bronze |
| A partir de 01:37:21 | Quatro DQs Bronze, até os mais curtos terminarem |
| 01:37:22 a 01:37:30 | Silver IBGE + Silver CID10 + DQs Bronze ainda ativos |
| 01:37:32 a 01:37:41 | Silver INMET + DQ Silver IBGE + DQ Silver CID10 + DQ Bronze SIH |
| 01:38:27 a 01:39:29 | Silver SIH + Silver INMET |
| 01:39:30 a 01:40:26 | Silver SIH + DQ Silver INMET |
| 01:40:27 a 01:40:53 | Silver SIH + Silver INMET diário + Gold município-estação |
| 01:40:54 a 01:41:03 | Silver SIH + Silver INMET diário + DQ Gold município-estação |
| 01:41:03 a 01:49:02 | Silver SIH + Silver INMET diário |
| 01:49:03 a 01:50:32 | Silver SIH + DQ Silver INMET diário |
| 01:50:32 a 01:57:21 | Apenas Silver SIH |
| 01:57:22 a 02:19:06 | DQ SIH e cadeia das fatos Gold, sequencialmente |

**Os quatro ramos aproveitaram paralelismo.** A principal sobreposição entre tasks pesadas foi SIH + INMET diário, durante os **514,990s completos do INMET diário**.

# Caminho crítico real

A sequência dominante foi reconstruída a partir das dependências históricas e dos horários efetivos. Em cada convergência, foi identificado o predecessor que terminou por último.

| Ordem | Task no caminho crítico | Segundos |
|--:|:--|--:|
| 1 | sih.bronze_sih | 0,661 |
| 2 | sih.dq_bronze_sih | 65,446 |
| 3 | sih.silver_sih | 1.133,510 |
| 4 | sih.dq_silver_sih | 47,266 |
| 5 | gold.gold_fato_internacao | 617,946 |
| 6 | gold.dq_gold_fato_internacao | 19,542 |
| 7 | gold.gold_fato_internacao_meteorologia | 525,490 |
| 8 | gold.dq_gold_fato_internacao_meteorologia | 91,577 |

A ordem acima representa a cadeia de dependências: **1 → 2 → 3 → 4 → 5 → 6 → 7 → 8**.

| Componente | Tempo |
|:--|--:|
| Execução das tasks desse caminho | **41m41,439s** |
| Transições, início e encerramento da DAG | **4,983s** |
| **Wall-clock observado** | **41m46,422s** |

Nos pontos de convergência:

1. Silver SIH aguardava DQ Bronze SIH e IBGE; **SIH terminou por último**.
2. Gold fato internação aguardava DQ Silver SIH, IBGE e CID10 e DQ Gold município-estação; **DQ Silver SIH terminou por último**.
3. Gold fato meteorologia aguardava DQ da fato e DQ INMET diário; **DQ da fato terminou por último**.

A soma das três maiores transformações críticas é 2.276,946808s, equivalente a **90,84% do wall-clock**. O tempo crítico total não foi obtido somando indiscriminadamente as 24 tasks.

# Esperas por dependência e fila

| Situação | Tempo observado | Interpretação |
|:--|--:|:--|
| DQ município-estação pronto até DQ SIH terminar | 17m06,354s | A fato ainda precisava do SIH |
| DQ INMET diário pronto até DQ da fato terminar | 18m15,708s | A integração meteorológica ainda precisava da fato |
| Última dependência concluída até início da task | Aproximadamente 0,122 a 1,034s | Transições curtas |
| queued_dttm até início da task | Aproximadamente 0,021 a 0,140s | Sem fila prolongada observada |

Os dois primeiros intervalos não são ociosidade global: outra task necessária estava trabalhando. Eles medem a antecedência de uma entrada em relação à última dependência, não uma espera adicional a ser somada ao caminho crítico.

Não há evidência de que scheduler, pools ou fila sejam o gargalo principal. Não foram alteradas dependências, concorrência ou configurações.

# Possíveis contenções de recursos

Todas as task instances registraram o mesmo worker, **854a3d19578b**. Os comandos nos logs usam **spark-submit --master local[2]**. Houve aplicações locais concorrentes no mesmo ambiente.

- **CPU:** possibilidade de competição entre aplicações Spark e a conversão Python do SIH.
- **Memória:** JVMs e processos Python simultâneos.
- **Disco:** conversões temporárias, leituras, gravações e possíveis shuffles compartilhando recursos.

Isso identifica possibilidade de contenção, mas não demonstra saturação. Não foram obtidas séries históricas de CPU, memória, I/O, GC ou spill que permitam quantificar esse efeito. Sobreposição de tasks também não significa que todas estavam executando estágios Spark ao mesmo tempo.

As duas maiores fatos Gold executaram depois de terminarem os demais ramos desta DAG. Assim, sua sobreposição com outras tasks desta mesma run não explica seus tempos; eventual carga externa não foi medida.

# Inspeção estática: escopo e evidências

A inspeção se concentrou nas cinco transformações mais demoradas e no DQ Gold mais demorado, incluindo o módulo chamado pelo lote SIH. Os scripts menores não foram objeto de uma auditoria ampla.

## SIH: a maior parcela está antes do Spark

Os logs permitem decompor as 36 competências processadas:

| Trecho medido pelos marcadores | Tempo acumulado |
|:--|--:|
| Conversão DBC → DBF | 34,190s |
| Conversão DBF → Parquet | **837,453s — 13m57,453s** |
| Após conversão Parquet até Silver salva | 257,463s — 4m17,463s |
| Restante da task | Aproximadamente 4,404s |

A conversão DBF → Parquet respondeu por **73,88% da task SIH**. Ela usa leitura incremental de DBF, construção de tabelas Arrow e escrita Parquet em Python, conforme **src/silver/sih.py, linha 107**.

Os intervalos derivam de mensagens de log e incluem pequenos custos entre marcadores. O trecho posterior ao Parquet também contém trabalho Python e gravação, portanto não deve ser interpretado como CPU Spark pura.

O lote em **src/silver/processar_sih_lote.py, linha 82** chama o processamento das 36 competências sequencialmente. Em **src/silver/sih.py**, há leitura da mesma dimensão IBGE por competência (linha 507), contagem após join (530), contagens com e sem município (547 e 555), contagem Silver (577) e gravação (615). Não foi encontrada persistência explícita desse fluxo.

## Fato de internação: contagens de nulos custosas

| Marco do log | Horário UTC |
|:--|:--|
| “Validando campos importantes...” | 02:00:47,424 |
| Última coluna validada | 02:05:20,425 |
| **Tempo do bloco** | **273,001s — 4m33,001s** |

Esse bloco representa **44,18% da task** e **10,89% da DAG**. O código executa uma ação filter(...).count() para cada uma das 12 colunas sobre df_gold, em **src/gold/fato_internacao.py, linhas 1708 a 1741**.

O restante do script também utiliza contagens, distinct, dropDuplicates, anti-joins, agregações de resumo e escrita sobre linhagens relacionadas, sem persistência explícita. Isso aponta possibilidade de recomputação, não prova que cada ação execute exatamente o mesmo plano físico.

A gravação, entre “Gravando Gold...” e o resumo seguinte, ocupou aproximadamente **69,665s** (02:06:24,165 a 02:07:33,830). O intervalo inclui a execução necessária para produzir os dados e gravá-los; não isola o disco.

## Gold meteorologia e INMET diário

Em **src/gold/fato_internacao_meteorologia.py**, df_inmet participa de contagem (495), estações distintas (504–505), verificação de chaves inválidas (577), duplicidades por groupBy (592–603), período (638), correspondência de estações e join. Não foi encontrada persistência explícita no script.

O intervalo do log dedicado à validação da chave INMET foi de aproximadamente **79,828s**, de 02:10:35,338 a 02:11:55,166. Ele inclui a checagem de chaves inválidas e de duplicidades; não é uma medição isolada de shuffle.

O script já utiliza broadcast explícito no join meteorológico. Há agregações coletadas como uma única linha, repartition antes da escrita e releitura para validar a gravação. Essas operações não são, por si só, evidência de desperdício.

Em **src/silver/inmet_diario.py**, df_diario.count() aparece nas linhas 888 e 925 sem transformação intermediária do DataFrame. Há ainda distinct, groupBy, validações em loops, escrita e releitura. A task é cara, mas não crítica nesta run.

## DQ Gold meteorologia e INMET horário

Em **src/quality/gold/dq_fato_internacao_meteorologia.py**, há várias verificações sobre a fato: contagens, anti-joins de estações, filtros por período e consistência, e agregações de cobertura. O script cacheia apenas df_inmet (linha 103); a fato é lida sem cache explícito nesse ponto. A sessão é encerrada no finally.

Em **src/silver/inmet.py**, o fluxo por ano já usa cache antes de contagem e escrita (linha 589), seguido de unpersist (634). Esse ponto não é uma prioridade de intervenção.

# Classificação dos achados e recomputações

| Achado | Potencial | Justificativa |
|:--|:--|:--|
| Fato: 12 contagens separadas de nulos/vazios | **ALTO** | Bloco medido em 273s no caminho crítico; repete avaliações de uma linhagem com joins |
| Fato: múltiplas ações, resumos e escrita sem persistência explícita | **ALTO** | Possível repetição de processamento; custo por recomputação exige métricas de execução |
| SIH: conversão DBF → Parquet em 36 competências | **ALTO** | Maior parcela medida; solução precisa preservar a conversão e sua correção, além de tuning Spark |
| SIH: contagens de total, correspondência e Silver separadas | **MÉDIO** | Repetição por 36 meses, mas o trecho Spark não domina a task |
| SIH: leitura e contagem repetidas da dimensão IBGE | **MÉDIO** | Trabalho repetido confirmado; dimensão pequena e ganho provavelmente limitado |
| Gold meteorologia: várias ações sobre df_inmet sem persistência explícita | **MÉDIO** | Contagem, chave, período e correspondência reutilizam a mesma fonte |
| INMET diário: count repetido e validações em loops | **ALTO na task** | Repetição explícita; benefício direto no wall-clock limitado pela folga do ramo |
| DQ Gold meteorologia: várias verificações sobre a fato sem cache | **MÉDIO** | Possibilidade de leituras repetidas durante 91,577s críticos |
| INMET horário: cache/count/write/unpersist existente | **BAIXO nesse ponto** | Já há reaproveitamento explícito |
| collect de agregações escalares | **BAIXO como transferência** | Retorno pequeno; custo relevante pode estar no cálculo anterior |

Distinct, dropDuplicates, groupBy, joins e repartition não são, isoladamente, prova de desperdício. A suspeita surge quando ações subsequentes voltam a executar a mesma linhagem. As verificações de qualidade também não devem ser simplesmente removidas.

As classificações indicam prioridade de investigação, não economia garantida. Um achado de alto potencial dentro de uma task fora do caminho crítico pode ter pouco efeito direto na duração total.

# Três oportunidades recomendadas

## 1. Consolidar contagens de nulos da fato de internação

Substituir, em um experimento futuro, as 12 ações separadas por uma agregação conjunta, mantendo as condições de nulos/vazios, resultados por coluna e critérios de reprovação.

O alvo medido é de **273,001s**, diretamente no caminho crítico. A economia será menor que o tempo integral do bloco e precisa ser medida. É uma mudança localizada, fácil de explicar academicamente: obter as mesmas métricas com menos ações sobre os dados.

## 2. Avaliar persistência da dimensão INMET preparada na Gold meteorológica

Avaliar reaproveitamento apenas da dimensão INMET preparada entre suas validações e o join, liberando-a após o uso. A dimensão pequena oferece menor exposição de memória que persistir a fato completa.

Pode reduzir leituras e cálculos repetidos. O intervalo de 79,828s na validação da chave justifica medir o trecho, mas **não demonstra que todo esse tempo seja eliminável com persistência**. O ganho ainda não foi medido.

## 3. Consolidar métricas de correspondência do SIH por competência

Calcular as métricas de correspondência em uma agregação por competência, preservando as condições e verificações existentes, sem alterar joins ou filtros territoriais.

Pode reduzir ações repetidas ao longo de 36 meses. O benefício é potencialmente modesto, pois a conversão DBF → Parquet domina o SIH.

Não se prioriza o INMET diário como primeiro experimento para reduzir os 42 minutos: sua saída ficou pronta mais de 18 minutos antes da outra dependência da integração final. Não são propostas combinações de shuffle, AQE, broadcast, pools ou concorrência.

# Comparação com a execução anterior

A execução anterior confirmada no banco foi:

**manual__2026-09-10T00:00:31.704106+00:00**

Ela terminou em sucesso, com 16 tasks e duração de **2.754,626248s**.

| Base | Antes | Atual | Redução | Percentual |
|:--|--:|--:|--:|--:|
| Tempos informados | 45m54s | 41m46s | **4m08s** | **9,01%** |
| Metadados precisos | 45m54,626s | 41m46,422s | **4m08,205s** | **9,01%** |

Fórmula: redução percentual = (tempo anterior − tempo atual) / tempo anterior × 100.

As três maiores transformações ficaram mais rápidas:

| Task | Anterior | Atual | Redução |
|:--|--:|--:|--:|
| Silver SIH | 1.195,142s | 1.133,510s | 61,632s |
| Gold fato internação | 689,747s | 617,946s | 71,801s |
| Gold fato meteorologia | 675,714s | 525,490s | 150,224s |

Essas três reduções somam aproximadamente **283,657s**, em tasks do caminho dominante. Isso ajuda a explicar contabilmente como a execução atual terminou mais cedo apesar das novas etapas.

**Não permite concluir que a integração Bronze causou a melhora.** Há somente uma observação de cada execução, sem controle experimental.

# Limitações da comparação

- Cache do sistema operacional, carga da máquina, concorrência e variação de I/O são hipóteses plausíveis, mas não foram medidos.
- Os logs mostram novos processos spark-submit. Não há evidência de reaproveitamento da mesma JVM entre DAG Runs. Aquecimento dentro de uma aplicação, como o lote SIH, é uma questão distinta.
- Não foi obtido perfil histórico de CPU, memória, GC, spill ou disco.
- Os tempos internos vêm de marcadores de logs, não de instrumentação de cada estágio.
- O código histórico da DAG foi recuperado; os scripts foram inspecionados na versão atual do workspace, corroborada pelas mensagens dos logs. Não foi comprovada a identidade histórica de cada script por hash.
- A Bronze reutilizou entradas existentes, limitando a generalização para uma primeira aquisição.
- Comparar apenas durações não garante igualdade de condições da máquina nem permite atribuir causalidade.
- Estimativas de impacto pela retirada de tasks mantêm hipoteticamente as demais durações; mudanças de sobreposição podem alterar o comportamento real.

# Recomendação do primeiro experimento

Começar **somente pela agregação conjunta dos nulos/vazios da fato de internação**.

Em uma tarefa futura:

1. Comparar original e variante sobre as mesmas entradas, com saídas isoladas.
2. Preservar as 12 condições, contagens e critérios de reprovação.
3. Medir o bloco de validação e a task completa em pelo menos três execuções de cada variante.
4. Alternar a ordem das variantes e registrar condições de carga comparáveis.
5. Comparar medianas e dispersão, não apenas o melhor resultado.
6. Confirmar equivalência dos resultados e aprovação do DQ.

A referência observada é **273,001s para o bloco** e **617,946s para a task**. Por estar no caminho crítico e não depender de um ramo paralelo nessa etapa, uma redução consistente nessa task tem possibilidade direta de reduzir o wall-clock da DAG.

O experimento não foi executado nesta análise.

# Rastreabilidade das evidências

As consultas de metadados foram somente de leitura. A seleção da run usou dag_id igual a tcc_pipeline e ordenação decrescente por start_date; estado, duração e as 24 task instances foram conferidos antes da análise.

Campos utilizados:

- **dag_run:** run_id, state, start_date, end_date e diferença entre fim e início.
- **task_instance:** task_id, state, start_date, end_date, duration, queued_dttm, try_number, hostname e dag_version_id.
- **dag_code:** source_code associado à versão 01a088dc-8205-7d14-bb61-cb94cc782a7b.
- **Logs:** diretório logs/dag_id=tcc_pipeline, run de 10/09/2026 01:37:19,578492 UTC, subdiretórios de cada task, tentativa 1. No Windows, caracteres de dois-pontos no nome do diretório podem aparecer com representação especial.

Cálculos:

- Duração da DAG: fim da run menos início da run.
- Soma por tipo: soma do campo duration, sem interpretar o resultado como wall-clock.
- Paralelismo: interseção dos intervalos start_date/end_date.
- Caminho crítico observado: cadeia de predecessores que terminaram por último nas convergências, conferida com as durações.
- Transição: início da task menos término da última dependência.
- Fila: start_date menos queued_dttm.
- Tempos internos: diferenças entre marcadores de log, acumuladas quando aplicável às 36 competências.

A documentação registra uma observação operacional, separa medições de hipóteses e mantém as recomendações como trabalho futuro.

