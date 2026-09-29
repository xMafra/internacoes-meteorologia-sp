---
title: "Resultados dos experimentos de performance"
subtitle: "Análise operacional da DAG tcc_pipeline após os Experimentos 1 e 2"
date: "11 de setembro de 2026"
lang: pt-BR
geometry: margin=2cm
fontsize: 10pt
toc-title: "Sumário"
header-includes:
  - \usepackage{pdflscape}
  - \usepackage{longtable}
  - \usepackage{booktabs}
  - \usepackage{array}
  - \usepackage{fvextra}
  - \DefineVerbatimEnvironment{Highlighting}{Verbatim}{breaklines,commandchars=\\\{\}}
  - \setlength{\emergencystretch}{3em}
---

# Resultado principal

A última execução da DAG **tcc_pipeline** terminou em **33m25,611s**, com as **24 tasks em sucesso e todas na primeira tentativa**.

Em relação à execução imediatamente anterior, de 46m46,285s, houve redução observada de **13m20,674s, ou 28,53%**. Em relação ao baseline anterior aos experimentos, de 41m46,422s, a redução foi de **8m20,810s, ou 19,98%**.

A Gold fato_internacao_meteorologia caiu de **751,996s para 217,760s**, redução de **71,04%**. Essa task responde contabilmente por **66,72% da redução total** entre as duas últimas runs, pois integra o caminho crítico.

Os logs confirmam **8.409.047 registros antes e depois do join e na gravação**. O DQ final aprovou as verificações de cardinalidade, schema, chaves, flags, correspondência e cobertura.

Os resultados são consistentes com benefício do reaproveitamento do INMET. Entretanto, esta comparação é observacional: a redução de todas as tasks não pode ser atribuída exclusivamente ao Experimento 2.

# Fontes e identificação das execuções

Foram consultadas as tabelas dag_run e task_instance do banco PostgreSQL de metadados do Airflow e os logs locais em logs/dag_id=tcc_pipeline. Também foram conferidos o código atual dos experimentos, as dependências da DAG e o diff local de configuração.

Esta tarefa apenas analisou os dados já disponíveis e produziu documentação. Não executou jobs, DQs ou a DAG novamente e não alterou código ou datasets.

As três referências principais são:

| Referência | Run ID |
|:--|:--|
| A — baseline antes dos experimentos | manual__2026-09-10T01:37:19.578492+00:00 |
| B — após Experimento 1 | manual__2026-09-10T23:06:31.740806+00:00 |
| C — após Experimentos 1 e 2 | manual__2026-09-11T02:23:24.553751+00:00 |

| Referência | Início UTC | Fim UTC | Duração |
|:--|:--|:--|--:|
| A | 10/09 01:37:20,237862 | 10/09 02:19:06,659535 | 41m46,422s |
| B | 10/09 23:06:32,794670 | 10/09 23:53:19,080142 | 46m46,285s |
| C | 11/09 02:23:24,765581 | 11/09 02:56:50,377059 | **33m25,611s** |

No horário de São Paulo, a execução C ocorreu em **10/09/2026, das 23:23:24 às 23:56:50**. A data UTC é 11/09.

As três runs possuem a mesma referência de versão da DAG nas task instances: **01a088dc-8205-7d14-bb61-cb94cc782a7b**. Isso identifica a definição da DAG; não comprova, por si só, identidade dos scripts externos ou de toda a infraestrutura entre runs.

# O que foi implementado

## Experimento 1 — contagens consolidadas na fato de internação

No bloco “Validando campos importantes...” de src/gold/fato_internacao.py, 12 actions independentes filter(...).count() foram substituídas por uma agregação com 12 expressões e uma action first().

A condição permaneceu NULL ou vazio após trim e cast para string. Foram preservadas as 12 colunas, sua ordem, mensagens e ValueError na primeira coluna inválida. Coalesce preserva zero para DataFrame vazio.

Foi adicionado um cronômetro somente para esse bloco. A implementação anterior foi validada com seis testes sintéticos e preservada no Experimento 2.

## Experimento 2 — persistência somente do INMET preparado

Em src/gold/fato_internacao_meteorologia.py, df_inmet recebe persist(StorageLevel.MEMORY_AND_DISK) após a normalização das chaves e é liberado com unpersist() depois da escrita.

A contagem existente de chaves inválidas materializa o cache. Não foi adicionado count de aquecimento. A fato com 8,4 milhões de registros não foi persistida. Join left, broadcast, regras, paths e escrita foram mantidos.

Foram identificadas nove actions consumidoras da linhagem após a preparação: validação de chaves, duplicidades, período, dois anti-joins, estatísticas do join, observações incompletas, amostra e gravação. As contagens iniciais do INMET permanecem antes da persistência.

O Experimento 2 teve três testes sintéticos, incluindo cinco subcasos de erro. Os testes confirmaram igualdade de métricas, linhas, schema e flags, além do uso de InMemoryTableScan. Os seis testes do Experimento 1 e cinco de orquestração também passaram. Esses são resultados da etapa de implementação, não testes repetidos nesta análise.

# Impacto global

| Comparação | Economia observada | Redução |
|:--|--:|--:|
| B → C: execução anterior para final | **800,674s — 13m20,674s** | **28,53%** |
| A → C: baseline para final | **500,810s — 8m20,810s** | **19,98%** |

A execução B havia ficado aproximadamente cinco minutos mais lenta que A, apesar da melhora do bloco do Experimento 1. A execução C ficou abaixo de ambas. Por isso, o relatório mantém os dois referenciais e evita usar somente a execução mais lenta como baseline.

A redução percentual foi calculada por (tempo anterior − tempo atual) / tempo anterior × 100.

# Comparação das 24 tasks

Todos os valores abaixo estão em segundos. A coluna “Economia B–C” é positiva quando a task ficou mais rápida e negativa quando ficou mais lenta.

| Task | A | B | C | Economia B–C |
|:--|--:|--:|--:|--:|
| cid10.bronze_cid10 | 0,480 | 3,557 | 0,599 | 2,958 |
| inmet.bronze_inmet | 0,487 | 3,562 | 0,613 | 2,949 |
| sih.bronze_sih | 0,661 | 3,769 | 0,781 | 2,988 |
| ibge.bronze_ibge | 0,480 | 3,551 | 0,590 | 2,961 |
| cid10.dq_bronze_cid10 | 0,443 | 0,584 | 0,431 | 0,152 |
| inmet.dq_bronze_inmet | 9,317 | 9,342 | 9,484 | -0,142 |
| ibge.dq_bronze_ibge | 0,529 | 0,644 | 0,528 | 0,116 |
| sih.dq_bronze_sih | 65,446 | 68,043 | 68,016 | 0,027 |
| ibge.silver_ibge | 8,230 | 9,943 | 8,966 | 0,978 |
| cid10.silver_cid10 | 9,457 | 11,818 | 10,613 | 1,205 |
| inmet.silver_inmet | 118,167 | 147,799 | 126,026 | 21,774 |
| ibge.dq_silver_ibge | 10,157 | 12,111 | 9,721 | 2,391 |
| cid10.dq_silver_cid10 | 9,565 | 11,683 | 9,398 | 2,285 |
| sih.silver_sih | 1.133,510 | 1.235,909 | 1.149,727 | 86,182 |
| inmet.dq_silver_inmet | 56,525 | 65,659 | 57,717 | 7,942 |
| gold.gold_municipio_estacao | 26,465 | 28,886 | 26,066 | 2,821 |
| inmet.silver_inmet_diario | 514,990 | 529,913 | 491,073 | 38,839 |
| gold.dq_gold_municipio_estacao | 8,567 | 8,646 | 9,946 | -1,300 |
| inmet.dq_silver_inmet_diario | 89,201 | 98,444 | 81,753 | 16,691 |
| sih.dq_silver_sih | 47,266 | 73,198 | 47,494 | 25,704 |
| gold.gold_fato_internacao | 617,946 | 513,360 | 396,221 | 117,139 |
| gold.dq_gold_fato_internacao | 19,542 | 23,785 | 18,230 | 5,555 |
| \textnormal{gold.gold\_fato\_\allowbreak internacao\_\allowbreak meteorologia} | 525,490 | 751,996 | 217,760 | 534,236 |
| \textnormal{gold.dq\_gold\_fato\_\allowbreak internacao\_\allowbreak meteorologia} | 91,577 | 130,848 | 103,579 | 27,269 |

As durações vêm do campo duration das task instances. Incluem custos do operador e dos processos, não apenas CPU Spark. A soma das economias das 24 tasks não equivale à redução da DAG, devido ao paralelismo.

Na run C, a soma de todas as durações é **2.845,329s**, enquanto o wall-clock é **2.005,611s**. O cálculo do impacto total deve considerar o caminho crítico.

# Experimento 1: resultado do bloco e da task

| Medição | A | B | C |
|:--|--:|--:|--:|
| Bloco de campos importantes | 273,001s | 57,380s | **45,625s** |
| Gold fato internação completa | 617,946s | 513,360s | **396,221s** |

O bloco ficou **83,29% mais rápido que o baseline A**, com economia de **227,376s**. Entre B e C, o bloco melhorou **11,755s**, enquanto a task inteira melhorou **117,139s**.

Como a implementação do Experimento 1 já estava presente em B, a redução adicional da task em C não deve ser tratada como efeito direto da nova persistência na Gold meteorológica: essa outra task executa depois da fato de internação.

Linha observada na run C:

~~~text
02:47:56.573277 UTC
[PERFORMANCE] Validação consolidada de campos importantes: 45.625 segundos
~~~

# Experimento 2: resultado da task meteorológica

| Comparação | Antes | Depois | Economia | Redução |
|:--|--:|--:|--:|--:|
| B → C | 751,996s | 217,760s | **534,236s** | **71,04%** |
| A → C | 525,490s | 217,760s | **307,730s** | **58,56%** |

A task passou de **12m31,996s para 3m37,760s** entre as duas últimas runs. A queda também é expressiva quando se usa A, anterior às otimizações, como referência.

## Cronômetros observados na execução final

| Log | Tempo |
|:--|--:|
| INMET preparação/materialização | **43,191s** |
| INMET validações: chave, período e estações | **45,639s** |
| Join meteorológico até agregação de validação | **1,345s** |
| INMET reutilização total: persistência até escrita | **95,664s** |

Os intervalos se sobrepõem e **não devem ser somados**. Preparação/materialização inclui a primeira action que preenche o cache. Validações inclui esse primeiro cálculo. Reutilização total inclui validações, join, métricas, amostra e escrita.

O tempo de reutilização também não é a duração total da task. Aproximadamente **122,096s** da task estão fora desse intervalo, incluindo inicialização, trabalho anterior à persistência e verificações posteriores à escrita. Não se deve atribuir todo esse restante a uma única operação.

## Comparação dos mesmos marcadores de log

Para evitar comparar cronômetros com abrangências diferentes, os seguintes intervalos foram reconstruídos usando mensagens que existem nas três runs.

| Trecho | A (s) | B (s) | C (s) | Redução B–C |
|:--|--:|--:|--:|--:|
| Chaves inválidas e duplicidades | 79,828 | 129,011 | 43,636 | 66,18% |
| Chave, período e estações | 200,665 | 316,786 | 45,636 | 85,59% |
| Join até estatísticas | 43,996 | 55,259 | 1,345 | 97,57% |
| Gravação até validação pós-escrita | 80,687 | 105,351 | 46,076 | 56,26% |
| Estatísticas do join até início da escrita | 86,988 | 105,889 | 2,553 | 97,59% |

Definições dos intervalos:

- **Chaves inválidas e duplicidades:** “Validando chave codigo_estacao + data...” até “Duplicidades na chave INMET”.
- **Chave, período e estações:** a mesma abertura até “Códigos de estação compatíveis”.
- **Join até estatísticas:** “Integrando fato_internacao...” até “Registros antes do JOIN”, emitido após a action agregada.
- **Gravação:** “Gravando Gold...” até “Validando gravação”.
- **Estatísticas até escrita:** de “Registros antes do JOIN” até “Gravando Gold”, incluindo regras, outras métricas e amostra.

A diferença entre os **45,636s derivados dos timestamps** e os **45,639s do perf_counter** é de milissegundos, compatível com pontos de início/fim e transporte de logs distintos.

Os **1,345s do join incluem a agregação de validação**; não representam todas as actions posteriores que voltam a consumir o join, nem somente tempo do algoritmo de join. Da mesma forma, o intervalo de gravação inclui computação da linhagem e escrita, não apenas I/O de disco.

A redução expressiva aparece em várias reutilizações da mesma linhagem, e não somente na primeira contagem. Esse padrão é consistente com o objetivo da persistência. Não substitui medição histórica de leitura física, cache hit, GC ou spill.

# Caminho crítico e contribuição para a redução

A sequência dominante permanece:

~~~text
sih.bronze_sih
  → sih.dq_bronze_sih
  → sih.silver_sih
  → sih.dq_silver_sih
  → gold.gold_fato_internacao
  → gold.dq_gold_fato_internacao
  → gold.gold_fato_internacao_meteorologia
  → gold.dq_gold_fato_internacao_meteorologia
~~~

Nas convergências, o DQ Bronze SIH terminou depois do DQ Bronze IBGE; o DQ Silver SIH terminou depois das demais entradas da fato; e o DQ da fato terminou depois do DQ INMET diário. Assim, a cadeia acima determina a conclusão desta run.

| Task crítica | Duração C | Economia B–C (s) |
|:--|--:|--:|
| sih.bronze_sih | 00:00:00,781 | 2,988 |
| sih.dq_bronze_sih | 00:01:08,016 | 0,027 |
| sih.silver_sih | 00:19:09,727 | 86,182 |
| sih.dq_silver_sih | 00:00:47,494 | 25,704 |
| gold.gold_fato_internacao | 00:06:36,221 | 117,139 |
| gold.dq_gold_fato_internacao | 00:00:18,230 | 5,555 |
| \textnormal{gold.gold\_fato\_\allowbreak internacao\_\allowbreak meteorologia} | 00:03:37,760 | 534,236 |
| \textnormal{gold.dq\_gold\_fato\_\allowbreak internacao\_\allowbreak meteorologia} | 00:01:43,579 | 27,269 |

| Componente | B | C | Economia |
|:--|--:|--:|--:|
| Tasks do caminho crítico | 2.800,906s | 2.001,807s | 799,100s |
| Transições, início e encerramento | 5,379s | 3,805s | 1,574s |
| **Duração da DAG** | **2.806,285s** | **2.005,611s** | **800,674s** |

A economia da Gold meteorológica, **534,236s**, equivale a **66,72%** da economia total. Os demais **266,438s** vêm da variação nas outras tasks críticas e nas transições.

Essa é uma decomposição contábil do tempo observado. Não significa que 66,72% seja uma estimativa causal isolada do efeito do cache.

# Paralelismo e gargalo atual

O ramo INMET diário rodou em paralelo ao SIH: sua Silver executou das **02:26:41,254 às 02:34:52,327 UTC**, inteiramente dentro da janela da Silver SIH, das **02:24:34,406 às 02:43:44,133 UTC**.

O DQ INMET diário terminou às **02:36:14,873**, enquanto o DQ da fato só terminou às **02:51:27,341**. O INMET ficou disponível **15m12,468s antes** da outra dependência necessária à integração meteorológica.

A Silver SIH continua sendo a maior task:

- **19m09,727s** nesta execução.
- **57,33% do wall-clock da DAG**.
- Quase o mesmo tempo do baseline A: 18m53,510s.
- Mais rápida que B em 1m26,182s.

Os dois experimentos reduziram o peso das fatos Gold, tornando o SIH proporcionalmente mais dominante. A análise anterior localizou grande parte do tempo SIH na conversão DBF → Parquet; essa decomposição interna não foi medida novamente nesta tarefa.

# Evidências de preservação funcional

Os mesmos números foram observados nas três execuções comparadas:

| Verificação | Resultado |
|:--|--:|
| INMET diário | **43.840 registros** |
| Estações INMET | **40** |
| Fato antes do join | **8.409.047** |
| Fato depois do join | **8.409.047** |
| Registros gravados | **8.409.047** |
| Internações com match | **8.236.990** |
| Internações sem match | **172.057** |
| Sem match antes do período INMET | **172.057** |
| Sem match depois do período INMET | **0** |
| Sem match dentro do período INMET | **0** |
| Duplicidades de chave INMET | **0** |

As internações anteriores ao período meteorológico foram preservadas. Os logs do DQ final registraram PASS para schema, tipos, cardinalidade, preservação da fato base, nulos/vazios obrigatórios, domínio da flag, unicidade da chave INMET, correspondência das estações, coerência entre flag e motivo, 24 observações nos matches e ausência de métricas sem match.

Coberturas aprovadas no DQ final:

| Variável | Cobertura | Referência |
|:--|--:|--:|
| Temperatura | 95,04% | 90% |
| Umidade | 94,66% | 90% |
| Precipitação | 84,94% | 80% |
| Vento | 86,78% | 80% |

Essas evidências sustentam preservação dos critérios verificados. Não constituem uma comparação linha a linha de todos os arquivos antes/depois, nem prova de igualdade completa de cada valor do dataset.

# Ambiente e limites de causalidade

Os comandos spark-submit registrados mantêm **local[2]**. Na fato de internação permanece autoBroadcastJoinThreshold=-1; na Gold meteorológica permanecem shuffle.partitions=40 e o broadcast explícito do código. Os logs da Gold meteorológica e do DQ final registram Spark 3.5.0.

O workspace contém uma alteração preexistente no docker-compose.airflow.yaml: a publicação da API mudou de **8080:8080 para 8180:8080**. Ela foi apenas observada, não alterada nesta análise. O mapeamento da porta, isoladamente, não demonstra explicação para a redução dos jobs; a inspeção não comprova se houve reinício de serviços ou outras mudanças de ambiente entre runs.

Limitações:

- Há uma execução por estado analisado, sem desenho experimental controlado.
- A e B já apresentavam variação relevante antes da persistência.
- Tasks não alteradas pelo Experimento 2 também ficaram mais rápidas.
- Não há séries históricas de CPU, memória, I/O, GC ou spill nesta análise.
- Cache do sistema operacional, carga concorrente e comportamento de I/O podem variar; não foram identificados como causa comprovada.
- As durações de task incluem inicialização e encerramento; os marcadores de log não são perfis exclusivos de cada estágio.
- A mesma versão de DAG não garante igualdade do conteúdo dos datasets, imagens ou scripts externos em cada instante histórico.

A conclusão defensável é: **a execução após os dois experimentos foi mais rápida, com maior economia concentrada na task alvo do Experimento 2 e manutenção dos controles funcionais observados**.

# Recomendação e redação para o TCC

Recomenda-se manter as duas alterações localizadas como resultado implementado e registrar estas medições como evidência operacional. Se for necessário sustentar um ganho típico, repetir execuções em condições comparáveis e reportar mediana e dispersão, sem usar apenas o melhor tempo. Nenhuma nova execução foi disparada nesta análise.

Texto sugerido:

> Após a consolidação das métricas de validação e a persistência do DataFrame meteorológico preparado, o pipeline apresentou duração de 33min25,611s. Em comparação à execução imediatamente anterior, de 46min46,285s, observou-se redução de 28,53%. A etapa de integração meteorológica apresentou redução de 751,996s para 217,760s, mantendo 8.409.047 registros e aprovação dos controles de qualidade. Como as medições não foram realizadas em um experimento controlado com repetições, os valores representam ganhos observados e não uma estimativa isolada de causalidade.

\begin{landscape}
\section{Horários das 24 tasks da execução final}
Horários UTC de 11/09/2026, truncados a milissegundos. Todas em success, tentativa 1.
\par\medskip
\scriptsize
\setlength{\tabcolsep}{4pt}
\renewcommand{\arraystretch}{1.25}
\begin{longtable}{p{102mm}p{38mm}p{38mm}p{35mm}}
\toprule
Task & Início & Fim & Duração\\
\midrule
\endfirsthead
\toprule
Task & Início & Fim & Duração\\
\midrule
\endhead
cid10.bronze\_cid10 & 02:23:24.963 & 02:23:25.562 & 00:00:00,599 \\
inmet.bronze\_inmet & 02:23:24.963 & 02:23:25.576 & 00:00:00,613 \\
sih.bronze\_sih & 02:23:24.964 & 02:23:25.745 & 00:00:00,781 \\
ibge.bronze\_ibge & 02:23:24.968 & 02:23:25.558 & 00:00:00,590 \\
cid10.dq\_bronze\_cid10 & 02:23:26.086 & 02:23:26.517 & 00:00:00,431 \\
inmet.dq\_bronze\_inmet & 02:23:26.086 & 02:23:35.570 & 00:00:09,484 \\
ibge.dq\_bronze\_ibge & 02:23:26.086 & 02:23:26.614 & 00:00:00,528 \\
sih.dq\_bronze\_sih & 02:23:26.086 & 02:24:34.102 & 00:01:08,016 \\
ibge.silver\_ibge & 02:23:27.241 & 02:23:36.207 & 00:00:08,966 \\
cid10.silver\_cid10 & 02:23:27.241 & 02:23:37.855 & 00:00:10,613 \\
inmet.silver\_inmet & 02:23:36.086 & 02:25:42.112 & 00:02:06,026 \\
ibge.dq\_silver\_ibge & 02:23:37.239 & 02:23:46.960 & 00:00:09,721 \\
cid10.dq\_silver\_cid10 & 02:23:38.378 & 02:23:47.776 & 00:00:09,398 \\
sih.silver\_sih & 02:24:34.406 & 02:43:44.133 & 00:19:09,727 \\
inmet.dq\_silver\_inmet & 02:25:43.208 & 02:26:40.925 & 00:00:57,717 \\
gold.gold\_municipio\_estacao & 02:26:41.240 & 02:27:07.305 & 00:00:26,066 \\
inmet.silver\_inmet\_diario & 02:26:41.254 & 02:34:52.327 & 00:08:11,073 \\
gold.dq\_gold\_municipio\_estacao & 02:27:08.190 & 02:27:18.136 & 00:00:09,946 \\
inmet.dq\_silver\_inmet\_diario & 02:34:53.120 & 02:36:14.873 & 00:01:21,753 \\
sih.dq\_silver\_sih & 02:43:44.336 & 02:44:31.830 & 00:00:47,494 \\
gold.gold\_fato\_internacao & 02:44:32.571 & 02:51:08.792 & 00:06:36,221 \\
gold.dq\_gold\_fato\_internacao & 02:51:09.111 & 02:51:27.341 & 00:00:18,230 \\
gold.gold\_fato\_internacao\_meteorologia & 02:51:27.733 & 02:55:05.493 & 00:03:37,760 \\
gold.dq\_gold\_fato\_internacao\_meteorologia & 02:55:06.370 & 02:56:49.949 & 00:01:43,579 \\
\bottomrule
\end{longtable}
\normalsize
\end{landscape}

# Rastreabilidade e entregáveis

Fontes consultadas:

- PostgreSQL/Airflow: dag_run e task_instance, com filtro dag_id=tcc_pipeline e as três run_ids identificadas.
- Logs de gold.gold_fato_internacao, gold.gold_fato_internacao_meteorologia e respectivos DQs; comandos spark-submit e timestamps das tasks.
- Código atual das duas Gold, dependências da DAG e diff do Compose.
- Relatórios anteriores: Relatorio_Performance_Operacional_Airflow e Relatorio_Experimento_2_Persistencia_INMET.

Valores calculados:

- Wall-clock = end_date − start_date da run.
- Redução = duração anterior − duração final.
- Percentual = redução / duração anterior × 100.
- Caminho crítico = sequência de dependências cujos predecessores terminaram por último nas convergências.
- Intervalos internos = diferenças entre os mesmos marcadores de log.
- Participação da Gold meteorológica = sua economia / economia do wall-clock.

Os logs estão sob logs/dag_id=tcc_pipeline, em diretórios run_id correspondentes. No Windows, os dois-pontos podem aparecer com representação especial nos nomes desses diretórios.

Arquivos produzidos nesta análise: **docs/Relatorio_Resultados_Performance_Experimentos_1_2.md** e sua versão **PDF**. Os relatórios anteriores foram preservados.
