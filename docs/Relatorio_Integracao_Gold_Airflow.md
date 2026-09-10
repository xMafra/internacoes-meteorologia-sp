---
title: "Integração da camada Gold ao Apache Airflow"
subtitle: "Auditoria, refatoração de runtime e validação da DAG"
date: "9 de setembro de 2026"
lang: pt-BR
geometry: margin=2.2cm
fontsize: 11pt
toc-title: "Sumário"
---

# Resultado e estado inicial

A DAG tcc_pipeline foi ampliada de quatro TaskGroups e dez tasks para cinco TaskGroups e 16 tasks. O novo grupo gold contém três transformações e seus três gates de qualidade. Cada transformação aguarda os gates de todas as fontes identificadas no código.

O usuário informou que a execução completa das dez tasks Silver/DQ, após a refatoração anterior, terminou em SUCCESS. Essa informação constitui o baseline operacional informado; nesta tarefa não se repetiu a execução de datasets.

O repositório já apresentava mudanças não commitadas da refatoração Silver e seus relatórios. Elas foram preservadas. Antes de editar, foi salvo um snapshot textual em /tmp no worker para comparação com o estado inicial desta tarefa. O diff contra HEAD inclui também essas alterações anteriores e não deve ser confundido com o incremento Gold.

Foram encontrados três scripts em src/gold e três DQs, common.py, quality_config.py e __init__.py em src/quality/gold. A DAG utiliza SparkSubmitOperator, spark_default, retries 1 nas transformações, retries 0 nos DQs e durable=False. A stack declarada no projeto é Airflow 3.3.1, provider Spark 6.3.2, PySpark 3.5.0, Python 3.11 e Java 17.

# Auditoria dos datasets e dependências

Todos os caminhos abaixo são relativos a /home/jovyan/work/data. Foram extraídos das constantes e chamadas de leitura efetivamente usadas nos scripts, não inferidos pelos nomes dos jobs.

| Saída Gold | Entradas reais | Gates prévios |
|:--|:--|:--|
| gold/municipio_estacao | silver/ibge/2022; silver/inmet/2023, /2024 e /2025 | DQ Silver IBGE e INMET horário |
| gold/fato_internacao | silver/sih; silver/ibge; silver/cid10; gold/municipio_estacao | DQ SIH, IBGE, CID10 e município-estação |
| gold/fato_internacao_meteorologia | gold/fato_internacao; silver/inmet_diario | DQ fato internação e DQ INMET diário |

Município-estação usa INMET horário, não a agregação diária. Por isso pode iniciar após o DQ horário, sem aguardar o diário. A fato meteorológica também relê sua própria saída para validação pós-gravação; essa leitura não é uma dependência externa nem cria uma aresta circular.

## Entradas dos DQs

- DQ município-estação: lê apenas gold/municipio_estacao.
- DQ fato internação: lê a fato, Silver SIH, Silver IBGE/2022, Silver CID10 e Gold município-estação.
- DQ fato meteorologia: lê a fato meteorológica, a fato base e INMET diário.

Os inputs adicionais dos DQs já estão protegidos pelos ancestrais da transformação imediatamente anterior. O trigger rule all_success foi verificado em todas as seis tasks Gold. Assim, o sucesso de uma única fonte não libera uma transformação que ainda aguarda outras.

## Ambiguidade preservada

A transformação fato_internacao lê silver/ibge recursivamente, enquanto o DQ IBGE e o DQ da fato leem silver/ibge/2022. O caminho não foi alterado para evitar mudança funcional especulativa. Se novas subpastas forem adicionadas sob silver/ibge, a transformação poderá consumir dados além dos cobertos pelo gate de 2022. A equivalência do conteúdo deve ser observada em execução real; não foi feita varredura dos datasets nesta tarefa.

# Auditoria Spark e transferência de configurações

| Origem | Baseline encontrado | Classificação e destino |
|:--|:--|:--|
| municipio_estacao.py | master local[2]; UI false | Runtime: master permanece na connection; UI na base comum da DAG |
| fato_internacao.py | autoBroadcastJoinThreshold -1 | Tuning: conf específico da task da fato |
| fato_internacao_meteorologia.py | shuffle 40; UI false | Tuning/runtime: conf da task meteorológica |
| Helper DQ herdado da Silver | master local[2]; UI false; shuffle 4 | Runtime/tuning: connection, base comum e conf shuffle 4 dos três DQs |

Não foram encontrados nos scripts Gold hardcodes de memória, executor cores ou caminhos Python específicos de imagem. appName e nível de log ERROR foram preservados. Opções como mergeSchema e recursiveFileLookup fazem parte da leitura existente e permaneceram intactas.

A propriedade de broadcast foi preservada especificamente na transformação fato internação, pois o comentário anterior registrava sua finalidade de reduzir pressão sobre a memória do driver. O comentário explicativo foi transferido para a DAG. Esse tuning não foi estendido aos DQs, pois eles não o possuíam no baseline.

## Configuração final das seis tasks

Todas usam spark_default e spark.ui.enabled=false. Nenhuma recebe spark.master no conf. Não foram adicionadas configurações de Python, memória ou cores.

| Task dentro do grupo gold | Conf adicional | Retries |
|:--|:--|:--:|
| gold_municipio_estacao | Nenhum | 1 |
| dq_gold_municipio_estacao | spark.sql.shuffle.partitions=4 | 0 |
| gold_fato_internacao | spark.sql.autoBroadcastJoinThreshold=-1 | 1 |
| dq_gold_fato_internacao | spark.sql.shuffle.partitions=4 | 0 |
| gold_fato_internacao_meteorologia | spark.sql.shuffle.partitions=40 | 1 |
| dq_gold_fato_internacao_meteorologia | spark.sql.shuffle.partitions=4 | 0 |

As transformações recebem retry_delay de um minuto. As seis tasks mantêm durable=False, conforme o padrão existente. A ausência de shuffle explícito em uma task significa uso da configuração efetiva do ambiente Spark, não ausência de particionamento.

# Arquivos modificados e criados

Somente cinco arquivos de implementação foram modificados nesta tarefa:

1. dags/tcc_pipeline.py: acrescentados dois dicionários de tuning, seis constantes de caminhos, o TaskGroup gold, seis operadores e suas dependências.
2. src/gold/municipio_estacao.py: removidas duas chamadas do builder, master e UI.
3. src/gold/fato_internacao.py: removida a chamada de configuração de broadcast e seu comentário local; ajuste preservado na DAG.
4. src/gold/fato_internacao_meteorologia.py: removidas as configurações de shuffle e UI do builder.
5. src/quality/gold/common.py: substituída a reexportação do helper Silver por implementação própria de criar_spark.

Não foi necessário criar spark_session.py para Gold: o common.py existente já é a interface importada pelos três DQs e agora implementa a sessão mínima diretamente. Isso elimina a dependência da Silver sem alterar os imports ou regras dos DQs. __all__ e assinatura da função foram preservados.

Não foram criados arquivos de código. Foram criados este relatório Markdown e sua versão PDF. O helper Silver antigo permaneceu intacto, fora do escopo de limpeza desta tarefa; o Gold não o utiliza mais.

# Grafo final da DAG

Os nomes abaixo representam as tasks dentro de seus respectivos grupos. Uma lista entre colchetes exige sucesso de todos os elementos.

```text
inmet:
  silver_inmet -> dq_silver_inmet
    -> silver_inmet_diario -> dq_silver_inmet_diario
ibge:
  silver_ibge -> dq_silver_ibge
cid10:
  silver_cid10 -> dq_silver_cid10
sih:
  silver_sih -> dq_silver_sih

gold:
  [dq_silver_ibge, dq_silver_inmet]
    -> gold_municipio_estacao
    -> dq_gold_municipio_estacao

  [dq_silver_sih, dq_silver_ibge, dq_silver_cid10,
   dq_gold_municipio_estacao]
    -> gold_fato_internacao
    -> dq_gold_fato_internacao

  [dq_gold_fato_internacao, dq_silver_inmet_diario]
    -> gold_fato_internacao_meteorologia
    -> dq_gold_fato_internacao_meteorologia
```

As dependências entre as dez tasks originais foram preservadas. Foram adicionadas somente as arestas para Gold e entre Gold e seus gates. Os ids completos das novas tasks têm prefixo gold., por exemplo gold.gold_fato_internacao.

# Validações executadas

Os checks Python foram executados no worker com docker compose -f docker-compose.airflow.yaml exec -T airflow-worker python -B -c. Não iniciaram sessões Spark nem executaram os corpos dos scripts de transformação.

| Verificação | Resultado |
|:--|:--|
| compile dos arquivos no snapshot, incluindo DAG e helper alterados | PASS |
| Execução isolada dos imports extraídos por AST dos módulos Gold/DQ | PASS |
| Comparação textual da Silver e dos três DQs Gold com snapshot | PASS: intactos |
| Comparação AST dos três scripts Gold, ignorando apenas chamadas config/master anteriores | PASS: restante da estrutura executável idêntico |
| Busca por .config, .master, /opt/conda e dependência src.quality.silver em Gold/DQ Gold | Nenhuma ocorrência |
| Importação da DAG via runpy no Airflow | PASS |
| Número de grupos e tasks | PASS: cinco grupos, 16 tasks |
| Comparação das dez tasks originais com baseline | PASS: conf, aplicação, retries, delay, connection, nome, trigger rule e arestas internas preservados |
| Upstreams exatos das seis tasks Gold e reciprocidade dos downstreams | PASS |
| Ordenação topológica | PASS: 16 tasks, sem ciclos |
| all_success, retries, connection e existência dos scripts Gold | PASS |
| Comparação exata dos conf das seis tasks Gold | PASS |
| Simulação de erro crítico nos três main() DQ Gold | PASS: ValueError propagado e spark.stop chamado |
| airflow dags list-import-errors | No data found |
| git diff --check | Sem erros; avisos de normalização LF/CRLF |
| Revisão integral do diff | Mudanças restritas ao escopo, além do baseline anterior já existente |

Os gates foram testados com relatório crítico e sessão simulados, preservando a chamada real a exigir_aprovacao. As funções de validação de dados foram substituídas por mocks somente no processo temporário do teste, sem editar seus arquivos. A propagação de erro foi comprovada em Python; o estado FAILED de uma task real não foi observado nesta tarefa.

# Estatísticas Git e resumo do diff

O comando git diff --stat retornou o estado acumulado contra HEAD:

```text
dags/tcc_pipeline.py                     | 155
src/gold/fato_internacao.py              |   4
src/gold/fato_internacao_meteorologia.py |   8
src/gold/municipio_estacao.py            |   2
src/quality/gold/common.py               |  13
src/quality/silver/dq_cid10.py           |   2
src/quality/silver/dq_ibge.py            |   2
src/quality/silver/dq_inmet.py           |   2
src/quality/silver/dq_inmet_diario.py    |   2
src/quality/silver/dq_sih.py             |   2
src/silver/cid10.py                      |   2
src/silver/ibge.py                       |   2
src/silver/inmet.py                      |  15
src/silver/inmet_diario.py               |   2
src/silver/sih.py                        |   3
15 files changed, 165 insertions(+), 51 deletions(-)
```

Arquivos não rastreados, inclusive relatórios e o helper Silver criado anteriormente, não entram nessa estatística. As linhas Silver exibidas pertencem à refatoração anterior. A comparação com o snapshot inicial desta tarefa confirmou que elas não foram modificadas agora.

O incremento Gold acrescenta configurações e orquestração à DAG, remove 14 linhas dos builders/comentário dos três scripts Gold e transforma common.py em helper independente. Não houve mudança em joins, compatibilidade CID-10, regras meteorológicas, schemas, colunas, filtros, granularidade, valores esperados, paths ou escrita. Nenhum commit ou push foi feito.

# Limitações, riscos e validação operacional pendente

O pipeline Gold de mais de oito milhões de registros não foi executado. Nenhum dataset foi apagado ou sobrescrito. Também não foram alterados Dockerfiles, requirements, Docker Compose, connection ou permissões.

Os testes de estrutura e AST sustentam a preservação da lógica, mas não substituem execução real para validar recursos, resolução do master, comportamento do spark-submit, duração, consumo de memória e resultados dos DQs. Os valores armazenados na connection não foram reconfigurados nem consultados; a DAG mantém sua referência.

Na próxima execução operacional, devem ser observados os três gates, as contagens esperadas existentes, a aplicação efetiva do broadcast -1 e shuffle 40/4, e a ordem de liberação das tasks. A divergência de escopo do path IBGE deve ser considerada se houver outras subpastas além de 2022.

As Gold usam os modos de escrita existentes, inclusive overwrite. Uma futura execução real poderá substituir suas saídas como já previsto nos scripts; nenhuma escrita ocorreu nesta tarefa. Execuções manuais também passam a depender da submissão externa para receber o tuning e o master que foram retirados dos scripts.

O relatório anterior da Silver é um registro histórico: sua observação de que o DQ Gold ainda dependia do helper Silver descreve o estado anterior. Esta implementação resolve essa dependência.
