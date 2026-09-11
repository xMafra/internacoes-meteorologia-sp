---
title: "Auditoria técnica da camada Bronze"
subtitle: "Inventário, riscos e proposta de integração ao Apache Airflow"
date: "9 de setembro de 2026"
lang: pt-BR
geometry: margin=2.2cm
fontsize: 11pt
toc-title: "Sumário"
---

# Objetivo, escopo e conclusão

Este documento registra a auditoria da camada Bronze do TCC de Engenharia de Dados e a proposta de integração à DAG tcc_pipeline. A auditoria foi realizada por leitura do repositório, listagens de arquivos, consulta aos metadados dos ZIPs e imports leves no worker Airflow. Não houve downloads, execução dos DQs completos, conversão de dados ou execução do pipeline.

Durante a auditoria, nenhum arquivo foi modificado. Este Markdown e sua versão PDF foram criados posteriormente, mediante solicitação de documentação. As propostas deste relatório não foram implementadas.

A Bronze já possui funções de aquisição para SIH, INMET e IBGE, além de extração local para CID-10. Os quatro DQs Bronze existem. A integração exige wrappers que respeitem os resultados dos gates, parâmetros temporais explícitos e tratamento cuidadoso dos arquivos existentes.

Os achados centrais são: dependência cruzada da Silver SIH em Bronze IBGE; aquisição externa CID-10 não implementada; downloads que verificam existência, mas não integridade; e custo potencialmente relevante dos DQs SIH e INMET.

# Baseline operacional

O usuário informou que as 16 tasks atuais de Silver, DQ Silver, Gold e DQ Gold terminaram em SUCCESS em uma execução completa de aproximadamente 45 minutos. Esse é o baseline operacional informado, não uma medição realizada nesta auditoria.

A DAG possui cinco TaskGroups: inmet, ibge, cid10, sih e gold. O ambiente do projeto utiliza Airflow 3.3.1, Spark/PySpark 3.5.0, Python 3.11, Java 17, Docker e arquitetura Bronze–Silver–Gold.

# Inventário dos scripts e fontes

| Fonte | Script e funções | Entrada | Saída e método |
|:--|:--|:--|:--|
| SIH | src/bronze/sih.py; download_sih e download_sih_periodo | FTP DATASUS | DBC mensal, download urllib |
| INMET | src/bronze/inmet.py; download_inmet | HTTPS INMET | ZIP anual, download urllib em blocos |
| IBGE | src/bronze/ibge.py; download_malha_sp_2022 | HTTPS IBGE | ZIP original SP/2022, download urllib em blocos |
| CID-10 | src/bronze/cid10.py; processar_cid10 e extrair_cid10 | CID10CSV.zip local | Extração de quatro CSVs |

Os módulos de aquisição SIH, INMET e IBGE definem funções, mas não possuem um bloco de execução que dispare a coleta. Executar apenas o arquivo Python não inicia o download. CID-10 possui bloco __main__ que chama a preparação local.

A busca fora de src/bronze não identificou outro fluxo implementado de aquisição. O notebook de validação contém referências e erros históricos de importação do PySUS; isso não torna PySUS uma dependência dos coletores atuais.

## Endpoints registrados no código

```text
SIH:
ftp://ftp.datasus.gov.br/dissemin/publicos/SIHSUS/
200801_/Dados/RDSP{AA}{MM}.dbc

INMET:
https://portal.inmet.gov.br/uploads/dadoshistoricos/{ANO}.zip

IBGE:
https://geoftp.ibge.gov.br/organizacao_do_territorio/
malhas_territoriais/malhas_municipais/municipio_2022/
UFs/SP/SP_Municipios_2022.zip
```

As quebras acima são apenas para apresentação. Não há autenticação explícita nos coletores. Apesar do nome geoftp, o script IBGE utiliza HTTPS.

Não foi encontrada URL ou implementação de download do ZIP CID-10. Sua aquisição deve ser tratada como provisionamento externo, possivelmente manual, sem afirmar uma origem não comprovada pelo repositório. Fonte, versão e política de atualização precisam ser documentadas antes de automatizar essa etapa.

# Estrutura real dos datasets

A listagem encontrou 45 arquivos, aproximadamente 978 MB em unidades decimais, sem arquivos com extensão .tmp.

```text
data/bronze/
  sih/
    2023/RDSP2301.dbc ... RDSP2312.dbc
    2024/RDSP2401.dbc ... RDSP2412.dbc
    2025/RDSP2501.dbc ... RDSP2512.dbc
  inmet/
    2023/2023.zip
    2024/2024.zip
    2025/2025.zip
  ibge/
    2022/SP_Municipios_2022.zip
  cid10/
    CID10CSV.zip
    CID-10-CAPITULOS.CSV
    CID-10-GRUPOS.CSV
    CID-10-CATEGORIAS.CSV
    CID-10-SUBCATEGORIAS.CSV
```

| Fonte | Arquivos | Bytes | Granularidade e organização |
|:--|--:|--:|:--|
| SIH | 36 | 665.115.072 | Um DBC RD de SP por competência; registros administrativos de internação/AIH |
| INMET | 3 | 300.740.085 | ZIP nacional por ano; CSVs por estação com observações horárias |
| IBGE | 1 | 10.464.202 | Malha municipal de SP, referência 2022; atributos e geometrias |
| CID-10 | 5 | 1.885.997 | ZIP e quatro tabelas hierárquicas de classificação |

A Bronze preserva DBC, ZIP e CSV, não Parquet. As conversões pertencem às etapas seguintes. A estrutura temporal e os nomes atuais são consumidos diretamente pela Silver.

## Inspeção leve dos ZIPs

Foram consultados somente os diretórios centrais dos arquivos, sem extração ou teste integral de CRC:

- INMET 2023: 567 membros; 415.560.362 bytes descompactados informados.
- INMET 2024: 565 membros; 405.471.533 bytes descompactados informados.
- INMET 2025: 595 membros, incluindo diretório; 376.671.813 bytes descompactados informados.
- IBGE: cinco membros, incluindo DBF, SHP, SHX, PRJ e CPG.
- CID-10: seis membros; o extrator seleciona quatro tabelas.

Os ZIPs INMET totalizam cerca de 1,20 GB descompactados e contêm estações de outros estados. A Silver filtra SP. Ela aceita CSVs na raiz e também na subpasta 2025/. Presença e metadados não comprovam integridade integral nem aprovação nos DQs.

# Relação Bronze–Silver

Todos os paths desta seção são relativos a /home/jovyan/work/data.

| Transformação | Entradas efetivamente lidas | Gates necessários |
|:--|:--|:--|
| Silver SIH | bronze/sih/{ano}/RDSP{AA}{MM}.dbc e bronze/ibge/2022/SP_Municipios_2022.zip | DQ Bronze SIH e DQ Bronze IBGE |
| Silver INMET | bronze/inmet/{ano}/{ano}.zip, 2023–2025 | DQ Bronze INMET |
| Silver IBGE | bronze/ibge/2022/SP_Municipios_2022.zip | DQ Bronze IBGE |
| Silver CID10 | Quatro CSVs extraídos em bronze/cid10 | DQ Bronze CID10 |
| Silver INMET diário | Silver INMET | DQ Silver INMET, já existente |

A função de leitura de municípios em src/silver/sih.py abre diretamente o membro SP_Municipios_2022.dbf do ZIP IBGE. Portanto, DQ Bronze IBGE deve liberar tanto Silver IBGE quanto Silver SIH. Essa leitura não exige esperar pela transformação Silver IBGE.

# Estado dos DQs Bronze

Os quatro scripts estão em src/quality/bronze. O common.py define Report e Check. Não existe nível WARNING nesse modelo: qualquer check falso reprova o relatório.

| DQ | Verificações existentes | Limites |
|:--|:--|:--|
| dq_sih.py | Competências presentes; tamanho; nome/diretório; conversão DBC para DBF; campos e primeiro registro; duplicidade SHA-256; temporários | Não valida todos os registros nem a competência contida nos dados |
| dq_inmet.py | ZIPs anuais; tamanho; nome/diretório; CRC; presença de CSVs; cabeçalho e ano em amostra; SHA-256; temporários | Não comprova cobertura completa de SP ou todas as observações |
| dq_ibge.py | Presença; tamanho; ano; CRC; conjunto SHP/SHX/DBF/PRJ; temporários | Não valida atributos, geometrias, quantidade de municípios ou nome exato exigido por SIH |
| dq_cid10.py | ZIP e CSVs; tamanhos; CRC; membros esperados; cabeçalhos; igualdade SHA-256 ZIP/extraídos; temporários | Cabeçalho com duas ou mais colunas não comprova schema completo |

O DQ SIH converte cada DBC inteiro em DBF temporário, embora examine apenas o primeiro registro depois. O DQ INMET usa testzip, percorrendo conteúdo descompactado, e posteriormente calcula hashes dos ZIPs. Não são apenas verificações de existência.

## Falhas e integração com tasks Python

Os quatro main() retornam 0 quando aprovados e 1 quando reprovados. Seus blocos de execução usam SystemExit(main()). Assim, pela linha de comando, reprovação produz código de saída 1.

Uma task Python que apenas retorne validar_*() não falhará automaticamente com um relatório reprovado. Tampouco retornar o inteiro 1 de main() equivale a FAILED em PythonOperator. O wrapper futuro deve verificar report.aprovado e lançar exceção na reprovação.

Erros inesperados não tratados também podem falhar o processo, possivelmente sem relatório completo. Nenhuma regra foi alterada ou executada nesta auditoria.

# Idempotência e reprocessamento

Os quatro processos foram classificados como parcialmente idempotentes, considerando falhas e integridade, e não apenas repetição bem-sucedida.

| Fonte | Proteções atuais | Fragilidades |
|:--|:--|:--|
| SIH | Ignora destino existente; usa .dbc.tmp; promove com replace; limpa temporário nas exceções tratadas | Existente inválido é ignorado; sem timeout explícito ou retomada por byte |
| INMET | Ignora existente; usa .zip.tmp; download em blocos e promoção | ZIP não validado antes da promoção; ausência de atualização/versionamento |
| IBGE | Mesmo padrão INMET | Não detecta revisão remota nem recupera arquivo inválido existente |
| CID-10 | Destinos determinísticos; mesmo ZIP reproduz mesmos CSVs após sucesso | Sobrescrita direta por arquivo; extração parcial ou mistura de versões em falha |

Riscos comuns dos downloads:

- Existência de arquivo vazio ou corrompido impede nova aquisição automática.
- Interrupção abrupta pode deixar temporário, apesar da limpeza nas exceções tratadas.
- Execuções concorrentes do mesmo destino compartilham o nome temporário.
- Não há comparação com checksum oficial, ETag ou versão remota.
- Não há timeout explícito no código.
- Conteúdo inválido pode chegar ao nome definitivo antes do DQ.
- Retomada reinicia o arquivo incompleto; não continua do último byte.

O lote Bronze SIH para na primeira exceção. Ao repetir, ignora os destinos já presentes. Isso difere do lote Silver SIH, que tenta os demais meses e falha ao final.

Na CID-10, cada CSV é aberto com wb; o conjunto inteiro não é publicado atomicamente. Uma futura recuperação deve definir explicitamente o tratamento de arquivos inválidos antes de substituí-los.

# Dependências externas e ambiente

| Processo | Dependências Python | Recursos externos ou de sistema |
|:--|:--|:--|
| Download SIH | urllib e pathlib, biblioteca padrão | FTP, DNS, disco e escrita local |
| Download INMET/IBGE | urllib, ssl e pathlib | HTTPS, certificados, DNS, disco e escrita local |
| Extração CID-10 | zipfile e pathlib | ZIP local e espaço de saída |
| DQ SIH | dbfread e pyreaddbc, além da biblioteca padrão | Conversor do pacote e espaço temporário |
| Outros DQs | Biblioteca padrão | Leitura local, CPU para CRC e hash |

Não foi identificada necessidade de requests, PySUS, wget, curl ou Spark para a Bronze atual. Java pertence ao ambiente Spark posterior, não aos coletores Python simples.

Imports somente leitura no worker confirmaram Python 3.11.15, Airflow 3.3.1, provider standard 1.17.0, dbfread 2.0.7, pyreaddbc 2.0.4, airflow.sdk.task, PythonOperator e BashOperator. A importação de dbc2dbf confirmou disponibilidade do módulo, não a execução de uma conversão.

Os volumes já expõem src e data ao worker. Os wrappers precisarão garantir que a raiz do projeto esteja no caminho de imports. Não foram comprovadas conectividade externa ou disponibilidade dos endpoints. Nenhuma instalação foi feita.

# Proposta de TaskGroups e operators

Recomenda-se manter os cinco TaskGroups existentes e acrescentar aquisição e DQ Bronze aos quatro grupos de fontes.

| Grupo | Aquisição/preparação proposta | Gate proposto | Operator |
|:--|:--|:--|:--|
| sih | bronze_sih: 36 competências fixas | dq_bronze_sih | @task ou PythonOperator |
| inmet | bronze_inmet: 2023–2025 | dq_bronze_inmet | @task ou PythonOperator |
| ibge | bronze_ibge: SP/2022 | dq_bronze_ibge | @task ou PythonOperator |
| cid10 | bronze_cid10: preparar ZIP provisionado | dq_bronze_cid10 | @task ou PythonOperator |

Essa alternativa resulta em cinco TaskGroups e 24 tasks. Os decorators podem usar airflow.sdk.task, cuja disponibilidade foi verificada no ambiente instalado. PythonOperator e BashOperator estão disponíveis em airflow.providers.standard.operators.python e airflow.providers.standard.operators.bash.

As funções de aquisição devem executar dentro da task, nunca durante o parse da DAG. Retornos para XCom devem ser pequenos e serializáveis, como strings de paths ou resumos.

BashOperator executando os DQs pela CLI é uma alternativa para preservar o exit code, mas não existe dependência de shell que o torne obrigatório. SparkSubmitOperator permanece para Silver e Gold; a Bronze atual não é um job Spark.

Até esclarecer a origem oficial do ZIP, bronze_cid10 representa preparação local, não aquisição externa totalmente automatizada.

# Grafo completo proposto

```text
Provisionamento externo CID10
  -> bronze_cid10 -> dq_bronze_cid10
  -> silver_cid10 -> dq_silver_cid10

HTTPS IBGE
  -> bronze_ibge -> dq_bronze_ibge
     -> silver_ibge -> dq_silver_ibge
     -> silver_sih (também aguarda dq_bronze_sih)

FTP SIH: 36 competências
  -> bronze_sih -> dq_bronze_sih
  -> silver_sih (também aguarda dq_bronze_ibge)
  -> dq_silver_sih

HTTPS INMET: 2023, 2024, 2025
  -> bronze_inmet -> dq_bronze_inmet
  -> silver_inmet -> dq_silver_inmet
  -> silver_inmet_diario -> dq_silver_inmet_diario

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

As listas entre colchetes exigem sucesso de todos os elementos. Todas as convergências devem utilizar all_success. As quatro aquisições/preparações podem executar em paralelo; DQ IBGE libera duas Silver. A concorrência deve respeitar CPU, memória, disco e rede do ambiente local.

# Estratégia de retries proposta

| Processo | Retries | Delay inicial | Observação |
|:--|--:|:--|:--|
| Aquisição SIH | 3 | 2 minutos | Backoff exponencial; teto sugerido de 10 minutos |
| Aquisição INMET | 2 | 2 minutos | Limitar concorrência de arquivos maiores |
| Aquisição IBGE | 2 | 1 minuto | Referência pequena e estável |
| Extração CID-10 | 0 | Não aplicável | Repetição não corrige ZIP ausente/corrompido |
| DQs Bronze | 0 | Não aplicável | Reprovação exige diagnóstico |
| Silver/Gold existentes | Preservar | Preservar | Transformações 1; DQs 0 |

Os números são propostas, não resultados medidos. Rede transitória, como timeout ou reset, pode justificar retry. Dados inválidos, argumentos incorretos, imports ausentes e falhas de código não devem ser repetidos indiscriminadamente.

Parte dos erros de aquisição é encapsulada atualmente em RuntimeError. O wrapper e o tratamento futuro precisam preservar a causa para distinguir falhas transitórias de erros permanentes.

# Incrementalidade e as 36 competências SIH

Recomenda-se garantir a presença e integridade do acervo fixo 2023–2025, baixando ausentes, em vez de refazer todos os downloads. Apenas exists() não distingue um arquivo válido de um inválido.

| Unidade SIH | Vantagem | Custo |
|:--|:--|:--|
| Uma task para 36 meses | Reutiliza download_sih_periodo; menor mudança | Retry agregado; primeira falha interrompe o restante |
| Uma task por competência | Isolamento e retomada precisos | Mais tasks e controle de concorrência |
| Uma task por ano | Três unidades simples | Ainda agrega 12 meses por retry |

Para a primeira integração, recomenda-se uma task para o período completo, aproveitando a função existente e o acervo já presente. Os parâmetros devem fixar janeiro/2023 a dezembro/2025. O DQ SIH possui defaults de fim baseados na data atual; não devem ser usados para o recorte do TCC.

Se a coleta se tornar recorrente, a competência é a unidade natural futura, com poucas aquisições simultâneas. Não se recomenda disparar 36 downloads concorrentes. Dynamic task mapping não foi implementado.

Arquivo válido já disponível deve resultar em sucesso. Marcar esse caso como SKIPPED pode bloquear etapas seguintes sob all_success. Evitar reprocessamento Silver/Gold por ausência de mudanças exigiria uma política adicional de dependências e invalidação, não apenas pular o download.

# Problemas e ambiguidades encontrados

1. CID-10 sem aquisição externa implementada ou origem/versionamento comprovados.
2. SIH, INMET e IBGE sem CLI que execute suas funções de download.
3. Retorno de relatório ou inteiro não falha automaticamente uma task Python.
4. Dependência Bronze IBGE para Silver SIH precisa constar na DAG.
5. DQ IBGE valida conjunto shapefile, mas não o nome exato do DBF exigido por SIH.
6. DQ SIH possui defaults temporais móveis, inadequados ao recorte fixo sem argumentos explícitos.
7. Arquivos existentes inválidos são ignorados pelos coletores.
8. Extração CID-10 não publica o conjunto de CSVs atomicamente.
9. Temporários são pesquisados em toda a raiz da fonte, inclusive fora do recorte solicitado.
10. Ausência de versão ou hash persistido para detectar revisão silenciosa das fontes.
11. Diferença já documentada na Gold: leitura recursiva de silver/ibge versus gates sobre silver/ibge/2022.

Esses pontos foram registrados sem correções especulativas. O comportamento dos DQs foi determinado por leitura do código, não por aprovação dos datasets durante a auditoria.

# Segurança dos dados e implementação futura

Não foram apagados, movidos ou sobrescritos dados. Não houve mudanças de permissões, Dockerfiles, requirements, Docker Compose, connection, commit ou push. Os imports foram feitos com python -B para evitar geração de bytecode.

Antes de implementar recuperação automática de arquivos corrompidos, deve-se definir uma política explícita de substituição e preservação da fonte. Downloads para o mesmo destino não devem competir pelo mesmo temporário. Extração CID-10 deve considerar publicação segura do conjunto, sem transformar a auditoria em autorização para sobrescrever dados existentes.

# Impacto provável no tempo de execução

Os 45 minutos são o baseline informado para as 16 tasks atuais. Com o acervo existente, os downloads devem ser evitados, mas os DQs ainda acrescentam custo:

- SIH: 36 conversões DBC para DBF e cerca de 665 MB de leitura para hashes.
- INMET: CRC de cerca de 1,20 GB descompactados, amostras e hashes dos ZIPs.
- IBGE e CID-10: tendência de custo menor pelo volume, ainda sem medição.

Uma aquisição inicial das fontes exigiria aproximadamente 977 MB de transferência pelos tamanhos atuais, excluindo os CSVs CID-10 extraídos. Rede, disco e sobreposição dos ramos determinam o acréscimo real. Não é possível estimar minutos com segurança por análise estática.

## Cinco oportunidades simples para investigar

| Prioridade | Oportunidade | Avaliação pragmática |
|:--|:--|:--|
| 1 | Evitar downloads repetidos de fontes válidas | Comparar acervo presente com aquisição de um arquivo ausente |
| 2 | Reduzir varreduras repetidas nos DQs | Medir cada gate e agrupar métricas compatíveis preservando regras |
| 3 | Revisar recomputações Gold | Investigar actions repetidas e cache pontual com controle de memória |
| 4 | Reutilizar preparação auxiliar IBGE no lote SIH | Medir preparação uma vez por lote, preservando resultados |
| 5 | Ajustar concorrência dos ramos independentes | Comparar tempo total e pressão de recursos com limites simples |

Há evidência estática de várias actions em src/common/validators.py e nas duas fatos Gold. A revisão não recomenda remover verificações críticas para ganhar tempo.

Broadcast, AQE e reparticionamento não são a primeira prioridade. A fato já possui broadcast automático desativado por motivo de memória; a fato meteorológica usa repartition e partitionBy por ano/mês. Alterações nesses pontos exigem medição e cuidado adicional.

# Próxima etapa recomendada

Primeiro, preparar wrappers de aquisição e DQ, recorte temporal fixo, timeouts, classificação de falhas e política para arquivos inválidos. Manter CID-10 como insumo provisionado até esclarecer a origem.

Depois, adicionar oito tasks Bronze/DQ à DAG e a dependência DQ Bronze IBGE para Silver SIH, preservando as 16 tasks existentes. Validar inicialmente com o acervo atual, sem forçar downloads ou sobrescritas, e medir separadamente os quatro gates Bronze.

# Evidências e limitações da auditoria

Foram consultados src/bronze, src/quality/bronze, consumidores Silver, código Gold e validadores comuns, DAG, configurações Docker, requirements, README, notebook de validação e a estrutura data/bronze. Foram usados rg, leitura de arquivos, listagens, metadados de ZIP e imports leves no worker.

Não foram feitos downloads, testes de conectividade, descompactação integral, conversões, hashes integrais dos datasets, execução dos DQs ou pipeline. Não foi feita pesquisa externa de endpoints. O estado Git consultado ao final da auditoria estava sem alterações.

Este documento registra o estado observado na auditoria; não é evidência de aprovação integral dos dados nem de integração Bronze já implementada.
