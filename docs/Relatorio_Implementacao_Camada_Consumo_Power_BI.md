---
title: "Implementação da camada de consumo para Power BI"
subtitle: "TCC — integração de saúde e meteorologia"
date: "14 de setembro de 2026"
lang: pt-BR
geometry: margin=1.8cm
fontsize: 10pt
fontfamily: fontspec
mainfont: Liberation Serif
sansfont: Liberation Sans
monofont: Liberation Mono
monofontoptions:
  - Scale=0.78
toc-title: "Sumário"
colorlinks: true
header-includes:
  - \usepackage{longtable}
  - \usepackage{booktabs}
  - \usepackage{array}
  - \usepackage{fvextra}
  - \DefineVerbatimEnvironment{Highlighting}{Verbatim}{breaklines,commandchars=\\\{\}}
  - \setlength{\emergencystretch}{3em}
---

# Resumo executivo

Foi implementada e materializada a camada `data/consumption/` para consumo no Power BI. A solução contém duas tabelas fato e quatro dimensões, considera exclusivamente internações com `data_internacao` entre 01/01/2023 e 31/12/2025 e preserva as camadas Bronze, Silver e Gold existentes.

As duas fatos reconciliam exatamente **8.236.990 internações**, **422.557 óbitos**, **39.845.279 dias de permanência** e **488.421 dias de UTI**. Os valores financeiros também coincidem integralmente entre as duas fatos e a Gold de origem.

A implementação acrescenta agregações diárias de radiação, pressão atmosférica, temperatura de orvalho e rajada de vento diretamente da Silver INMET horária. Também oferece temperatura média com defasagens exatas de 1, 3 e 7 dias.

O Data Quality terminou com status **PASS**. Os oito testes sintéticos foram aprovados. A DAG foi importada sem erros e recebeu o novo TaskGroup `consumption`.

Nenhum commit foi realizado.

# Escopo implementado

## Datasets criados

| Dataset | Grão |
|:--|:--|
| `fato_municipio_dia` | Data de internação + município |
| `fato_municipio_dia_cid` | Data + município + categoria CID + sexo + faixa etária |
| `dim_municipio` | Município |
| `dim_cid` | Categoria CID |
| `dim_tempo` | Dia de calendário |
| `dim_estacao` | Estação meteorológica |

## Código criado

- `src/consumption/transformations.py`: transformações reutilizáveis e testáveis.
- `src/consumption/build.py`: leitura das fontes, construção e gravação dos seis datasets.
- `src/quality/consumption/dq_consumption.py`: regras de qualidade e reconciliação.
- `tests/test_consumption.py`: testes unitários com DataFrames sintéticos.
- Arquivos `__init__.py` para os novos pacotes.

## Código alterado

- `dags/tcc_pipeline.py`: inclusão da configuração, caminhos, TaskGroup e dependências da camada de consumo.

Não foram alteradas regras Bronze, Silver ou Gold. Não foram removidos datasets existentes, criadas conexões, alteradas portas ou modificadas configurações do Docker Compose como parte desta implementação.

# Arquitetura final

O fluxo acrescentado ao pipeline é:

```text
Gold fato_internacao_meteorologia ──────┐
Silver SIH ─────────────────────────────┤
Silver INMET horário ──────────────────┼─> build_consumption
Silver INMET diário ────────────────────┤          │
Gold municipio_estacao + Silver IBGE ──┘          v
                                              seis Parquets
                                                   │
                                                   v
                                            dq_consumption
```

As fontes Silver necessárias somente alimentam a transformação depois de seus respectivos gates de qualidade. A transformação também aguarda a aprovação de `gold.dq_gold_fato_internacao_meteorologia`.

# Recorte temporal

O filtro analítico utiliza somente:

```text
data_internacao >= 2023-01-01
data_internacao <= 2025-12-31
```

As colunas `ano` e `mes` de competência SIH não são usadas para selecionar as internações. A coluna física `ano` das novas fatos é novamente derivada da data analítica e serve exclusivamente ao particionamento.

# Resultados materializados

| Dataset | Linhas | Arquivos Parquet | Tamanho aproximado | Partições físicas |
|:--|--:|--:|--:|:--|
| `fato_municipio_dia` | 578.832 | 3 | 26,98 MB | `ano` |
| `fato_municipio_dia_cid` | 6.597.066 | 3 | 113,40 MB | `ano` |
| `dim_municipio` | 645 | 1 | 35,65 KB | Nenhuma |
| `dim_cid` | 1.658 | 1 | 69,42 KB | Nenhuma |
| `dim_tempo` | 1.096 | 1 | 13,05 KB | Nenhuma |
| `dim_estacao` | 40 | 1 | 3,63 KB | Nenhuma |

## Particionamento

As fatos são particionadas por ano da data analítica, resultando em apenas três partições: 2023, 2024 e 2025. Essa escolha permite leitura por período no Power BI e evita uma árvore mensal com muitos arquivos pequenos.

As dimensões são pequenas e foram gravadas em um arquivo cada, sem particionamento físico.

# Schema de fato_municipio_dia

| Campo | Tipo |
|:--|:--|
| `municipio_dia_id` | string |
| `data` | date |
| `codigo_municipio` | string |
| `codigo_estacao` | string |
| `qtd_internacoes` | bigint |
| `qtd_obitos` | bigint |
| `dias_permanencia_total` | bigint |
| `dias_uti_total` | bigint |
| `valor_total_internacoes` | decimal(20,2) |
| `valor_total_uti` | decimal(20,2) |
| `temperatura_media_dia_c` | double |
| `temperatura_minima_dia_c` | double |
| `temperatura_maxima_dia_c` | double |
| `umidade_media_dia_pct` | double |
| `umidade_minima_dia_pct` | double |
| `umidade_maxima_dia_pct` | double |
| `precipitacao_dia_mm` | double |
| `vento_medio_dia_ms` | double |
| `vento_maximo_dia_ms` | double |
| `qtd_observacoes_inmet` | bigint |
| `qtd_obs_temperatura` | bigint |
| `qtd_obs_umidade` | bigint |
| `qtd_obs_precipitacao` | bigint |
| `qtd_obs_vento` | bigint |
| `meteorologia_disponivel` | int |
| `motivo_sem_meteorologia` | string |
| `radiacao_global_dia_kj_m2` | double |
| `qtd_obs_radiacao` | bigint |
| `pressao_media_dia_mb` | double |
| `pressao_minima_dia_mb` | double |
| `pressao_maxima_dia_mb` | double |
| `qtd_obs_pressao` | bigint |
| `orvalho_medio_dia_c` | double |
| `orvalho_minimo_dia_c` | double |
| `orvalho_maximo_dia_c` | double |
| `qtd_obs_orvalho` | bigint |
| `rajada_maxima_dia_ms` | double |
| `qtd_obs_rajada` | bigint |
| `temperatura_media_lag_1d` | double |
| `temperatura_media_lag_3d` | double |
| `temperatura_media_lag_7d` | double |
| `ano` | int — partição física |

A chave `municipio_dia_id` segue o formato `AAAAMMDD_codigo_municipio`, por exemplo `20240115_3509502`.

As taxas e médias não são armazenadas. A tabela preserva numeradores e denominadores para que o Power BI calcule mortalidade, permanência média e valor médio no contexto de filtro correto.

# Schema de fato_municipio_dia_cid

| Campo | Tipo |
|:--|:--|
| `municipio_dia_id` | string |
| `data` | date |
| `codigo_municipio` | string |
| `codigo_categoria` | string |
| `sexo` | string |
| `faixa_etaria` | string |
| `qtd_internacoes` | bigint |
| `qtd_obitos` | bigint |
| `dias_permanencia_total` | bigint |
| `dias_uti_total` | bigint |
| `valor_total_internacoes` | decimal(20,2) |
| `valor_total_uti` | decimal(20,2) |
| `ano` | int — partição física |

Nenhum campo meteorológico foi duplicado nessa tabela. A relação com a meteorologia ocorre por `municipio_dia_id` ou pelas dimensões de data e município, conforme o modelo Power BI adotado.

## Tratamento da idade

A faixa etária foi construída a partir de `COD_IDADE` e `IDADE` da Silver SIH:

| `COD_IDADE` | Interpretação |
|:--|:--|
| 2 | Idade em dias; faixa 0–17 |
| 3 | Idade em meses; faixa 0–17 |
| 4 | Idade em anos |
| 5 | `100 + IDADE`; faixa 80+ |
| 0 ou código inválido | Ignorada |

As categorias finais são `0-17`, `18-39`, `40-59`, `60-79`, `80+` e `Ignorada`. A categoria Ignorada preserva o registro e mantém a reconciliação dos totais.

## Hierarquia CID

A Silver SIH fornece o diagnóstico principal. A categoria é obtida por meio do mapeamento `codigo_cid → codigo_categoria` efetivamente resolvido pela Gold, incluindo suas regras de compatibilidade e fallback. A reconstrução não apresentou diagnóstico sem categoria no recorte.

# Schemas das dimensões

## dim_municipio

| Campo | Tipo |
|:--|:--|
| `codigo_municipio` | string |
| `codigo_municipio_sih` | string |
| `nome_municipio` | string |
| `sigla_uf` | string |
| `area_km2` | double |
| `latitude_municipio` | double |
| `longitude_municipio` | double |
| `codigo_estacao_referencia` | string |
| `distancia_estacao_km` | double |
| `ano_referencia_ibge` | int |

## dim_cid

| Campo | Tipo |
|:--|:--|
| `codigo_categoria` | string |
| `descricao_categoria` | string |
| `cat_inicial_grupo` | string |
| `cat_final_grupo` | string |
| `codigo_grupo` | string |
| `descricao_grupo` | string |
| `codigo_capitulo` | string |
| `descricao_capitulo` | string |

`codigo_grupo` é formado pelos limites inferior e superior do grupo, como `J00-J06`. A dimensão contém 1.658 categorias únicas e reflete a hierarquia resolvida da Gold.

## dim_tempo

| Campo | Tipo |
|:--|:--|
| `data` | date |
| `data_id` | int |
| `ano` | int |
| `mes` | int |
| `nome_mes` | string |
| `ano_mes` | string |
| `trimestre` | int |
| `dia_mes` | int |
| `dia_semana_numero` | int |
| `dia_semana_nome` | string |
| `fim_de_semana` | boolean |

O calendário é contínuo de 01/01/2023 a 31/12/2025, com 1.096 dias. A dimensão não converte automaticamente dias ausentes da fato em zero internações.

## dim_estacao

| Campo | Tipo |
|:--|:--|
| `codigo_estacao` | string |
| `nome_estacao` | string |
| `latitude_estacao` | double |
| `longitude_estacao` | double |
| `uf` | string |
| `regiao` | string |
| `altitude_m` | double |

A consistência histórica foi verificada antes da deduplicação. Dez estações apresentam revisões de nome, coordenadas ou altitude entre 2023 e 2025. UF e região são estáveis. Como o schema solicitado não prevê vigência temporal, a dimensão usa deterministicamente o cadastro mais recente, de 2025, e o DQ registra a situação como aviso.

# Meteorologia adicional

## Regras de agregação

| Campo horário da Silver | Indicador diário | Agregação |
|:--|:--|:--|
| `radiacao_global_kj_m2` | `radiacao_global_dia_kj_m2` | Soma |
| `pressao_estacao_mb` | pressão média, mínima e máxima | Média, mínimo e máximo |
| `temperatura_orvalho_c` | orvalho médio, mínimo e máximo | Média, mínimo e máximo |
| `vento_rajada_max_ms` | `rajada_maxima_dia_ms` | Máximo |

`radiacao_global_kj_m2` representa energia global por área acumulada no intervalo horário. Portanto, a soma dos intervalos válidos é semanticamente adequada para a energia diária. O contador `qtd_obs_radiacao` deve sempre acompanhar a análise para revelar dias parciais.

Não foi calculada média aritmética de `vento_direcao_graus`.

## Cobertura observada

Denominador: 578.832 combinações município-dia existentes.

| Indicador | Linhas preenchidas | Cobertura |
|:--|--:|--:|
| Radiação global diária | 507.681 | 87,71% |
| Pressão média/mínima/máxima | 530.298 | 91,62% |
| Orvalho médio/mínimo/máximo | 520.477 | 89,92% |
| Rajada máxima | 480.934 | 83,09% |

Os contadores das quatro famílias adicionais estão presentes em todas as 578.832 linhas. Um contador zero indica ausência de observação válida, sem transformar o valor meteorológico nulo em erro de qualidade.

# Lags de temperatura

Foram criados:

- `temperatura_media_lag_1d`;
- `temperatura_media_lag_3d`;
- `temperatura_media_lag_7d`.

Cada valor é obtido pela igualdade entre estação e data civil exata:

```text
lag_1d = temperatura em data - 1 dia
lag_3d = temperatura em data - 3 dias
lag_7d = temperatura em data - 7 dias
```

Não foi utilizada a linha anterior de uma janela. A busca consulta a Silver INMET diária, mesmo quando não existe internação na data do lag. Quando a data ou a medição não existe, o resultado permanece nulo. Nenhum valor de dezembro de 2022 foi inventado.

# Reconciliação das métricas de saúde

| Métrica | Gold 2023–2025 | Fato município-dia | Fato município-dia-CID |
|:--|--:|--:|--:|
| Internações | 8.236.990 | 8.236.990 | 8.236.990 |
| Óbitos | 422.557 | 422.557 | 422.557 |
| Dias de permanência | 39.845.279 | 39.845.279 | 39.845.279 |
| Dias de UTI | 488.421 | 488.421 | 488.421 |
| Valor total | R$ 14.389.294.434,48 | R$ 14.389.294.434,48 | R$ 14.389.294.434,48 |
| Valor de UTI | R$ 3.821.109.214,10 | R$ 3.821.109.214,10 | R$ 3.821.109.214,10 |

Os valores monetários foram convertidos para `decimal(20,2)` antes da soma, evitando diferenças de representação binária próprias de `double`.

# Data Quality

O script `src/quality/consumption/dq_consumption.py` valida:

1. schema mínimo das duas fatos;
2. unicidade de `municipio_dia_id` na fato diária;
3. unicidade da chave completa na fato CID;
4. ausência de chaves obrigatórias nulas ou vazias;
5. `qtd_internacoes > 0`;
6. óbitos entre zero e internações;
7. reconciliação das seis métricas de saúde com a Gold;
8. igualdade dos totais entre as duas fatos;
9. domínio das faixas etárias;
10. integridade referencial com as quatro dimensões;
11. unicidade das chaves dimensionais;
12. calendário contínuo e período correto;
13. ausência de meteorologia na fato CID;
14. unicidade meteorológica por data e município;
15. limites dos contadores horários;
16. preservação dos contadores vindos da Gold;
17. correspondência exata dos lags com suas datas;
18. cobertura informativa das variáveis adicionais;
19. consistência histórica do cadastro de estações.

## Resultado

```text
STATUS FINAL: PASS
```

Resultados relevantes:

- zero duplicidades nas duas fatos;
- zero chaves obrigatórias nulas;
- zero chaves ausentes nas dimensões;
- zero divergências nos totais de saúde;
- zero contadores fora do intervalo 0–24;
- zero divergências em cada um dos três lags;
- um aviso não bloqueante sobre as dez estações com metadados revisados.

# Testes automatizados

Foram criados oito testes com pequenos DataFrames sintéticos:

- conversão de `COD_IDADE + IDADE` em faixa etária;
- criação da chave `municipio_dia_id`;
- agregação dos numeradores de saúde;
- agregações meteorológicas adicionais;
- lags por data exata em série esparsa;
- hierarquia CID resolvida;
- calendário contínuo;
- fluxo integrado das principais regras de DQ.

Resultado:

```text
Ran 8 tests in 18.818s
OK
```

A suíte foi executada com Spark 3.5.0. Os avisos de sockets emitidos pelo runtime Python após ações Spark não causaram falhas nos testes.

# Integração com a DAG

Foi adicionado o TaskGroup `consumption`, com duas tasks `SparkSubmitOperator`:

| Task | Função |
|:--|:--|
| `consumption.build_consumption` | Materializa as duas fatos e quatro dimensões |
| `consumption.dq_consumption` | Valida os datasets materializados |

Dependências confirmadas pela DAG carregada:

```text
gold.dq_gold_fato_internacao_meteorologia ─┐
inmet.dq_silver_inmet                     ├──> consumption.build_consumption
inmet.dq_silver_inmet_diario              ┤
sih.dq_silver_sih                         ┘

consumption.build_consumption
    -> consumption.dq_consumption
```

A conexão permanece `spark_default`; nenhuma conexão foi criada ou alterada. As novas tasks utilizam 40 partições de shuffle, como a Gold meteorológica.

Durante a materialização real, o heap padrão de 1 GiB foi insuficiente para as agregações das duas fatos. As tasks de consumo receberam `spark.driver.memory = 4g`. Essa configuração é exclusiva do novo TaskGroup e não altera `spark_default` nem as tasks existentes.

A DAG passou na validação de importação:

```text
airflow dags list-import-errors --output json
[]
```

# Decisões técnicas

## Separação das fatos

A meteorologia permanece em `fato_municipio_dia`, onde é única por data e município. A fato CID contém somente métricas de saúde. Isso evita replicar indicadores meteorológicos a cada categoria, sexo e faixa etária.

## Dias sem internação

A dimensão de tempo contém todos os dias, mas as fatos contêm apenas combinações observadas. A implementação não presume que ausência de linha significa zero internações.

## Valores financeiros

Os campos representam valores registrados no SIH. O schema preserva `valor_total_internacoes` e `valor_total_uti` separadamente; a camada não soma os dois nem os interpreta como custo econômico completo.

## Processamento da Gold ampla

A saúde é agregada separadamente das colunas meteorológicas. A combinação meteorológica única é reduzida ao grão municipal diário e depois unida aos totais de saúde. A equivalência foi garantida pelo DQ.

# Estado do Git

Nenhum commit foi realizado. O `git diff --stat` dos arquivos rastreados no momento da entrega era:

```text
dags/tcc_pipeline.py                     | 57 ++++++++++++++++++++++++++++++++
docker-compose.airflow.yaml              |  2 +-
src/gold/fato_internacao_meteorologia.py | 43 ++++++++++++++++++++++++
3 files changed, 101 insertions(+), 1 deletion(-)
```

O `git diff --stat` não inclui arquivos ainda não rastreados. As modificações de `docker-compose.airflow.yaml` e `src/gold/fato_internacao_meteorologia.py` já existiam antes desta implementação e não foram alteradas durante o trabalho da camada de consumo.

Estado completo observado:

```text
 M dags/tcc_pipeline.py
 M docker-compose.airflow.yaml
 M src/gold/fato_internacao_meteorologia.py
?? docs/Relatorio_Auditoria_Camada_Consumo_Power_BI.md
?? docs/Relatorio_Auditoria_Camada_Consumo_Power_BI.pdf
?? docs/Relatorio_Experimento_2_Persistencia_INMET.md
?? docs/Relatorio_Experimento_2_Persistencia_INMET.pdf
?? docs/Relatorio_Resultados_Performance_Experimentos_1_2.md
?? docs/Relatorio_Resultados_Performance_Experimentos_1_2.pdf
?? src/consumption/
?? src/quality/consumption/
?? tests/test_consumption.py
?? tests/test_fato_meteorologia_persistencia.py
```

# Conclusão

A camada de consumo está implementada, materializada e validada. O modelo oferece o grão diário municipal para meteorologia e totais de saúde, além do detalhamento por categoria CID, sexo e faixa etária para as análises do Power BI.

Os totais são reconciliados com a Gold, a idade respeita a unidade SIH, os lags utilizam datas civis exatas e as lacunas meteorológicas permanecem explícitas por meio dos indicadores nulos e contadores de observações.

**Status final: implementação concluída; DQ aprovado; testes aprovados; nenhuma alteração nas regras Bronze, Silver ou Gold; nenhum commit criado.**
