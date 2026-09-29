---
title: "Auditoria da camada de consumo para Power BI"
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

A Gold `fato_internacao_meteorologia` contém **8.409.047 registros**, confirmados por metadados dos Parquets e leitura das linhas. A chave **data de internação + município** determina uma única combinação de estação, indicadores e controles meteorológicos nos dados atuais.

A arquitetura recomendada para a futura pasta `data/consumption/` reúne:

- uma fato municipal diária, com totais de saúde e meteorologia;
- uma fato de saúde por dia, município, categoria CID, sexo e faixa etária;
- dimensões de município, categoria CID, tempo e estação.

**Condição importante:** a coluna `idade` da Gold não está normalizada em anos. A simulação correta de faixas etárias precisou consultar `COD_IDADE` na Silver SIH. A futura implementação deve resolver essa informação sem presumir que o número da Gold representa anos.

Em 2023–2025, todas as **8.236.990 internações registradas no recorte** encontram uma linha INMET. Entretanto, apenas **95,04%** possuem temperatura preenchida e **84,94%** possuem precipitação preenchida. Correspondência no join não equivale a completude meteorológica.

## Escopo e método

A auditoria original foi somente leitura. Foram inspecionados os scripts geradores, os schemas reais, as partições físicas e os dados dos Parquets. Contagens e agregações foram executadas em memória com PyArrow/Pandas no container existente. Não houve execução do pipeline, otimização ou benchmark.

Este documento organiza os resultados já obtidos; não representa nova execução da auditoria. O Markdown e o PDF foram criados posteriormente, por solicitação do usuário. Código, DAG, configurações e datasets permanecem intactos. Nenhuma estrutura de consumo foi implementada.

As contagens de “internações” neste relatório representam **linhas SIH preservadas pela Gold**. Não constituem uma validação de pacientes únicos ou de episódios hospitalares deduplicados.

## Fontes internas inspecionadas

- `src/gold/fato_internacao_meteorologia.py`
- `src/gold/fato_internacao.py`
- `src/gold/municipio_estacao.py`
- `src/silver/inmet_diario.py`
- `src/silver/inmet.py`
- `src/silver/cid10.py`
- `src/silver/ibge.py`
- `src/silver/sih.py`
- Parquets das seis bases solicitadas; Silver INMET horária e Silver SIH consultadas para esclarecer variáveis adicionais e unidade etária.

\newpage

# A. Schema encontrado

## Inventário de datasets e partições

| Dataset relativo a `data/` | Linhas | Partições físicas |
|:--|--:|:--|
| `gold/fato_internacao_meteorologia` | 8.409.047 | `ano`, `mes` |
| `gold/fato_internacao` | 8.409.047 | Nenhuma |
| `gold/municipio_estacao` | 645 | Nenhuma |
| `silver/inmet_diario` | 43.840 | `data` |
| `silver/cid10` | 12.451 | Nenhuma |
| `silver/ibge/2022` | 645 | Nenhuma |

O diretório `ibge/2022` é uma pasta de referência, não uma partição Hive `ano=2022`. A Gold integrada tem 36 arquivos Parquet; a fato de internação tem oito. A Silver diária tem 2.192 arquivos.

**Atenção temporal:** `ano` e `mes` da Gold representam a competência SIH (`ANO_CMPT`, `MES_CMPT`), não necessariamente o ano e o mês da entrada hospitalar.

## Schema completo da Gold integrada

São **47 colunas**, incluindo as duas colunas de partição. Os tipos abaixo usam a nomenclatura equivalente do Spark: `bigint` corresponde a inteiro de 64 bits; `int`, a inteiro de 32 bits.

### Tempo, município e estação

| Coluna exata | Tipo | Origem |
|:--|:--|:--|
| `data_internacao` | date | SIH: `DT_INTER` |
| `data_saida` | date | SIH: `DT_SAIDA` |
| `CD_MUN` | string | IBGE, associado ao município de residência SIH |
| `NM_MUN` | string | IBGE |
| `SIGLA_UF` | string | IBGE |
| `AREA_KM2` | double | IBGE |
| `latitude_municipio` | double | Derivada da geometria IBGE |
| `longitude_municipio` | double | Derivada da geometria IBGE |
| `codigo_estacao` | string | INMET, via `municipio_estacao` |
| `estacao` | string | INMET |
| `latitude_estacao` | double | INMET |
| `longitude_estacao` | double | INMET |
| `distancia_km` | double | Cálculo entre coordenadas IBGE e INMET |
| `ano` | int | SIH: `ANO_CMPT`; partição física |
| `mes` | int | SIH: `MES_CMPT`; partição física |

### CID e saúde

| Coluna exata | Tipo | Origem |
|:--|:--|:--|
| `codigo_cid` | string | SIH: `DIAG_PRINC` |
| `descricao_cid` | string | CID com compatibilidade/fallback na Gold |
| `codigo_categoria` | string | CID com compatibilidade/fallback |
| `descricao_categoria` | string | CID com compatibilidade/fallback |
| `cat_inicial_grupo` | string | Hierarquia CID |
| `cat_final_grupo` | string | Hierarquia CID |
| `descricao_grupo` | string | Hierarquia CID |
| `codigo_capitulo` | string | Hierarquia CID |
| `descricao_capitulo` | string | Hierarquia CID |
| `sexo` | string | SIH: `SEXO` |
| `idade` | bigint | SIH: `IDADE`, sem conversão da unidade |
| `dias_permanencia` | bigint | SIH: `DIAS_PERM` |
| `obito` | bigint | SIH: `MORTE` |
| `dias_uti` | bigint | SIH: `UTI_INT_TO` |
| `valor_total` | double | SIH: `VAL_TOT` |
| `valor_uti` | double | SIH: `VAL_UTI` |

### Meteorologia e controles

| Coluna exata | Tipo | Origem |
|:--|:--|:--|
| `qtd_observacoes_inmet` | bigint | INMET diário: `qtd_observacoes` |
| `qtd_obs_precipitacao` | bigint | Contagem horária válida |
| `qtd_obs_temperatura` | bigint | Contagem horária válida |
| `qtd_obs_umidade` | bigint | Contagem horária válida |
| `qtd_obs_vento` | bigint | Contagem horária válida |
| `precipitacao_dia_mm` | double | Agregação INMET |
| `temperatura_media_dia_c` | double | Agregação INMET |
| `temperatura_minima_dia_c` | double | Agregação INMET |
| `temperatura_maxima_dia_c` | double | Agregação INMET |
| `umidade_media_dia_pct` | double | Agregação INMET |
| `umidade_minima_dia_pct` | double | Agregação INMET |
| `umidade_maxima_dia_pct` | double | Agregação INMET |
| `vento_medio_dia_ms` | double | Agregação INMET |
| `vento_maximo_dia_ms` | double | Agregação INMET |
| `meteorologia_disponivel` | int | Flag calculada pelo join |
| `motivo_sem_meteorologia` | string | Classificação calculada na integração |

## Particularidades dos schemas auxiliares

- `fato_internacao`: 31 colunas anteriores ao enriquecimento meteorológico. `mes` é `string`, enquanto na Gold integrada é descoberto como `int` na partição.
- `municipio_estacao`: 11 colunas — seis atributos municipais, quatro atributos de estação e `distancia_km`.
- `inmet_diario`: 19 colunas — código/nome da estação, duas coordenadas, cinco contadores, nove indicadores e `data`. A partição `data` foi descoberta como `string` pelo PyArrow; recomenda-se conversão explícita para `date` no consumo.
- `cid10`: 15 colunas, incluindo as descrições abreviadas de CID, categoria, grupo e capítulo, além de `restricao_sexo` e `causa_obito`. Esses atributos adicionais não estão na Gold final.
- `ibge/2022`: `CD_MUN`, `CD_MUN_6`, `NM_MUN`, `SIGLA_UF` (string), `AREA_KM2`, `latitude`, `longitude` (double). São 645 códigos distintos de sete dígitos e também 645 de seis dígitos, todos de SP.

# B. Variáveis meteorológicas disponíveis

## Indicadores da Silver diária e da Gold final

As duas camadas contêm os mesmos **nove indicadores meteorológicos**. Todos possuem valores efetivamente preenchidos nos dados auditados, embora com cobertura parcial.

| Indicador / campo | Cálculo no código |
|:--|:--|
| `temperatura_media_dia_c` | Média de `temperatura_c` |
| `temperatura_minima_dia_c` | Mínimo de `temperatura_c` |
| `temperatura_maxima_dia_c` | Máximo de `temperatura_c` |
| `umidade_media_dia_pct` | Média de `umidade_pct` |
| `umidade_minima_dia_pct` | Mínimo de `umidade_pct` |
| `umidade_maxima_dia_pct` | Máximo de `umidade_pct` |
| `precipitacao_dia_mm` | Soma de `precipitacao_mm` |
| `vento_medio_dia_ms` | Média de `vento_velocidade_ms` |
| `vento_maximo_dia_ms` | Máximo de `vento_velocidade_ms` |

As temperaturas mínima e máxima são extremos das leituras de `temperatura_c`, não agregações dos campos horários `temperatura_min_c` e `temperatura_max_c`. O vento máximo diário não representa rajada máxima. A data diária deriva de `data_hora_utc`.

## Radiação, pressão e outros campos horários

**Radiação solar existe nos dados carregados, mas somente na Silver horária. Pressão atmosférica também existe somente nessa camada entre as bases meteorológicas auditadas.** Nenhuma das duas foi incorporada à Silver diária ou à Gold final.

| Campo existente na Silver horária | Situação na Silver diária / Gold |
|:--|:--|
| `radiacao_global_kj_m2` | Ausente |
| `pressao_estacao_mb` | Ausente |
| `pressao_max_mb` | Ausente |
| `pressao_min_mb` | Ausente |
| `temperatura_orvalho_c` | Ausente |
| `temperatura_max_c` | Não preservado como tal |
| `temperatura_min_c` | Não preservado como tal |
| `orvalho_max_c` | Ausente |
| `orvalho_min_c` | Ausente |
| `umidade_max_pct` | Não preservado como tal |
| `umidade_min_pct` | Não preservado como tal |
| `vento_direcao_graus` | Ausente |
| `vento_rajada_max_ms` | Ausente |

Os quatro campos horários usados nas agregações são `precipitacao_mm`, `temperatura_c`, `umidade_pct` e `vento_velocidade_ms`. Todos os 13 campos adicionais listados acima têm valores não nulos em cada um dos três anos carregados. Exemplos:

| Ano | Linhas horárias | Radiação não nula | Pressão na estação não nula |
|:--|--:|--:|--:|
| 2023 | 350.400 | 183.819 | 332.236 |
| 2024 | 351.360 | 172.947 | 307.455 |
| 2025 | 350.400 | 167.467 | 304.207 |

A Silver horária tem raízes anuais e partição física por `codigo_estacao`. Uma derivação futura pode agregar seus campos adicionais sem modificar a Gold atual, após definir unidades, agregações e critérios de qualidade.

## Cobertura de 2023–2025

A Silver diária contém **40 estações × 1.096 dias = 43.840 linhas**, de 01/01/2023 a 31/12/2025. Não há duplicidade em `codigo_estacao + data`, e todas as linhas têm `qtd_observacoes = 24`.

**Vinte e quatro registros horários não significam 24 medições válidas por variável.** Os contadores `qtd_obs_*` devem acompanhar as análises.

### Estação-dias sem valor

| Ano | Estação-dias | Temperatura | Umidade | Precipitação | Vento |
|:--|--:|--:|--:|--:|--:|
| 2023 | 14.600 | 575 | 922 | 1.684 | 2.153 |
| 2024 | 14.640 | 1.415 | 1.852 | 2.517 | 2.865 |
| 2025 | 14.600 | 1.415 | 1.425 | 3.070 | 2.652 |

As contagens de ausência são iguais entre os três indicadores de temperatura, entre os três de umidade e entre os dois de vento.

### Correspondência INMET por ano de internação

| Ano da entrada | Registros SIH | Correspondência INMET |
|:--|--:|--:|
| 2023 | 2.627.931 | 100% |
| 2024 | 2.800.835 | 100% |
| 2025 | 2.808.224 | 100% |
| **Total** | **8.236.990** | **100%** |

### Cobertura das variáveis nas internações do recorte

| Família | Registros com valor | Cobertura | Com pelo menos 18 horas válidas |
|:--|--:|--:|--:|
| Temperatura | 7.828.255 | 95,04% | 91,65% |
| Umidade | 7.797.221 | 94,66% | 89,44% |
| Precipitação | 6.996.820 | 84,94% | 82,21% |
| Vento | 7.148.169 | 86,78% | 83,52% |

Os percentuais acima usam as 8.236.990 linhas SIH como denominador, portanto são ponderados pelas internações. Nas **578.832 combinações município-dia existentes** em 2023–2025, as ausências são: temperatura, **49.954**; umidade, **57.324**; precipitação, **107.113**; vento, **99.623**.

# C. Campos de saúde disponíveis

| Necessidade | Campo | Interpretação |
|:--|:--|:--|
| Entrada e saída | `data_internacao`, `data_saida` | Datas SIH |
| Município | `NM_MUN`, `CD_MUN` | Residência; código IBGE de sete dígitos |
| CID | `codigo_cid` | Diagnóstico principal |
| Categoria | `codigo_categoria` | Disponível, sem nulos |
| Grupo | `cat_inicial_grupo`, `cat_final_grupo`, `descricao_grupo` | Sem código único de grupo pronto |
| Capítulo | `codigo_capitulo`, `descricao_capitulo` | Disponível |
| Sexo | `sexo` | Valores observados: 1 e 3 |
| Idade | `idade` | Número bruto; unidade ausente na Gold |
| Óbito | `obito` | Valores observados: 0 e 1 |
| Permanência | `dias_permanencia` | Dias registrados |
| UTI | `dias_uti` | Origem: `UTI_INT_TO` |
| Valor | `valor_total`, `valor_uti` | Valores registrados no SIH |
| Estação | `codigo_estacao` | Estação associada ao município |

## Perfil observado na Gold completa

- Sexo 1: **3.746.600** registros; sexo 3: **4.662.447**.
- Óbito igual a 1: **430.332** registros; óbito igual a 0: **7.978.715**.
- Nenhum nulo em sexo, idade, óbito, dias de permanência, dias de UTI, valor total ou valor de UTI.
- Idade bruta: **0 a 99**; permanência: **0 a 361 dias**; UTI: **0 a 115 dias**.
- Valor total: **0 a 329.010,27**; valor de UTI: **0 a 150.150,00**.

Essas distribuições correspondem à Gold completa, incluindo as datas de internação anteriores a 2023.

## Unidade da idade: achado crítico

A Gold copia `IDADE` e não preserva `COD_IDADE`. A Silver contém:

| Código | Unidade | Registros | Intervalo bruto |
|:--|:--|--:|:--|
| 2 | Dias | 228.664 | 0–30 |
| 3 | Meses | 165.554 | 1–11 |
| 4 | Anos | 8.010.719 | 1–99 |
| 5 | Centena de anos | 4.109 | 0–28 |
| 0 | Ignorada | 1 | 0 |

A interpretação depende de `COD_IDADE`; para o código 5, a idade é `100 + IDADE`. As referências de apoio constam ao final deste documento.

Consequências de aplicar as faixas diretamente à Gold:

- **12.720 registros** de bebês com idade em dias seriam classificados em 18–39;
- **4.109 centenários** seriam classificados em faixas inferiores a 80+;
- uma idade ignorada seria tratada como zero. Esse registro tem data de internação de **13/12/2024**.

A Gold isolada não permite recuperar a unidade com segurança por registro. A camada futura deve consultar a Silver ou utilizar uma derivação independente que preserve chave e atributos necessários.

# D. Cardinalidade das granularidades

As contagens são **exatas**, sem produto cartesiano: somente combinações existentes. Foram usados `data_internacao`, `CD_MUN`, `codigo_categoria` e `sexo`.

| Grão | Gold completa | Data de entrada em 2023–2025 |
|:--|--:|--:|
| A: dia + município | 598.380 | 578.832 |
| B: A + categoria CID | 5.566.715 | 5.490.908 |
| C: B + sexo | 6.010.303 | 5.929.780 |
| D: C + faixa da idade bruta | 6.686.667 | 6.599.390 |

A última linha representa a simulação literal na Gold e **não deve ser usada como classificação etária válida**.

## Simulação etária com unidade correta

Foram simuladas apenas em memória as faixas 0–17, 18–39, 40–59, 60–79 e 80+. A simulação consultou `COD_IDADE` na Silver e a correspondência CID→categoria da Gold.

A correspondência possui **9.573 códigos CID e 1.658 categorias**, sem mapeamentos ausentes. As contagens A, B e C reconstruídas pela Silver coincidiram exatamente com as da Gold.

| Tratamento da idade ignorada | Base completa | 2023–2025 |
|:--|--:|--:|
| Somente as cinco faixas válidas | 6.684.308 | 6.597.065 |
| Cinco faixas + grupo Ignorada | 6.684.309 | 6.597.066 |

Recomenda-se preservar o grupo Ignorada para conservar os totais. Distribuição de registros nas faixas corrigidas, na base completa:

| Faixa | Registros |
|:--|--:|
| 0–17 | 1.180.868 |
| 18–39 | 2.422.780 |
| 40–59 | 2.023.105 |
| 60–79 | 2.199.785 |
| 80+ | 582.508 |
| Ignorada | 1 |

## Dias sem registros

A grade de 645 municípios × 1.096 dias teria **706.920 linhas** em 2023–2025. A granularidade A observada tem 578.832, uma diferença de **128.088 combinações**. Essas combinações não foram criadas. Para análises temporais futuras, é necessário distinguir zero internações, ausência de registro e completude da extração.

# E. Validação de data + município para meteorologia

## Associação fixa de estação

Foram encontrados 645 municípios e 40 estações. Há **zero municípios com mais de uma estação** tanto em `municipio_estacao` quanto ao longo das datas da Gold.

O script seleciona a estação mais próxima pela distância Haversine. Não há vigência temporal nem substituição diária em função da disponibilidade de medições. A relação é fixa no snapshot auditado; isso não constitui garantia sobre versões futuras do cadastro.

| Estatística da distância | Valor |
|:--|--:|
| Mínima | 0,69 km |
| Mediana | 32,44 km |
| Média | 34,29 km |
| Máxima | 145,63 km |
| Municípios acima de 100 km | 3 |

## Unicidade das variáveis

Foram comparados estação, todos os nove indicadores, contadores horários, flag e motivo de ausência por `data_internacao + CD_MUN`. O resultado foi **zero chaves com combinações divergentes**, incluindo diferenças entre nulo e preenchido.

A comparação resultou em 598.380 combinações meteorológicas distintas, exatamente o número de chaves municipais diárias da Gold completa. Não foram encontrados nulos em data, município ou código da estação nessa representação.

## Limite temporal e significado da flag

A Gold contém datas de internação de **01/01/2008 a 31/12/2025**. Os **172.057 registros anteriores a 2023** não encontram meteorologia. Dentro de 2023–2025, todos encontram estação-dia.

`meteorologia_disponivel = 1` significa correspondência no join. Uma linha pode ter essa flag e ainda apresentar temperatura, umidade, precipitação ou vento nulos.

# F. Recomendação técnica para Power BI

**Recomendação: A + D**, com tratamento correto da idade e preservação explícita da idade ignorada.

| Opção | Capacidade analítica |
|:--|:--|
| B | Categoria, grupo e capítulo; elimina sexo e idade |
| C | Preserva sexo; elimina faixa etária |
| D | Preserva os recortes solicitados; pode agregar para C e B |

Em 2023–2025, D corrigida com Ignorada tem aproximadamente **11,3% mais linhas que C** e **20,1% mais que B**. A diferença favorece preservar o detalhamento necessário, mas não substitui uma avaliação futura de tamanho do modelo. Nenhum desempenho Power BI foi medido.

Separação proposta:

1. `fato_municipio_dia`: totais de saúde e meteorologia, uma linha por município-dia.
2. `fato_municipio_dia_cid`: medidas de saúde por categoria, sexo e faixa etária.
3. Dimensões compartilhadas de município, CID e tempo; estação associada à exposição meteorológica.

A meteorologia deve ser calculada e representada no grão municipal diário, evitando médias ponderadas acidentalmente pelo número de internações ou de categorias. As duas fatos contêm medidas de saúde sobre os mesmos registros: seus totais não devem ser somados entre si.

# G. Riscos e decisões pendentes

1. **Idade sem unidade na Gold.** É o principal impedimento para construir D corretamente usando somente a Gold.
2. **Competência e entrada hospitalar diferentes.** O filtro temporal analítico deve usar `data_internacao`; `ano` e `mes` representam competência.
3. **Linhas SIH não demonstram episódios únicos.** A Gold não preserva `N_AIH`, `IDENT` e demais atributos necessários à investigação de continuidade/repetição. A auditoria confirmou registros, não pacientes únicos.
4. **Dias ausentes da agregação.** A tabela observada omite 128.088 município-dias em 2023–2025. Preencher zeros exige critério de completude da fonte.
5. **Qualidade meteorológica variável.** Precipitação parcial pode subestimar acumulados; extremos e médias podem ser afetados por horas ausentes. Manter os contadores horários.
6. **Exposição espacial aproximada.** A estação fixa representa o município de residência; há distâncias relevantes e compartilhamento de estações.
7. **Convenção temporal UTC.** Definir e documentar o alinhamento com a data civil da internação antes de interpretar exposição no mesmo dia.
8. **Perda de subcategoria CID.** B, C e D permitem categoria→grupo→capítulo, mas não recuperação do código diagnóstico completo. A Gold aplica compatibilizações além da Silver CID.
9. **Valores financeiros.** Preferir “valor registrado da internação” a “custo econômico real”. Não somar valor total e valor de UTI sem validar sua composição.
10. **Lags em calendário contínuo.** Buscar data menos 1, 3 ou 7 dias; não usar a linha anterior de uma série esparsa. Os primeiros dias de janeiro de 2023 requerem dezembro de 2022 para alguns lags, período ausente da Silver diária auditada.
11. **Associação não demonstra causalidade.** Considerar sazonalidade, tendência e diferenças entre municípios. A dimensão IBGE auditada não contém população para construir taxas.

\newpage

# Proposta de schemas — não implementada

Todos os nomes desta seção são **propostos**. Não foram criados datasets em `data/consumption/`.

## fato_municipio_dia

**Grão e chave:** `data + codigo_municipio`.

| Campo | Tipo proposto |
|:--|:--|
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
| `meteorologia_disponivel` | boolean |
| `motivo_sem_meteorologia` | string |

Médias de permanência, valores médios e proporção de óbitos devem ser calculados a partir dos totais e denominadores. Os valores meteorológicos não devem ser somados sobre as internações.

Extensões futuras: `temperatura_media_lag_1_c`, `temperatura_media_lag_3_c` e `temperatura_media_lag_7_c` (double), acompanhadas dos respectivos controles de observações. Radiação e pressão só devem entrar após definição de suas agregações na Silver horária.

## fato_municipio_dia_cid

**Grão D e chave:** `data + codigo_municipio + codigo_categoria + sexo + faixa_etaria`.

| Campo | Tipo proposto |
|:--|:--|
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

Faixas: 0–17, 18–39, 40–59, 60–79 e 80+, com tratamento explícito de Ignorada. A construção depende da recuperação da unidade etária na Silver. Não replicar os indicadores meteorológicos em cada categoria como medidas aditivas.

## dim_municipio

**Grão:** município. **Chave:** `codigo_municipio`.

| Campo | Tipo proposto |
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

**Grão:** categoria CID. **Chave:** `codigo_categoria`.

| Campo | Tipo proposto |
|:--|:--|
| `codigo_categoria` | string |
| `descricao_categoria` | string |
| `cat_inicial_grupo` | string |
| `cat_final_grupo` | string |
| `codigo_grupo` | string, derivado dos limites do grupo |
| `descricao_grupo` | string |
| `codigo_capitulo` | string |
| `descricao_capitulo` | string |

Foram verificadas **1.658 categorias e 1.658 combinações de hierarquia**, sem conflitos ou nulos nesses atributos na Gold. A dimensão deve incorporar a hierarquia efetivamente resolvida pela Gold. Uma dimensão por CID completo exigiria uma fato no mesmo detalhamento.

## dim_tempo

**Grão:** dia de calendário, sem lacunas. **Chave:** `data`.

| Campo | Tipo proposto |
|:--|:--|
| `data` | date |
| `data_id` | int, formato AAAAMMDD |
| `ano` | int |
| `mes` | int |
| `nome_mes` | string |
| `ano_mes` | string |
| `trimestre` | int |
| `dia_mes` | int |
| `dia_semana_numero` | int |
| `dia_semana_nome` | string |
| `fim_de_semana` | boolean |

Os atributos devem derivar da data analítica, distinguindo-a da competência SIH. Um calendário anterior a 2023 pode representar as datas de lag, mas não supre a falta das medições correspondentes.

## dim_estacao

**Grão:** estação meteorológica. **Chave:** `codigo_estacao`.

| Campo | Tipo proposto |
|:--|:--|
| `codigo_estacao` | string |
| `nome_estacao` | string |
| `latitude_estacao` | double |
| `longitude_estacao` | double |
| `uf` | string |
| `regiao` | string |
| `altitude_m` | double |

UF, região e altitude existem na Silver horária. A consistência desses atributos por estação deverá ser validada na futura construção.

# Conclusão

A camada de consumo é viável mantendo Bronze → Silver → Gold → DQ intacto. O desenho **A + D** preserva os recortes analíticos solicitados e mantém a meteorologia em sua granularidade municipal diária.

Antes da implementação, as decisões centrais são: recuperar a unidade etária; definir o calendário e os zeros; explicitar a qualidade meteorológica; e documentar o alinhamento temporal e os lags. Radiação e pressão são possibilidades reais de extensão a partir da Silver horária, mas ainda não integram os indicadores diários disponíveis.

# Referências de apoio à interpretação da idade

Os resultados quantitativos e schemas deste relatório provêm dos dados e scripts locais. As fontes externas abaixo apoiam apenas a interpretação de campos SIH.

- Ministério da Saúde. *Auditoria no SUS: noções básicas sobre Sistemas de Informações*. Dicionário: `COD_IDADE`, `IDADE`, `DIAS_PERM` e `MORTE`. [Documento](https://bvsms.saude.gov.br/bvs/publicacoes/auditoria_sus2004_2.pdf).
- Projeto microdatasus. *process_sih.R*. Tratamento de `COD_IDADE`, incluindo centena de anos (`100 + idade`). [Código-fonte](https://github.com/rfsaldanha/microdatasus/blob/master/R/process_sih.R).

**Status final: proposta documentada; nenhuma implementação na camada de consumo.**
