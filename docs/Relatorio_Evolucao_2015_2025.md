# Relatório técnico — evolução do TCC para 2015–2025

Data da execução e consolidação: 01/10/2026.

## 1. Resultado executivo

O pipeline foi evoluído e executado para o período analítico completo de
**01/01/2015 a 31/12/2025**. A arquitetura Medallion foi preservada. Bronze,
Silver, Gold, Consumption, controles de qualidade, orquestração Airflow e
modelo semântico Power BI foram ajustados onde havia dependência do recorte
2023–2025.

O resultado publicado contém **27.671.456 internações**, um painel completo de
**2.591.610 município-dias** (645 municípios × 4.018 dias) e uma fato analítica
CID com **21.751.139 linhas agregadas**. Todos os DQs finais de Silver, Gold e
Consumption terminaram em `PASS`; os 27 testes automatizados executados
terminaram com sucesso.

As seis perguntas de pesquisa foram materializadas em Parquet em
`data/validation/perguntas_pesquisa`. Os resultados são descritivos e
observacionais. Nenhum contraste apresentado neste relatório deve ser
interpretado como efeito causal da temperatura.

## 2. Períodos efetivamente usados

| Fonte/camada | Período | Finalidade |
|---|---:|---|
| SIH — competências | 2015-01 a 2026-07 | Capturar apresentações tardias de internações ocorridas até 2025 |
| SIH — filtro analítico | 2015-01-01 a 2025-12-31 em `DT_INTER` | Define o universo final de internações |
| INMET — período técnico | 2014-12-25 a 2025-12-31 | Suportar D−7 desde 01/01/2015 |
| IBGE | Malha municipal de 2022 | Universo territorial de 645 municípios paulistas |
| CID-10 | Acervo integral disponível | Resolver capítulo, grupo, categoria e subcategoria |
| Consumption/Power BI | 2015-01-01 a 2025-12-31 | Período analítico final |

As datas e competências passaram a ser derivadas de
`src/common/study_period.py`. Os valores padrão podem ser substituídos por
variáveis de ambiente em reprocessamentos controlados, sem espalhar listas de
anos pelo código.

## 3. Auditoria do repositório

A busca sistemática cobriu `src`, `dags`, `tests`, `docs`, Docker, PBIP/TMDL,
DAX e Power Query. As ocorrências de 2023–2025, 36 meses/arquivos, 1.096 dias,
listas fixas e cardinalidades antigas foram classificadas da seguinte forma:

1. **Alteradas:** filtros e loops de Bronze/Silver/Gold/Consumption, DQs,
   calendário, partições esperadas, DAG e modelo semântico.
2. **Convertidas em configuração dinâmica:** período analítico, período
   técnico INMET, janela de competências SIH, anos, meses, dias esperados e
   partições anuais.
3. **Mantidas:** regras de negócio independentes do período, arquitetura
   Medallion, grãos das fatos, faixas etárias e limites térmicos.
4. **Preservadas como histórico:** relatórios e experimentos anteriores cuja
   numeração descreve explicitamente a versão 2023–2025.

Riscos identificados e tratados:

- competência SIH não é equivalente à data real da internação;
- a estação mais próxima atual pode não possuir cobertura histórica;
- ausência de linha SIH e ausência de meteorologia tinham semânticas distintas;
- lags baseados em linha anterior seriam incorretos em séries esparsas;
- joins com a hierarquia CID ou meteorologia poderiam multiplicar eventos;
- sobrescrita direta de Parquet podia deixar uma camada indisponível após falha;
- códigos e cardinalidades fixas da versão de três anos causavam falsos DQs.

## 4. Decisões arquiteturais

### 4.1 SIH: competência versus data de internação

Foram processadas **139 competências**, de 2015-01 a 2026-07. A seleção final
é sempre feita por `DT_INTER`/`data_internacao` entre 2015-01-01 e 2025-12-31.
Assim, competências posteriores contribuem para completar a cauda de 2025 sem
introduzir internações de 2026 na análise.

### 4.2 Município → estação por ano

A associação deixou de ser uma única estação contemporânea por município e
passou a ser uma seleção **município-ano**. Em cada ano, escolhe-se a estação
mais próxima entre as que atingem cobertura mínima de 80% para temperatura;
o critério, a distância e a cobertura escolhida ficam persistidos e auditáveis.

O mapa resultante possui 7.095 linhas (645 × 11), usa 42 estações, não contém
seleções abaixo de 80%, tem distância média de 39,20 km e máxima de 172,08 km.
Não foi aplicado forward fill, backfill nem imputação meteorológica.

### 4.3 Painel completo município-dia

Depois de confirmar a completude das competências ingeridas, a ausência de AIH
para um município-dia foi interpretada como zero evento observado. A fato
diária passou a ser construída por `dim_municipio × dim_tempo`, seguida de
`LEFT JOIN` com saúde e meteorologia:

- métricas de saúde ausentes são preenchidas com zero;
- meteorologia sem registro permanece `NULL`;
- `dia_sem_internacao` e flags/contadores meteorológicos tornam a semântica
  explícita.

Isso evita o viés de calcular médias apenas nos dias em que houve internação.

### 4.4 Lags por data civil exata

D−1, D−3 e D−7 usam joins com a data civil exata e a estação correspondente,
não `lag()` posicional. Foram validados isolamento municipal, séries esparsas,
virada de ano, ano bissexto e os primeiros dias de 2015. O período técnico do
INMET começa em 25/12/2014 justamente para suportar D−7 em 01/01/2015.

### 4.5 Duas fatos, sem duplicar meteorologia

`fato_municipio_dia` permanece no grão data + município e contém saúde,
meteorologia e lags. `fato_municipio_dia_cid` permanece no grão composto por
data, município, categoria CID, sexo e faixa etária, sem replicar colunas
meteorológicas. A
ligação analítica usa `municipio_dia_id`, evitando multiplicação das medidas
meteorológicas.

### 4.6 Publicação idempotente

Downloads existentes são reutilizados, o SIH é enumerado por uma lista explícita
de competências e as publicações derivadas usam staging + troca de diretório.
Diretórios temporários ou cópias de segurança não entram nas leituras mensais.

## 5. Cardinalidades e reconciliação

### 5.1 Principais tabelas

| Tabela | Linhas |
|---|---:|
| Silver SIH (competências 2015-01 a 2026-07) | 29.598.019 |
| Silver INMET horário | 3.723.456 |
| Silver INMET diário estação-dia | 155.144 |
| Gold `municipio_estacao` | 7.095 |
| Gold `fato_internacao` | 27.671.456 |
| Gold `fato_internacao_meteorologia` | 27.671.456 |
| Consumption `dim_municipio` | 645 |
| Consumption `dim_cid` | 1.797 |
| Consumption `dim_tempo` | 4.018 |
| Consumption `dim_estacao` | 43 |
| Consumption `fato_municipio_dia` | 2.591.610 |
| Consumption `fato_municipio_dia_cid` | 21.751.139 |

### 5.2 Internações por ano

| Ano | Internações | Com temperatura válida |
|---:|---:|---:|
| 2015 | 2.406.452 | 2.376.634 |
| 2016 | 2.404.559 | 2.356.355 |
| 2017 | 2.444.762 | 2.404.387 |
| 2018 | 2.480.863 | 2.417.479 |
| 2019 | 2.554.232 | 2.448.623 |
| 2020 | 2.221.057 | 2.189.600 |
| 2021 | 2.337.704 | 2.297.340 |
| 2022 | 2.484.567 | 2.404.557 |
| 2023 | 2.628.762 | 2.619.588 |
| 2024 | 2.801.991 | 2.746.502 |
| 2025 | 2.906.507 | 2.881.867 |
| **Total** | **27.671.456** | **27.142.932** |

As reconciliações total, anual e municipal entre Gold e as duas fatos de
Consumption não apresentaram divergências. A junção meteorológica preservou
todos os eventos: 27.625.127 encontraram estação-dia e 46.329 permaneceram sem
match, com meteorologia `NULL`. Nenhuma internação foi descartada por esse
motivo. Não houve categoria CID sem correspondência no universo final, inclusive
na auditoria anual.

## 6. Cobertura meteorológica

### 6.1 Cobertura de temperatura no acervo estação-dia

| Ano | Cobertura |
|---:|---:|
| 2015 | 97,69% |
| 2016 | 93,69% |
| 2017 | 89,22% |
| 2018 | 97,77% |
| 2019 | 93,99% |
| 2020 | 86,04% |
| 2021 | 67,90% |
| 2022 | 90,04% |
| 2023 | 96,06% |
| 2024 | 90,33% |
| 2025 | 90,31% |

A queda bruta de 2021 confirma que proximidade geográfica isolada não seria
uma regra segura. A seleção município-ano escolhe estações com disponibilidade
adequada e eleva a cobertura efetiva do painel.

### 6.2 Cobertura efetiva no painel município-dia

| Ano | Município-dias com temperatura |
|---:|---:|
| 2015 | 97,51% |
| 2016 | 98,53% |
| 2017 | 98,07% |
| 2018 | 97,50% |
| 2019 | 94,71% |
| 2020 | 98,01% |
| 2021 | 97,74% |
| 2022 | 95,87% |
| 2023 | 99,40% |
| 2024 | 97,22% |
| 2025 | 98,86% |

Por município, a cobertura de temperatura tem mínimo de 92,91%, média de
97,59% e máximo de 100%; nenhum município ficou abaixo de 80%. Na Gold, 98,09%
das internações possuem temperatura, 91,28% precipitação, 97,77% umidade e
93,23% vento. Foram detectadas 41 estações com revisões de metadados ao longo
dos anos; a dimensão exibe deterministicamente o cadastro de 2025, enquanto o
mapa histórico e as medidas anuais permanecem preservados.

## 7. Investigação da cauda SIH de 2025

A auditoria por competência confirmou que baixar somente até 2025-12 seria
insuficiente. As competências de 2026 acrescentaram **98.283 internações de
2025**, sendo 19.661 de novembro e 70.085 de dezembro.

| Competência | Registros com `DT_INTER` em 2025 | Novembro/2025 | Dezembro/2025 |
|---|---:|---:|---:|
| 2026-01 | 70.734 | 13.720 | 50.749 |
| 2026-02 | 20.283 | 5.019 | 14.168 |
| 2026-03 | 5.408 | 627 | 4.370 |
| 2026-04 | 1.098 | 189 | 613 |
| 2026-05 | 433 | 66 | 132 |
| 2026-06 | 64 | 11 | 13 |
| 2026-07 | 263 | 29 | 40 |

A competência mais recente disponível na auditoria, 2026-07, ainda acrescenta
40 casos de dezembro/2025. Portanto, a janela foi estendida até a última
competência publicada, mas permanece um **risco residual de truncamento à
direita**: futuras competências podem registrar pequenas quantidades adicionais.
O relatório não declara dezembro de 2025 definitivamente encerrado.

## 8. Data Quality, testes e desempenho

### 8.1 DQ

- Silver SIH: `PASS`, 29.598.019 linhas e 139 competências completas.
- Silver INMET horário e diário: `PASS`, período técnico exato e 43 estações.
- Gold mapa município-estação: `PASS`, 7.095 chaves únicas e cobertura mínima.
- Gold internações: `PASS`, total/anual, CID, domínios e métricas reconciliados.
- Gold meteorologia: `PASS`, sem multiplicação; 46.329 eventos sem estação-dia
  foram mantidos e reportados como `WARNING`.
- Consumption: `PASS`, calendário contínuo, painel 645 × 4.018, zeros distintos
  de `NULL`, integridade referencial, partições, lags e reconciliações.

Foram encontrados 16 registros `SEXO=4`, todos fora do período analítico, em
internações de 2026 presentes nas competências de suporte. Eles são registrados
como aviso; no recorte 2015–2025, o domínio observado é 1/3.

### 8.2 Testes automatizados

Foram executados 27 testes com sucesso:

- 5 de orquestração Bronze/DAG/períodos/gates;
- 6 de métricas e validação da fato de internações;
- 3 de persistência e equivalência meteorológica;
- 3 do parser histórico INMET;
- 10 da camada Consumption, incluindo calendário, painel, zeros × `NULL`,
  hierarquia CID, estação por ano, lags, virada de ano e ano bissexto.

A suíte encontrou uma regressão na reconciliação de um fluxo sintético: o
painel materializava anos de total zero, enquanto as fatos esparsas não criavam
esses grupos. O DQ foi corrigido para normalizar o domínio anual antes de
comparar; o teste foi mantido e passou na repetição.

### 8.3 Desempenho medido

Não houve aumento arbitrário de memória ou partições. O principal gargalo
observado foi a recomputação repetida da agregação diária INMET. A versão
otimizada persiste o agregado em `MEMORY_AND_DISK` e grava partições anuais:

- tentativa baseline: superior a 9 minutos e ainda incompleta quando
  interrompida;
- versão otimizada: **149,63 s**, concluída e validada;
- Gold internações: 449,46 s;
- Gold com meteorologia: 181,43 s;
- Consumption: 190,05 s;
- DQ Consumption: 273,54 s.

As fatos de Consumption permanecem particionadas dinamicamente por ano, de
2015 a 2025. Isso evita partições excessivamente pequenas e mantém pruning
temporal para o Power BI.

## 9. Validação explícita das seis perguntas

Definições mantidas: frio = temperatura < 20 °C; quente = temperatura ≥ 25 °C;
20 a 24,9 °C fica fora do contraste binário. O denominador é município-dia com
temperatura válida na condição correspondente.

### Pergunta 1 — faixas de temperatura

| Faixa | Município-dias | Internações | Média por município-dia |
|---|---:|---:|---:|
| < 15 °C | 92.285 | 1.232.506 | 13,355 |
| 15 a 19,9 °C | 524.456 | 7.749.512 | 14,776 |
| 20 a 24,9 °C | 1.236.945 | 13.650.067 | 11,035 |
| 25 a 29,9 °C | 637.738 | 4.335.019 | 6,797 |
| ≥ 30 °C | 37.607 | 175.828 | 4,675 |

### Pergunta 2 — estabilidade anual e sazonalidade

| Ano | Média frio | Média quente | Diferença absoluta | Diferença relativa | Dias frios | Dias quentes |
|---:|---:|---:|---:|---:|---:|---:|
| 2015 | 12,469 | 6,954 | 5,515 | 79,3% | 56.195 | 54.418 |
| 2016 | 12,671 | 6,597 | 6,074 | 92,1% | 70.088 | 53.825 |
| 2017 | 14,443 | 6,330 | 8,113 | 128,2% | 60.576 | 53.392 |
| 2018 | 14,800 | 5,898 | 8,901 | 150,9% | 55.253 | 50.211 |
| 2019 | 14,664 | 7,458 | 7,206 | 96,6% | 47.064 | 64.638 |
| 2020 | 13,353 | 5,640 | 7,713 | 136,8% | 53.524 | 63.302 |
| 2021 | 15,120 | 5,382 | 9,738 | 180,9% | 57.133 | 74.916 |
| 2022 | 15,358 | 5,258 | 10,100 | 192,1% | 63.653 | 57.346 |
| 2023 | 13,900 | 7,373 | 6,527 | 88,5% | 57.137 | 59.426 |
| 2024 | 16,310 | 8,103 | 8,207 | 101,3% | 38.810 | 79.011 |
| 2025 | 17,924 | 8,033 | 9,890 | 123,1% | 57.308 | 64.860 |

Também foi materializada a série ano-mês completa e a abertura anual pelas
cinco faixas térmicas. Ela permite inspecionar sazonalidade sem inferir que a
temperatura seja a causa das diferenças.

### Pergunta 3 — hierarquia CID

Há saídas próprias para capítulo, grupo e categoria, com médias, diferenças e
denominadores. Exemplos de maior diferença absoluta:

| Nível | Código/descrição | Média frio | Média quente | Diferença abs. | Diferença rel. |
|---|---|---:|---:|---:|---:|
| Capítulo | XV — Gravidez, parto e puerpério | 2,524 | 0,994 | 1,531 | 154,1% |
| Capítulo | IX — Aparelho circulatório | 1,582 | 0,715 | 0,867 | 121,2% |
| Capítulo | X — Aparelho respiratório | 1,379 | 0,573 | 0,807 | 140,8% |
| Grupo | O80–O84 — Parto | 1,381 | 0,515 | 0,866 | 168,0% |
| Grupo | J09–J18 — Influenza e pneumonia | 0,690 | 0,299 | 0,391 | 130,7% |
| Categoria | O80 — Parto único espontâneo | 1,081 | 0,346 | 0,735 | 212,3% |
| Categoria | J18 — Pneumonia não especificada | 0,535 | 0,231 | 0,304 | 131,9% |

O sinal não é universal: a categoria A90 (dengue) apresentou diferença de
−0,022 internação por município-dia (−47,1%) no contraste frio menos quente.

### Pergunta 4 — faixa etária, sexo e CID

| Faixa etária | Sexo | Média frio | Média quente | Diferença relativa |
|---|---:|---:|---:|---:|
| 0–17 | 1 | 1,205 | 0,488 | 146,8% |
| 0–17 | 3 | 1,037 | 0,425 | 143,7% |
| 18–39 | 1 | 1,266 | 0,582 | 117,5% |
| 18–39 | 3 | 3,376 | 1,430 | 136,1% |
| 40–59 | 1 | 1,673 | 0,800 | 109,1% |
| 40–59 | 3 | 1,692 | 0,796 | 112,5% |
| 60–79 | 1 | 1,788 | 0,880 | 103,1% |
| 60–79 | 3 | 1,612 | 0,785 | 105,4% |
| 80+ | 1 | 0,391 | 0,216 | 80,9% |
| 80+ | 3 | 0,524 | 0,276 | 90,0% |

A saída adicional `pergunta_4_cid_idade_sexo` comprova o cruzamento simultâneo
com categoria CID, sem replicar meteorologia na fato CID.

### Pergunta 5 — D0, D−1, D−3 e D−7

| Defasagem | Média frio | Média quente | Diferença abs. | Diferença rel. | Amostra frio | Amostra quente |
|---|---:|---:|---:|---:|---:|---:|
| D0 | 14,564 | 6,679 | 7,884 | 118,0% | 616.741 | 675.345 |
| D−1 | 14,592 | 6,649 | 7,943 | 119,5% | 616.731 | 675.255 |
| D−3 | 14,725 | 6,508 | 8,217 | 126,3% | 616.693 | 674.720 |
| D−7 | 14,736 | 6,557 | 8,179 | 124,7% | 616.667 | 673.707 |

### Pergunta 6 — comparação interna a cada município

Os 645 municípios possuem médias e denominadores próprios. Exemplos dos maiores
contrastes absolutos positivos:

| Município | Dias frios | Dias quentes | Média frio | Média quente | Diferença abs. | Diferença rel. |
|---|---:|---:|---:|---:|---:|---:|
| Mogi das Cruzes | 837 | 1.047 | 68,585 | 66,392 | 2,194 | 3,3% |
| São José do Rio Preto | 529 | 1.516 | 92,637 | 90,647 | 1,990 | 2,2% |
| Taboão da Serra | 1.779 | 196 | 44,722 | 42,974 | 1,748 | 4,1% |
| Mongaguá | 1.878 | 257 | 8,920 | 7,467 | 1,453 | 19,5% |

Há também contrastes negativos, por exemplo São Paulo (−53,520; −3,2%) e Itu
(−8,893; −29,5%). Isso reforça que a magnitude e até o sinal variam no
território e que comparar cada município consigo mesmo é mais apropriado do que
ordená-los apenas por volume total. A saída mantém `amostra_valida`, exigindo ao
menos 30 município-dias em cada condição.

## 10. Power BI

O PBIP/TMDL foi inspecionado com validação offline. Foram feitas apenas mudanças
mínimas no modelo semântico: período da dimensão temporal, exposição dos campos
necessários e relacionamento por `municipio_dia_id`. O revisor offline terminou
com pontuação 100/100. A alteração visual já existente em
`Dashboard/Dash.Report/report.json` foi preservada e não faz parte desta
implementação.

O modelo suporta slicer de 2015–2025, evolução mensal/anual, faixas térmicas,
frio × quente, hierarquia CID, sexo, faixa etária, lags e análise territorial.
Não houve redesenho das páginas do dashboard.

## 11. Arquivos implementados ou alterados

Principais grupos de mudança:

- configuração/utilitários: `src/common/study_period.py`, `sih_io.py`,
  `spark_io.py`, `validators.py`;
- ingestão/orquestração: `src/bronze/inmet.py`, `src/orchestration/bronze.py`,
  `dags/tcc_pipeline.py`;
- Silver: `src/silver/inmet.py`, `inmet_diario.py`, `sih.py`,
  `processar_sih_lote.py`;
- Gold: `src/gold/municipio_estacao.py`, `fato_internacao.py`,
  `fato_internacao_meteorologia.py`;
- Consumption: `src/consumption/build.py`, `transformations.py`;
- DQ: módulos Silver, Gold e Consumption em `src/quality`;
- auditorias: `src/audit/inmet_coverage.py`, `sih_competence.py`,
  `sih_domains.py`;
- validações: `src/validation/research_questions.py`, `report_summary.py`;
- testes: cinco módulos em `tests/`;
- Power BI: TMDL de `dim_tempo`, `fato_municipio_dia` e
  `fato_municipio_dia_cid`;
- documentação: `README.md` e este relatório.

## 12. Limitações remanescentes

1. **Causalidade:** os contrastes misturam sazonalidade, circulação viral,
   demografia, infraestrutura hospitalar, perfil socioeconômico, poluição e
   outros fatores não controlados.
2. **População:** não há denominador populacional; os resultados são médias por
   município-dia, não taxas por habitante.
3. **SIH 2025:** competências futuras ainda podem acrescentar poucos registros
   de novembro/dezembro de 2025.
4. **Meteorologia:** a estação selecionada é uma aproximação espacial; distância
   e cobertura ficam disponíveis para análise de sensibilidade.
5. **Cobertura:** `NULL` meteorológico é preservado e os denominadores das
   comparações excluem esses dias; não houve imputação.
6. **Cadastro de estações:** revisões históricas de metadados foram detectadas;
   a dimensão apresenta a revisão de 2025 e a seleção anual permanece auditável.
7. **Categorias raras:** diferenças relativas com poucos eventos devem ser
   interpretadas junto dos totais e denominadores, mesmo quando a exposição
   térmica atende ao mínimo de dias.

## 13. Reprodução

```powershell
docker compose exec -T spark spark-submit src/consumption/build.py
docker compose exec -T spark spark-submit src/quality/consumption/dq_consumption.py
docker compose exec -T spark spark-submit src/validation/research_questions.py
docker compose exec -T spark spark-submit src/validation/report_summary.py
```

As saídas analíticas completas estão em
`data/validation/perguntas_pesquisa`; este documento mostra apenas recortes
compactos para evidenciar que todas as perguntas são respondíveis sem depender
do Power BI.
