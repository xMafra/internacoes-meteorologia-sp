# Relatório de Auditoria das Métricas Analíticas

**Projeto:** Engenharia de Dados — internações hospitalares e condições meteorológicas no Estado de São Paulo  
**Período analisado:** 01/01/2015 a 31/12/2025  
**Data da auditoria:** 01/10/2026  
**Escopo:** validade matemática das métricas usadas nas seis perguntas de pesquisa

## Conclusão executiva

A auditoria identificou um efeito de composição municipal material. No agregado estadual, a média foi de **14,5637 internações por município-dia frio** e **6,6793 por município-dia quente**, diferença relativa de **+118,04%**. Quando cada município é comparado consigo mesmo e recebe o mesmo peso no resumo, a mediana da diferença relativa é **−1,89%** e a média é **−1,85%** entre 640 municípios válidos. Pelo sinal estrito, 39,22% ficaram acima de zero e 60,78% abaixo de zero. Em paralelo, 10,31% ficaram na faixa aproximadamente nula de −1% a +1%; 34,06% ficaram acima de +1% e 55,63% abaixo de −1%.

Portanto, o contraste estadual é matematicamente válido como descrição do conjunto de município-dias, mas **não representa o contraste típico dentro de um município**. A distribuição térmica é desigual: o decil de municípios com maior volume hospitalar concentra 13,81% dos dias frios e apenas 5,78% dos dias quentes; no decil de menor volume, as participações são 7,17% e 14,04%, respectivamente. Um índice de volume hospitalar basal ponderado pelos dias de cada condição foi 14,6688 para frio e 6,5832 para quente, diferença de 8,0855, muito próxima da diferença agregada observada de 7,8844. Isso é evidência descritiva de composição, não evidência causal.

As métricas CID e dos estratos foram confirmadas com o denominador correto: todos os município-dias com temperatura válida da condição, inclusive os dias sem evento da categoria. Também foi encontrado e corrigido um defeito nos lags em viradas de ano com troca de estação. O código de sexo foi preservado e recebeu rótulo baseado em documentação oficial do SIH/HOSPUB.

Nenhum threshold foi alterado, não houve normalização populacional, nenhuma conclusão causal foi produzida e o dashboard não foi redesenhado.

## 1. Definição matemática das métricas atuais

Seja \(i\) um município-dia, \(Y_i\) o número de internações e \(T_i\) a temperatura média diária atribuída ao município. Mantiveram-se os thresholds existentes:

- frio: \(T_i < 20\ ^\circ C\);
- quente: \(T_i \ge 25\ ^\circ C\);
- faixa intermediária: não entra no contraste frio versus quente.

As métricas estaduais atuais são:

\[
\bar{Y}_{frio} = \frac{\sum_i Y_i \mathbb{1}(T_i<20)}{\sum_i \mathbb{1}(T_i<20)}
\]

\[
\bar{Y}_{quente} = \frac{\sum_i Y_i \mathbb{1}(T_i\ge25)}{\sum_i \mathbb{1}(T_i\ge25)}
\]

\[
\Delta_{abs}=\bar{Y}_{frio}-\bar{Y}_{quente}
\qquad
\Delta_{rel}=\frac{\bar{Y}_{frio}-\bar{Y}_{quente}}{\bar{Y}_{quente}}
\]

Essa razão dá o mesmo peso a cada município-dia, não a cada município. Consequentemente, municípios com mais dias em uma condição e/ou maior volume de internações exercem maior influência sobre a respectiva média.

Para CID, idade e sexo, o numerador é filtrado pelo estrato, mas o denominador continua sendo o universo de município-dias expostos à condição. Para um estrato \(s\):

\[
\bar{Y}_{frio,s}=\frac{\sum_i Y_{i,s}\mathbb{1}(T_i<20)}{N_{frio}}
\qquad
\bar{Y}_{quente,s}=\frac{\sum_i Y_{i,s}\mathbb{1}(T_i\ge25)}{N_{quente}}
\]

com \(N_{frio}=616.741\) e \(N_{quente}=675.345\). Os zeros implícitos da fato CID esparsa permanecem, portanto, no denominador.

## 2. Definição matemática das métricas intramunicipais

Para cada município \(m\):

\[
\bar{Y}_{frio,m}=\frac{\sum_{i\in m}Y_i\mathbb{1}(T_i<20)}{\sum_{i\in m}\mathbb{1}(T_i<20)}
\]

\[
\bar{Y}_{quente,m}=\frac{\sum_{i\in m}Y_i\mathbb{1}(T_i\ge25)}{\sum_{i\in m}\mathbb{1}(T_i\ge25)}
\]

\[
\Delta_{rel,m}=\frac{\bar{Y}_{frio,m}-\bar{Y}_{quente,m}}{\bar{Y}_{quente,m}}
\]

O município é elegível somente quando possui pelo menos 30 dias frios e 30 dias quentes. A diferença relativa também precisa ter denominador quente diferente de zero. No resumo, cada município válido contribui uma vez. “Positivo” e “negativo” seguem literalmente \(\Delta_{rel,m}>0\) e \(\Delta_{rel,m}<0\). Em paralelo, “aproximadamente zero” foi operacionalizado como \(|\Delta_{rel,m}|\le 0,01\), podendo se sobrepor às classes por sinal. Para análise mutuamente exclusiva também são armazenadas as quantidades acima de +1% e abaixo de −1%. O limiar foi declarado no resultado e não foi ajustado após observar os dados.

## 3. Comparação: agregado estadual versus intramunicipal

| Medida | Resultado |
|---|---:|
| Município-dias frios | 616.741 |
| Município-dias quentes | 675.345 |
| Média agregada — frio | 14,5637 |
| Média agregada — quente | 6,6793 |
| Diferença absoluta agregada | 7,8844 |
| Diferença relativa agregada | +118,04% |
| Municípios totais / elegíveis / válidos | 645 / 640 / 640 |
| Média das diferenças relativas municipais | −1,85% |
| Q1 / mediana / Q3 | −6,29% / −1,89% / +2,05% |
| Mínimo / máximo | −33,26% / +28,35% |
| Positivos (> 0) / negativos (< 0) | 39,22% / 60,78% |
| Acima de +1% / abaixo de −1% / aproximadamente zero (±1%) | 34,06% / 55,63% / 10,31% |

O sinal e a magnitude mudam entre as metodologias. A métrica agregada responde “qual foi a média entre todos os município-dias classificados em cada condição?”. A intramunicipal responde “qual foi a distribuição do contraste quando cada município é comparado consigo mesmo?”. Uma não substitui automaticamente a outra.

### Distribuição por decis

| Decil | Municípios | Mínimo | Mediana | Média | Máximo |
|---:|---:|---:|---:|---:|---:|
| 1 | 64 | −33,26% | −12,67% | −15,24% | −10,63% |
| 2 | 64 | −10,58% | −8,76% | −8,74% | −7,32% |
| 3 | 64 | −7,30% | −6,29% | −6,28% | −5,33% |
| 4 | 64 | −5,32% | −4,37% | −4,36% | −3,34% |
| 5 | 64 | −3,28% | −2,69% | −2,65% | −1,89% |
| 6 | 64 | −1,88% | −1,10% | −1,04% | −0,25% |
| 7 | 64 | −0,22% | +0,74% | +0,74% | +1,54% |
| 8 | 64 | +1,55% | +2,05% | +2,19% | +3,11% |
| 9 | 64 | +3,14% | +4,46% | +4,73% | +6,56% |
| 10 | 64 | +6,58% | +10,47% | +12,14% | +28,35% |

## 4. Resultado intramunicipal por ano

O critério de 30 dias frios e 30 dias quentes foi reaplicado dentro de cada ano, sem flexibilização.

| Ano | Municípios válidos | Média | Q1 | Mediana | Q3 | Positivos | Negativos |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 2015 | 509 | +0,02% | −9,94% | −1,43% | +7,85% | 44,99% | 55,01% |
| 2016 | 521 | +2,08% | −7,82% | +1,51% | +9,96% | 54,32% | 45,49% |
| 2017 | 443 | +1,09% | −8,79% | −0,87% | +9,88% | 46,50% | 53,50% |
| 2018 | 410 | +0,27% | −10,54% | −0,79% | +7,55% | 47,56% | 52,44% |
| 2019 | 444 | +2,58% | −8,24% | +0,71% | +10,72% | 52,70% | 47,30% |
| 2020 | 393 | −13,54% | −22,70% | −15,30% | −7,22% | 14,25% | 85,75% |
| 2021 | 298 | −1,69% | −14,19% | −1,67% | +7,52% | 44,63% | 55,37% |
| 2022 | 392 | +0,74% | −9,56% | −0,12% | +10,05% | 49,74% | 50,26% |
| 2023 | 555 | −0,11% | −9,35% | −1,62% | +6,57% | 43,42% | 56,58% |
| 2024 | 454 | −2,87% | −10,08% | −3,87% | +3,34% | 34,14% | 65,64% |
| 2025 | 535 | +8,88% | −0,40% | +7,91% | +17,46% | 73,83% | 25,98% |

O resultado não é estável em sinal nem magnitude. O ano de 2020 é especialmente distinto, com mediana de −15,30%, e 2025 apresenta mediana de +7,91%. Essas diferenças devem ser descritas como heterogeneidade temporal observada; não autorizam atribuir causas.

## 5. Auditoria dos denominadores CID e dos estratos

A `fato_municipio_dia_cid` contém apenas combinações com evento. Usar a contagem de suas linhas como denominador excluiria dias de zero e produziria uma média condicional à ocorrência. A implementação auditada usa:

| Métrica | Numerador | Denominador frio / quente |
|---|---|---|
| Capítulo CID | internações do capítulo na condição | 616.741 / 675.345 município-dias |
| Grupo CID | internações do grupo na condição | 616.741 / 675.345 município-dias |
| Categoria CID | internações da categoria na condição | 616.741 / 675.345 município-dias |
| Faixa etária | internações da faixa na condição | 616.741 / 675.345 município-dias |
| Sexo | internações do sexo na condição | 616.741 / 675.345 município-dias |
| CID + faixa etária | internações da combinação na condição | 616.741 / 675.345 município-dias |
| CID + sexo | internações da combinação na condição | 616.741 / 675.345 município-dias |
| CID + faixa etária + sexo | internações da combinação na condição | 616.741 / 675.345 município-dias |

Exemplos observados demonstram o impacto do erro que foi evitado:

| Estrato | Média correta frio | Média incorreta, só dias com evento | Média correta quente | Média incorreta, só dias com evento |
|---|---:|---:|---:|---:|
| J18 — pneumonia por microrganismo não especificado | 0,5352 | 2,3479 | 0,2307 | 1,6306 |
| O80 — parto único espontâneo | 1,0806 | 3,9633 | 0,3460 | 2,5834 |
| Sexo masculino | 6,3228 | 9,3236 | 2,9670 | 5,0560 |
| Sexo feminino | 8,2408 | 11,4096 | 3,7123 | 5,9496 |

Os artefatos detalhados guardam, para cada estrato, numerador, município-dias com evento, denominador total de exposição, média correta e média incorreta para comparação.

## 6. Testes sintéticos dos zeros

Foram incluídos quatro testes analíticos específicos:

1. **CID esparsa:** 10 dias frios, com 2 casos no primeiro e 3 no segundo. A função retorna \(5/10=0,5\), enquanto a coluna de diagnóstico mostra que o cálculo incorreto seria \(5/2=2,5\). O cenário quente equivalente retorna \(4/10=0,4\), e não \(4/2=2,0\).
2. **CID + idade + sexo:** a combinação esparsa preserva os 10 dias frios e os 10 dias quentes como denominadores; não usa somente as datas em que a combinação aparece.
3. **Composição:** dois municípios sintéticos têm contraste intramunicipal exatamente zero, mas, ao trocar a participação de um município de alto volume entre frio e quente, o contraste agregado supera 90%. O teste prova que uma diferença agregada grande pode existir sem diferença dentro dos municípios.
4. **Sinal e aproximação de zero:** contrastes de +0,5%, −0,5% e zero produzem um positivo, um negativo e um zero pelo sinal estrito, enquanto os três pertencem simultaneamente à faixa de ±1%.

Esses testes estão em `tests/test_analytic_metrics.py` e exercitam as mesmas funções usadas na geração das evidências.

## 7. Análise CID intramunicipal

Todos os alvos usam os dias frios e quentes do próprio município como denominadores; dias sem evento CID contribuem com zero. A elegibilidade geral é de 30 dias em cada condição. Quando a média quente de um alvo é zero, a diferença relativa fica nula e o município não entra no resumo desse alvo.

| Alvo | Municípios válidos | Q1 | Mediana | Q3 | Positivos | Negativos |
|---|---:|---:|---:|---:|---:|---:|
| Cap. IX — aparelho circulatório | 640 | −4,56% | +3,19% | +11,90% | 60,63% | 39,38% |
| Cap. X — aparelho respiratório | 640 | +5,84% | +17,95% | +30,97% | 84,38% | 15,63% |
| Cap. XV — gravidez, parto e puerpério | 640 | −7,34% | −0,91% | +5,24% | 46,25% | 53,75% |
| J09–J18 — influenza e pneumonia | 640 | +6,28% | +22,66% | +39,43% | 82,81% | 17,19% |
| J20–J22 — outras infecções agudas das vias aéreas inferiores | 592 | +1,90% | +53,89% | +136,45% | 75,68% | 24,32% |
| O80–O84 — parto | 640 | −7,82% | +0,93% | +10,75% | 52,50% | 47,50% |

O capítulo respiratório e os dois grupos respiratórios mantêm contraste positivo na maioria dos municípios, embora muito menor do que alguns contrastes estaduais. Já o Capítulo XV e o grupo de parto ficam próximos de zero na mediana, apesar dos contrastes agregados elevados. O máximo de +3.013% em J20–J22 decorre de bases quentes muito pequenas em alguns municípios; por isso mediana e intervalo interquartil são mais informativos do que a média para esse alvo.

## 8. Análise do possível efeito de composição

### Evidências gerais

- Correlação entre volume total de internações do município e número de dias frios: **+0,1026**.
- Correlação entre volume e dias quentes: **−0,1114**.
- Volume basal diário ponderado pelos dias frios: **14,6688**.
- Volume basal diário ponderado pelos dias quentes: **6,5832**.
- Diferença de composição desse volume basal: **8,0855 internações por município-dia**.
- Diferença agregada efetivamente observada: **7,8844**.

O indicador basal usa a média diária de internações de todo o período de cada município, ponderada separadamente por seus dias frios e quentes. Ele não é um ajuste causal. A proximidade com a diferença agregada mostra que municípios de portes hospitalares distintos estão representados de forma muito diferente nas duas condições.

### Participação dos decis de volume hospitalar

| Decil de volume | Municípios | Volume diário médio | Participação dos dias frios | Participação dos dias quentes |
|---:|---:|---:|---:|---:|
| 1 | 65 | 0,49 | 7,17% | 14,04% |
| 2 | 65 | 0,76 | 8,60% | 11,71% |
| 3 | 65 | 1,07 | 9,76% | 10,90% |
| 4 | 65 | 1,45 | 7,88% | 12,04% |
| 5 | 65 | 2,23 | 11,11% | 9,34% |
| 6 | 64 | 3,24 | 9,83% | 10,14% |
| 7 | 64 | 5,14 | 10,32% | 9,01% |
| 8 | 64 | 8,04 | 9,60% | 9,55% |
| 9 | 64 | 13,41 | 11,92% | 7,49% |
| 10 | 64 | 71,68 | 13,81% | 5,78% |

Os municípios com mais dias frios incluem Monteiro Lobato, Santo Antônio do Pinhal, São Bento do Sapucaí, Potim e Campos do Jordão, cada um com 3.762 dias frios e somente 7 quentes. Campos do Jordão tem volume médio de 9,22 internações/dia, mas os demais exemplos mostram volumes de 0,59 a 3,30. Os municípios com mais dias quentes incluem São Francisco, Santa Clara d'Oeste, Suzanápolis, Populina e Santa Salete, cada um com 2.063 dias quentes e 273 frios, com volumes médios de 0,38 a 1,09 internações/dia.

Conclusão estritamente descritiva: existe forte assimetria territorial de exposição, e os grupos de maior volume hospitalar estão proporcionalmente mais presentes nos dias frios. Isso explica por que o agregado pode divergir do contraste intramunicipal, mas não identifica mecanismo causal.

## 9. Validação da virada de estação/ano nos lags

A auditoria encontrou que a implementação anterior buscava D−1, D−3 e D−7 usando a estação da linha corrente. Assim, se o município usasse a estação A em 2020 e a B em 2021, a linha de 01/01/2021 poderia procurar indevidamente dados de dezembro na estação B.

A implementação foi corrigida para resolver cada lag por **município + data-alvo** no painel diário já construído. Dessa forma, 01/01/2021 usa para D−1 a temperatura municipal de 31/12/2020, que já obedece à estação atribuída em 2020. O mesmo vale para 29/12/2020 em D−3 e 25/12/2020 em D−7. Somente as datas técnicas anteriores ao início analítico — os últimos sete dias de 2014 — usam a estação do primeiro ano para dar suporte ao começo de 2015.

O teste sintético define estação A em 2020 e B em 2021, injeta valores falsos deliberados para a estação B em dezembro e confirma que 01/01/2021 recebe **1,0, 3,0 e 7,0** de A em D−1, D−3 e D−7. Também valida isolamento entre municípios, data exata em série esparsa e ano bissexto.

Na base publicada, o DQ encontrou **zero divergências** nos três lags. Os resultados agregados corrigidos são:

| Defasagem | Média frio | Média quente | Diferença relativa |
|---|---:|---:|---:|
| D0 | 14,5637 | 6,6793 | +118,04% |
| D−1 | 14,5915 | 6,6497 | +119,43% |
| D−3 | 14,7242 | 6,5114 | +126,13% |
| D−7 | 14,7346 | 6,5640 | +124,48% |

Esses números são associações descritivas defasadas. Não demonstram temporalidade causal nem controlam tendências, sazonalidade ou composição municipal.

## 10. Correções implementadas

1. **Lags por município e data-alvo:** substituição do lookup baseado na estação corrente pelo painel município-dia, preservando a regra de estação válida na data passada.
2. **DQ dos lags:** a validação passou a reproduzir a mesma semântica temporal e compara D−1, D−3 e D−7 com a data municipal exata.
3. **Rótulo de sexo:** inclusão de `sexo_descricao` na fato CID, mantendo `sexo` original. O mapeamento é `1 = Masculino`, `3 = Feminino`, `0 = Ignorado`; qualquer valor não documentado recebe `Não mapeado`, sem inferência.
4. **Modelo semântico:** exposição de `Dados.sexo_descricao` no TMDL e no Power Query da tabela CID, sem alteração de páginas ou visuais.
5. **Auditoria reproduzível:** criação de `src/validation/metric_audit.py` e `src/validation/metric_audit_summary.py`, com publicação atômica de 21 conjuntos de evidências em `data/validation/auditoria_metricas`.
6. **Testes analíticos:** criação de `tests/test_analytic_metrics.py` e ampliação do teste de lags e do teste de domínio de sexo.

O mapeamento de sexo foi baseado no manual de auditoria hospitalar do Ministério da Saúde/DATASUS, que documenta `0 = ignorado`, `1 = masculino` e `3 = feminino`, e foi corroborado pelo manual operacional HOSPUB/SAME. Fontes: [Manual de Auditoria Hospitalar no SUS](https://bvsms.saude.gov.br/bvs/publicacoes/auditoria_sus2004_2.pdf) e [Manual de Operação HOSPUB/SAME](https://bvsms.saude.gov.br/bvs/publicacoes/pdoc/MNL-GPSL-PDOC-HOSPUB-SAME-ManualOperacao-Edicao1.0.pdf). Na base analítica 2015–2025 foram observados somente os códigos 1 e 3.

## 11. Testes e validações executados

| Validação | Resultado |
|---|---:|
| Testes da orquestração Bronze | 5/5 aprovados |
| Testes de métricas da fato de internação | 6/6 aprovados |
| Testes de persistência meteorológica | 3/3 aprovados |
| Testes históricos INMET | 3/3 aprovados |
| Testes da camada Consumption | 11/11 aprovados |
| Testes específicos da auditoria analítica | 4/4 aprovados |
| **Total** | **32/32 aprovados** |
| Validação AST | 66 arquivos sem erro sintático |
| DQ da camada Consumption | `STATUS FINAL: PASS` |
| Lags D−1/D−3/D−7 na base integral | 0 divergências em cada lag |
| Reconciliação anual Gold × fatos | 11/11 anos idênticos |
| Reconciliação municipal | 0 municípios divergentes |
| Domínio código/rótulo de sexo | 0 registros inválidos |

A camada Consumption foi reconstruída antes da validação. Continuaram preservados 2.591.610 município-dias, o período completo de 2015–2025 e 27.671.456 internações reconciliadas com a Gold. A revisão local do modelo semântico confirmou a presença simultânea do código e do rótulo de sexo no schema TMDL e na expansão Power Query. Não houve modificação do layout do relatório.

## 12. Recomendação técnica para as seis perguntas de pesquisa

### Pergunta 1 — Como as internações variam entre faixas de temperatura?

Usar a média agregada por município-dia como descrição do conjunto estadual, mas apresentá-la junto da distribuição intramunicipal e do alerta de composição. Se a formulação disser “em um município típico”, a mediana intramunicipal e o intervalo interquartil devem ser a evidência principal. Não interpretar o agregado como risco individual ou populacional.

### Pergunta 2 — O contraste frio versus quente é consistente ao longo dos anos?

Usar como principal a tabela anual intramunicipal: mediana, Q1, Q3, quantidade de municípios válidos e percentuais por sinal. Manter o agregado anual como medida secundária do volume de município-dias. A heterogeneidade de 2020 e 2025 precisa permanecer explícita; não resumir 2015–2025 com um único sinal.

### Pergunta 3 — Quais capítulos, grupos e categorias CID apresentam maiores contrastes?

Para o ranking estadual, usar numerador esparso dividido por todos os município-dias expostos, nunca por dias com evento. Para interpretação substantiva, combinar o ranking com mediana e IQR intramunicipais dos alvos relevantes. Dar preferência a alvos com numerador suficiente e evitar ordenar categorias raras somente pela diferença relativa.

### Pergunta 4 — Como os resultados diferem por idade e sexo?

Manter o denominador global de município-dias de exposição para cada estrato e preservar os zeros implícitos. Exibir `sexo_descricao` ao usuário e conservar `sexo` para rastreabilidade. Como não há denominadores populacionais por idade/sexo, os resultados descrevem contagens de internação por município-dia; não devem ser chamados de taxas de risco ou incidência dos grupos.

### Pergunta 5 — Há associação com temperaturas defasadas em D−1, D−3 e D−7?

Usar os lags corrigidos por município e data-alvo. Comparar D0, D−1, D−3 e D−7 como análise exploratória, informando as amostras válidas. O contraste agregado sofre o mesmo risco de composição; para uma versão metodologicamente mais forte da pergunta, o resumo intramunicipal por lag deve ser a medida principal em uma etapa posterior. Não descrever a defasagem como efeito retardado causal.

### Pergunta 6 — Quais municípios apresentam maior contraste?

Usar diretamente as métricas intramunicipais, com o filtro mínimo de 30 dias em cada condição e exibição conjunta de dias frios, dias quentes, médias e volume total. Separar contraste relativo de volume absoluto para evitar que pequenas bases sejam confundidas com maior importância assistencial. Para síntese estadual, usar mediana e IQR, não média ponderada por volume.

## Limitações preservadas

- Estudo ecológico e descritivo; não identifica causalidade.
- Ausência de denominadores populacionais e de padronização por idade/sexo.
- Internações refletem oferta, referência regional e capacidade hospitalar, além da demanda.
- A estação é uma aproximação municipal selecionada por município-ano.
- Não houve controle de sazonalidade, tendência, autocorrelação ou confundidores.
- Os thresholds `< 20 °C` e `>= 25 °C` foram mantidos exatamente como definidos antes da auditoria.

## Artefatos reproduzíveis

- Rotina principal: `src/validation/metric_audit.py`
- Extrato de conferência: `src/validation/metric_audit_summary.py`
- Testes: `tests/test_analytic_metrics.py` e `tests/test_consumption.py`
- Evidências Parquet: `data/validation/auditoria_metricas`
- Evidências das perguntas: `data/validation/perguntas_pesquisa`
- Relatório: `docs/Relatorio_Auditoria_Metricas_Analiticas.md`
