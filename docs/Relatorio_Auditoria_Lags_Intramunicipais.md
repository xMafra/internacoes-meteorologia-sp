# Relatório de Auditoria dos Lags Intramunicipais

**Pergunta de pesquisa:** a temperatura de dias anteriores apresenta associação diferente daquela observada com a temperatura do próprio dia?  
**Período:** 01/01/2015 a 31/12/2025  
**Defasagens:** D0, D−1, D−3 e D−7  
**Data da auditoria:** 01/10/2026

## Conclusão executiva

A análise estadual agregada foi substituída, para esta comparação metodológica, por contrastes calculados dentro de cada município. Cada município válido contribui uma vez para o resumo de cada lag, impedindo que municípios com maior volume absoluto dominem a estatística.

Nas internações totais, a mediana da diferença relativa é negativa em D0 (**−1,89%**) e D−1 (**−1,51%**) e positiva, embora pequena, em D−3 (**+1,75%**) e D−7 (**+1,80%**). O percentual com contraste positivo passa de 39,22% em D0 para 61,41% em D−3 e D−7.

Nos recortes respiratórios, as medianas são positivas em todos os lags. Para o Capítulo X, passam de +17,95% em D0 para +26,11% em D−7; para influenza/pneumonia, de +22,66% para +33,79%. J20–J22 apresenta contrastes elevados em todos os lags, mas sem progressão monotônica. Esses resultados mostram diferenças descritivas entre as associações por defasagem; não demonstram efeito retardado nem relação causal.

## 1. Definição das métricas

Para cada município \(m\) e defasagem \(l\), a temperatura usada para classificar o dia de internação é:

- D0: temperatura do próprio dia;
- D−1: temperatura municipal atribuída ao dia anterior;
- D−3: temperatura municipal atribuída a três dias antes;
- D−7: temperatura municipal atribuída a sete dias antes.

Os thresholds foram preservados:

- frio: temperatura do lag menor que 20 °C;
- quente: temperatura do lag maior ou igual a 25 °C.

Para cada combinação município-lag:

\[
\bar{Y}_{frio,m,l} =
\frac{\sum_{i\in m}Y_i\mathbb{1}(T_{i,l}<20)}
{\sum_{i\in m}\mathbb{1}(T_{i,l}<20)}
\]

\[
\bar{Y}_{quente,m,l} =
\frac{\sum_{i\in m}Y_i\mathbb{1}(T_{i,l}\ge25)}
{\sum_{i\in m}\mathbb{1}(T_{i,l}\ge25)}
\]

\[
\Delta_{abs,m,l}=\bar{Y}_{frio,m,l}-\bar{Y}_{quente,m,l}
\]

\[
\Delta_{rel,m,l}=
\frac{\bar{Y}_{frio,m,l}-\bar{Y}_{quente,m,l}}
{\bar{Y}_{quente,m,l}}
\]

A elegibilidade é recalculada independentemente em cada lag: pelo menos 30 dias frios e 30 dias quentes. Para entrar no resumo da diferença relativa, a média quente também precisa ser diferente de zero. Nas internações totais, todos os 640 municípios elegíveis possuem diferença relativa válida. Nos grupos CID esparsos, alguns municípios elegíveis não têm internação quente do alvo e, portanto, permanecem no detalhe com diferença relativa nula, mas não entram no resumo relativo.

“Positivo” e “negativo” significam estritamente diferença relativa maior e menor que zero. “Aproximadamente zero” é a faixa paralela \(|\Delta_{rel}|\le1\%\), igual à auditoria analítica anterior.

## 2. Internações totais — resumo intramunicipal

| Lag | Municípios válidos | Média | Q1 | Mediana | Q3 | Mínimo | Máximo | Positivos | Negativos | Aprox. zero |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| D0 | 640 | −1,85% | −6,29% | −1,89% | +2,05% | −33,26% | +28,35% | 39,22% | 60,78% | 10,31% |
| D−1 | 640 | −1,09% | −5,12% | −1,51% | +2,91% | −36,30% | +40,55% | 40,63% | 59,38% | 11,88% |
| D−3 | 640 | +1,38% | −2,30% | +1,75% | +5,11% | −27,76% | +30,30% | 61,41% | 38,59% | 13,75% |
| D−7 | 640 | +1,95% | −2,16% | +1,80% | +5,77% | −28,96% | +29,04% | 61,41% | 38,59% | 13,91% |

O deslocamento do centro da distribuição ocorre entre D−1 e D−3. Entretanto, as medianas de D−3 e D−7 são próximas de zero em magnitude absoluta e os intervalos interquartis continuam atravessando zero. A comparação evidencia mudança de distribuição, mas não permite afirmar que a temperatura anterior produz as internações observadas.

## 3. Capítulo X — doenças do aparelho respiratório

| Lag | Municípios válidos | Média | Q1 | Mediana | Q3 | Mínimo | Máximo | Positivos | Negativos | Aprox. zero |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| D0 | 640 | +19,33% | +5,84% | +17,95% | +30,97% | −42,61% | +124,02% | 84,38% | 15,63% | 2,81% |
| D−1 | 640 | +19,93% | +6,61% | +18,03% | +32,33% | −49,06% | +128,15% | 85,16% | 14,84% | 2,97% |
| D−3 | 640 | +24,12% | +11,52% | +22,21% | +35,42% | −57,23% | +123,53% | 90,16% | 9,84% | 1,72% |
| D−7 | 640 | +27,30% | +14,83% | +26,11% | +39,12% | −44,80% | +153,13% | 91,56% | 8,44% | 0,78% |

O centro e a maior parte da distribuição deslocam-se para valores mais positivos em D−3 e D−7. O padrão é descritivamente mais pronunciado do que nas internações totais.

## 4. J09–J18 — influenza e pneumonia

| Lag | Municípios válidos | Média | Q1 | Mediana | Q3 | Mínimo | Máximo | Positivos | Negativos | Aprox. zero |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| D0 | 640 | +25,08% | +6,28% | +22,66% | +39,43% | −58,10% | +177,80% | 82,81% | 17,19% | 3,13% |
| D−1 | 639 | +26,27% | +8,29% | +23,56% | +41,40% | −60,56% | +178,85% | 85,29% | 14,71% | 2,97% |
| D−3 | 640 | +32,63% | +13,10% | +28,95% | +45,64% | −65,65% | +472,52% | 88,91% | 11,09% | 0,94% |
| D−7 | 640 | +35,67% | +17,05% | +33,79% | +51,10% | −69,98% | +285,19% | 90,47% | 9,53% | 1,41% |

O padrão é semelhante ao Capítulo X: D−3 e D−7 apresentam medianas e quartis superiores aos de D0 e D−1. O máximo de D−3 é elevado, mas não determina a mediana; por isso o intervalo interquartil é a referência principal.

## 5. J20–J22 — outras infecções agudas das vias aéreas inferiores

| Lag | Municípios válidos | Média | Q1 | Mediana | Q3 | Mínimo | Máximo | Positivos | Negativos | Aprox. zero |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| D0 | 592 | +88,37% | +1,90% | +53,89% | +136,45% | −100,00% | +3.013,05% | 75,68% | 24,32% | 0,68% |
| D−1 | 594 | +82,68% | +7,31% | +57,62% | +113,23% | −100,00% | +1.033,52% | 78,79% | 21,21% | 0,84% |
| D−3 | 601 | +88,62% | +4,88% | +47,98% | +126,66% | −100,00% | +1.748,43% | 76,87% | 23,13% | 0,33% |
| D−7 | 597 | +101,23% | +12,54% | +56,64% | +136,14% | −100,00% | +2.922,71% | 79,56% | 20,44% | 0,50% |

Esse grupo é mais esparso e apresenta grande dispersão. As medianas são altas em todos os lags, mas D−3 é inferior a D0, D−1 e D−7. Assim, não há progressão monotônica compatível com a observada no Capítulo X e em J09–J18. Os máximos extremos refletem médias quentes pequenas em alguns municípios e não devem ser usados isoladamente.

## 6. Validade dos denominadores respiratórios

Para cada município, alvo e lag, o denominador é o número de dias do município classificados como frios ou quentes pela temperatura daquele lag. Dias sem internação do alvo CID entram com zero no numerador e permanecem no denominador.

A implementação não materializa todas as combinações município-dia-CID. Ela calcula:

1. denominadores no painel completo `fato_municipio_dia`;
2. numeradores na `fato_municipio_dia_cid` esparsa;
3. produto município × alvo para garantir alvos com zero evento;
4. junção dos numeradores e preenchimento apenas dos numeradores ausentes com zero.

O teste sintético usa 30 dias frios e 30 quentes, com influenza/pneumonia em apenas dois dias de cada condição. As médias obtidas são 5/30 no frio e 4/30 no quente, e não 5/2 e 4/2.

## 7. Elegibilidade independente por lag

Os dias frios e quentes são contados separadamente para cada coluna de temperatura. Um município pode, portanto, ser elegível em um lag e inelegível em outro. Um teste específico cria:

- D0 com 30 dias frios e 30 quentes: elegível;
- D−1 com 29 dias frios e 31 quentes: inelegível.

Na base integral, 640 municípios atingiram o mínimo de exposição nos quatro lags. A variação de municípios válidos nos grupos respiratórios decorre de média quente igual a zero para o alvo, não de alteração do threshold de exposição.

## 8. Materialização para uso posterior no Power BI

Os resultados foram publicados em `data/validation/auditoria_lags_intramunicipais`:

| Dataset | Granularidade | Linhas esperadas |
|---|---|---:|
| `contraste_total_municipio_lag` | município × lag | 2.580 |
| `resumo_total_lag` | lag | 4 |
| `contraste_respiratorio_municipio_lag` | município × alvo × lag | 7.740 |
| `resumo_respiratorio_lag` | alvo × lag | 12 |

Os detalhes contêm município, lag, internações frias/quentes, dias frios/quentes, médias municipais, diferenças absoluta e relativa e indicadores de elegibilidade/validade. Os resumos contêm todas as estatísticas solicitadas. Não foram criadas medidas ou páginas no Power BI nesta etapa e nenhum visual foi redesenhado.

## 9. Testes e Data Quality

Foram executados seis testes analíticos e onze testes da camada Consumption, todos aprovados. Dois testes são específicos desta extensão:

1. elegibilidade independente para D0, D−1, D−3 e D−7;
2. preservação dos dias sem evento no denominador respiratório municipal.

O DQ específico terminou com `STATUS FINAL: PASS` e verificou:

- schemas dos detalhes e resumos;
- domínio exato dos quatro lags e três alvos;
- unicidade das chaves;
- painel completo município × lag;
- painel completo município × alvo × lag;
- elegibilidade independente com mínimo 30/30;
- validade da diferença relativa;
- quatro linhas no resumo total e doze no respiratório;
- reconciliação dos municípios válidos entre detalhe e resumo.

## 10. Resposta metodológica à Pergunta 5

Sem ponderação por volume municipal, a associação observada muda entre os lags:

- nas internações totais, D0 e D−1 têm mediana ligeiramente negativa; D−3 e D−7 têm mediana ligeiramente positiva;
- no Capítulo X e em influenza/pneumonia, D−3 e principalmente D−7 apresentam distribuições mais positivas do que D0 e D−1;
- em J20–J22, as associações são positivas e dispersas em todos os lags, sem ordenação consistente.

A evidência principal recomendada para a Pergunta 5 passa a ser a mediana intramunicipal por lag, acompanhada por Q1, Q3, quantidade de municípios válidos e percentuais por sinal. O agregado estadual deve permanecer apenas como evidência descritiva secundária.

Essas comparações não controlam sazonalidade, tendência, autocorrelação, circulação viral ou outros confundidores; os lags também são correlacionados entre si. Consequentemente, não é possível concluir que um lag seja causalmente superior ou identificar um período de latência biológica a partir desta análise.

## Artefatos

- Geração: `src/validation/lag_intramunicipal_audit.py`
- DQ: `src/quality/consumption/dq_lag_intramunicipal.py`
- Testes: `tests/test_analytic_metrics.py`
- Dados: `data/validation/auditoria_lags_intramunicipais`
- Relatório: `docs/Relatorio_Auditoria_Lags_Intramunicipais.md`
