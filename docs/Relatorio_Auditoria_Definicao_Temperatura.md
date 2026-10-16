# Auditoria da definição térmica — limites absolutos, percentis municipais e percentis mensais

Etapa 1B • 05/10/2026 • evidência local • 2015–2025 • todas as saídas novas são experimentais. A metodologia oficial e o Power BI foram preservados.


## 1. Motivação

A comparação intramunicipal evita que uma grande cidade domine a mediana estadual, mas não torna os limites 20/25 °C equivalentes climaticamente. A mesma temperatura pode ocupar posições diferentes nas distribuições locais. Esta auditoria compara cobertura, distribuição de exposições e sensibilidade dos contrastes, sem procurar um método que produza um resultado previamente desejado.


## 2. Método absoluto atual

A: frio T<20 °C; quente T≥25 °C; intermediário 20≤T<25 °C. O intervalo é contínuo: 24,99 °C continua intermediário. Elegibilidade: ≥30 dias em cada condição. O método descreve exposições em uma escala física comum; não garante que os dias tenham a mesma excepcionalidade local.


Não há município sem dias frios nem sem dias quentes no período completo. Há 0 com menos de 30 dias frios e 5 com menos de 30 dias quentes. 'Poucos' é mostrado pela distribuição e pelo critério já existente de 30 dias; não foi inventado novo corte.


| Distribuição entre municípios | Mínimo | P05 | P10 | P25 | Mediana | P75 | P90 | P95 | Máximo |
|---|---|---|---|---|---|---|---|---|---|
| % dias frios | 6,98 | 6,98 | 10,47 | 12,41 | 20,52 | 32,08 | 45,21 | 50,17 | 95,94 |
| % dias quentes | 0,18 | 5,03 | 7,34 | 14,19 | 25,41 | 41,13 | 46,21 | 52,78 | 52,78 |
| Dias frios | 273,00 | 273,00 | 414,00 | 484,00 | 818,00 | 1.227,00 | 1.779,00 | 1.967,40 | 3.762,00 |
| Dias quentes | 7,00 | 196,60 | 292,00 | 547,00 | 1.003,00 | 1.609,00 | 1.828,00 | 2.063,00 | 2.063,00 |
| Equilíbrio min(F,Q)/max(F,Q) | 0,00 | 0,11 | 0,13 | 0,22 | 0,34 | 0,65 | 0,84 | 0,88 | 0,98 |

![01_dias_frios_absolutos](../data/validation/auditoria_definicao_temperatura/figuras/01_dias_frios_absolutos.png)

![02_dias_quentes_absolutos](../data/validation/auditoria_definicao_temperatura/figuras/02_dias_quentes_absolutos.png)

## 3. Método P20/P80 municipal e implementação

B: percentis da temperatura média diária válida de cada município no período 01/01/2015–31/12/2025. Frio T≤P20; quente T≥P80. Empates são incluídos integralmente, sem amostragem ou exclusão aleatória. A faixa restante é intermediária. A temperatura provém da série estação–município já atribuída no painel; nenhuma temperatura foi inferida das internações. Os sete dias técnicos de 2014 não entram nos percentis.


Os thresholds usam `pyspark.sql.functions.percentile`, exato e nativo Spark 3.5, com interpolação linear na posição (N−1)p. Não foi usado `percentile_approx` para os thresholds. O número limitado de dias por município torna o cálculo exato viável. Os resumos de internações reutilizam `percentile_approx(...,10000)` da Etapa 1 para preservar sua convenção empírica; erro de rank declarado de 1/10000, sem adicionar nada ao numerador ou denominador. Threshold térmico interpolado e mediana empírica das diferenças são operações distintas. [Documentação Spark: percentil exato](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile.html), [implementação da interpolação](https://github.com/apache/spark/blob/v3.5.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/percentiles.scala), [percentil aproximado](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html).


Foram observados 0 municípios e 0 estratos município×mês com P20=P80. A política explícita para um estrato degenerado é classificá-lo como intermediário, marcar o flag e não formar contrastes frio/quente nesse estrato. Os dias continuam contabilizados na cobertura; não se permite dupla classificação. O teste sintético de ausência de variação cobre essa exceção. Nos dados reais ela não foi necessária.


Fonte: 2.591.610 município-dias; 2.529.031 com temperatura válida; 62.579 sem temperatura válida (2,41%). Validade exige temperatura conhecida e finita e registro meteorológico disponível no painel. Não foi criada exigência de 24 observações horárias. Os totais de 27.671.456 internações abrangem todo o período, inclusive dias sem meteorologia; os numeradores térmicos incluem somente dias da respectiva condição. Ausência de temperatura nunca vira zero internação.


## 4. Cobertura e elegibilidade


| Método | Municípios elegíveis | Não elegíveis | Dias frios | Dias quentes | Intermediários |
|---|---|---|---|---|---|
| Absoluto <20 / ≥25 °C | 640 | 5 | 616.741 | 675.345 | 1.236.945 |
| P20/P80 municipal | 645 | 0 | 506.971 | 507.444 | 1.514.616 |
| P20/P80 município × mês | 645 | 0 | 510.551 | 510.642 | 1.507.838 |

B recupera 5 municípios e perde 0. A cobertura absoluta já é 99,22% dos municípios; o benefício de cobertura de B é pequeno em número, embora a comparabilidade da fração de exposição mude. Os recuperados são:


| Município | Motivo no método A |
|---|---|
| Potim | MENOS_30_DIAS_QUENTES |
| Campos do Jordão | MENOS_30_DIAS_QUENTES |
| Monteiro Lobato | MENOS_30_DIAS_QUENTES |
| São Bento do Sapucaí | MENOS_30_DIAS_QUENTES |
| Santo Antônio do Pinhal | MENOS_30_DIAS_QUENTES |

| Método | % frio: mínimo/mediana/máximo | % quente: mínimo/mediana/máximo | Equilíbrio: mínimo/mediana/máximo |
|---|---|---|---|
| Absoluto <20 / ≥25 °C | 6,98 / 20,52 / 95,94 | 0,18 / 25,41 / 52,78 | 0,00 / 0,34 / 0,98 |
| P20/P80 municipal | 20,00 / 20,05 / 20,18 | 20,00 / 20,06 / 20,25 | 0,99 / 1,00 / 1,00 |
| P20/P80 município × mês | 20,07 / 20,19 / 20,34 | 20,09 / 20,18 / 20,31 | 0,99 / 1,00 / 1,00 |

A diferença em relação a exatamente 20% está publicada por município. Em B, os intervalos observados para frio e quente na tabela acima incluem os empates nos limites. C também inclui os empates por mês; pequenas diferenças na quantidade de dias de cada mês e no tamanho amostral impedem exigir exatamente 20%. `dias_empatados_p20` e `dias_empatados_p80` permitem reproduzir os casos, sem eliminar observações.


## 5. Distribuição térmica municipal


| Variável | Mínimo | P05 | P10 | P25 | Mediana | P75 | P90 | P95 | Máximo |
|---|---|---|---|---|---|---|---|---|---|
| p20 | 12,40 | 16,81 | 17,26 | 18,55 | 19,91 | 21,26 | 21,79 | 22,75 | 22,75 |
| p80 | 17,88 | 23,06 | 23,25 | 24,30 | 25,40 | 26,69 | 27,09 | 27,58 | 27,58 |
| amplitude_p20_p80 | 4,40 | 4,83 | 4,83 | 5,32 | 5,54 | 5,86 | 6,39 | 6,60 | 8,24 |
| temperatura_media | 15,22 | 19,84 | 20,14 | 21,46 | 22,69 | 24,05 | 24,27 | 25,08 | 25,08 |
| dias_temperatura_valida | 3.733,00 | 3.825,00 | 3.842,00 | 3.890,00 | 3.915,00 | 3.976,00 | 3.994,00 | 4.014,00 | 4.018,00 |

A tabela municipal completa também fornece mínimo, P05/P10/P20/P25, mediana, P75/P80/P90/P95, máximo, média, desvio-padrão amostral, nome, coordenadas e número de estações distintas válidas. Nenhuma estação foi reassociada usando a referência de 2025 para anos anteriores. Mudanças históricas de estação continuam nas séries e limitam sua interpretação como climatologia medida exatamente em cada município. Municípios atendidos pela mesma estação podem compartilhar a série meteorológica.


![04_percentis_municipais](../data/validation/auditoria_definicao_temperatura/figuras/04_percentis_municipais.png)

Exemplos escolhidos por ordenação crescente da média térmica: primeira posição, posição central e última posição; desempate pelo código IBGE. A escolha não usa internações ou a direção do resultado.


| Perfil | Município | Média °C | P20 | P80 | % frio A | % quente A | Dias F/Q A | Dias F/Q B |
|---|---|---|---|---|---|---|---|---|
| MENOR_MEDIA | Campos do Jordão | 15,22 | 12,40 | 17,88 | 95,94 | 0,18 | 3.762/7 | 788/787 |
| MEDIANA_DAS_MEDIAS | Gavião Peixoto | 22,69 | 19,88 | 25,38 | 20,54 | 25,21 | 818/1.004 | 797/801 |
| MAIOR_MEDIA | Três Fronteiras | 25,08 | 22,75 | 27,58 | 6,98 | 52,78 | 273/2.063 | 785/786 |

## 6. Concordância entre classificações


| A \ B | Frio relativo | Intermediário relativo | Quente relativo |
|---|---|---|---|
| FRIO | 422.556 | 191.045 | 3.140 |
| INTERMEDIARIO | 84.415 | 1.070.280 | 82.250 |
| QUENTE | 0 | 253.291 | 422.054 |

Concordância de rótulos: 1.914.890 de 2.529.031 dias válidos (75,72%). Dos intermediários absolutos, 84.415 tornam-se relativamente frios e 82.250 relativamente quentes. Concordância não é acurácia: nenhum dos métodos é uma classificação verdadeira usada como referência.


| Caso | Município | Data | Temperatura °C | P20/P80 | A | B |
|---|---|---|---|---|---|---|
| INTERMEDIARIO_ABSOLUTO_FRIO_RELATIVO | Adamantina | 2020-06-22 | 21,00 | 21,43/26,93 | INTERMEDIARIO | FRIO |
| INTERMEDIARIO_ABSOLUTO_FRIO_RELATIVO | Adamantina | 2020-08-25 | 21,00 | 21,43/26,93 | INTERMEDIARIO | FRIO |
| INTERMEDIARIO_ABSOLUTO_FRIO_RELATIVO | Adolfo | 2025-10-19 | 21,00 | 21,26/26,90 | INTERMEDIARIO | FRIO |
| INTERMEDIARIO_ABSOLUTO_QUENTE_RELATIVO | Angatuba | 2015-12-13 | 24,00 | 17,00/23,34 | INTERMEDIARIO | QUENTE |
| INTERMEDIARIO_ABSOLUTO_QUENTE_RELATIVO | Angatuba | 2020-09-12 | 24,00 | 17,00/23,34 | INTERMEDIARIO | QUENTE |
| INTERMEDIARIO_ABSOLUTO_QUENTE_RELATIVO | Angatuba | 2024-10-08 | 24,00 | 17,00/23,34 | INTERMEDIARIO | QUENTE |
| FRIO_ABSOLUTO_COMUM_OU_QUENTE_RELATIVO | Águas de Lindóia | 2015-09-12 | 19,00 | 18,56/24,34 | FRIO | INTERMEDIARIO |
| FRIO_ABSOLUTO_COMUM_OU_QUENTE_RELATIVO | Águas de Lindóia | 2019-06-21 | 19,00 | 18,56/24,34 | FRIO | INTERMEDIARIO |
| FRIO_ABSOLUTO_COMUM_OU_QUENTE_RELATIVO | Águas de Lindóia | 2020-02-22 | 19,00 | 18,56/24,34 | FRIO | INTERMEDIARIO |

Os exemplos são as observações existentes mais próximas de 21, 24 e 19 °C nos três tipos de discordância pedidos, com desempate por município/data. Temperaturas mostradas são reais; não são valores substituídos para se ajustar ao exemplo.


## 7. Resultados gerais

A unidade continua municipal: Fm=eventos frios/dias frios; Qm=eventos quentes/dias quentes; Dm=Fm−Qm; D100m=100Dm. Somente depois são calculadas medianas e quartis dos municípios elegíveis. A reescala não é taxa populacional. Rm=Dm/Qm se Qm>0, senão NULL. Fm=Qm=0 produz D=0, com flags, e não um percentual artificial.


| Métrica | A — absoluto | B — municipal | C — municipal × mês |
|---|---|---|---|
| Municípios válidos absolutos | 640 | 645 | 645 |
| Municípios válidos relativos | 640 | 645 | 645 |
| Mediana da média fria | 2,6330 | 2,6514 | 2,5842 |
| Mediana da média quente | 2,6901 | 2,6667 | 2,6759 |
| Mediana D por município-dia | -0,044000 | -0,038619 | -0,103190 |
| Mediana D100 | -4,4000 | -3,8619 | -10,3190 |
| Q1 de D100 | -20,6902 | -19,2922 | -39,1196 |
| Q3 de D100 | 4,3726 | 4,8750 | -0,2522 |
| Mediana R (%) | -1,8928 | -1,4021 | -4,9341 |
| % municípios frio — consistência | 34,06 | 33,49 | 20,93 |
| % municípios quente — consistência | 55,62 | 54,11 | 71,63 |
| % diferença pequena — consistência | 10,31 | 12,40 | 7,44 |
| Internações totais no período | 27.671.456 | 27.671.456 | 27.671.456 |

A direção da mediana usa D absoluto. A consistência conserva as classes exclusivas FRIO (>+1%), QUENTE (<−1%) e PEQUENA (−1% a +1%, inclusive) na base com R definido. Há também contagens de sinais absolutos estritos. As bases estão separadas; a mediana das diferenças não é a diferença das medianas das médias.


Na mesma base de municípios elegíveis em A e B (N=640), D100 é -4,4000 em A e -3,8652 em B. Isso permite avaliar a classificação sem atribuir toda a diferença aos cinco municípios recuperados.


A conclusão de contraste mediano geral pequeno em A/B permanece descritivamente compatível com os dados: R mediano passa de -1,89% para -1,40%, e as medianas D100 permanecem dentro dos IQRs recíprocos, atravessados por zero. Não foi definido limiar clínico ou de relevância substantiva. Em C, R é -4,93% e a magnitude D100 é 2,35 vezes a de A: a direção permanece, mas não cabe estender a mesma magnitude às três perguntas.


![03_comparacao_municipal](../data/validation/auditoria_definicao_temperatura/figuras/03_comparacao_municipal.png)

## 8. Resultados anuais

Os percentis permanecem fixos no período 2015–2025; não são recalibrados a cada ano. A elegibilidade de 30 dias em cada condição é reavaliada anualmente. Foram publicados os resumos, suas bases anuais e a comparação usando somente municípios elegíveis em ambos no respectivo ano.


| Ano | D100 A | D100 B | D100 C | Direção A/B | N absoluto A/B/C | Robustez A/B |
|---|---|---|---|---|---|---|
| 2015 | -3,613 | -0,615 | -12,469 | QUENTE/QUENTE | 509/603/507 | ROBUSTO_DESCRITIVO |
| 2016 | 3,535 | 0,296 | -9,053 | FRIO/FRIO | 521/565/562 | ROBUSTO_DESCRITIVO |
| 2017 | -2,189 | -0,529 | -14,196 | QUENTE/QUENTE | 443/568/489 | ROBUSTO_DESCRITIVO |
| 2018 | -1,818 | -7,724 | -13,567 | QUENTE/QUENTE | 410/575/579 | ROBUSTO_DESCRITIVO |
| 2019 | 1,400 | -1,744 | -8,777 | FRIO/QUENTE | 444/634/557 | SENSIVEL_AO_METODO |
| 2020 | -33,070 | -37,658 | -19,006 | QUENTE/QUENTE | 393/570/589 | ROBUSTO_DESCRITIVO |
| 2021 | -5,069 | -7,218 | -10,680 | QUENTE/QUENTE | 298/464/426 | ROBUSTO_DESCRITIVO |
| 2022 | -0,579 | 1,299 | -6,993 | QUENTE/FRIO | 392/500/581 | SENSIVEL_AO_METODO |
| 2023 | -5,714 | -4,934 | -23,035 | QUENTE/QUENTE | 555/629/590 | ROBUSTO_DESCRITIVO |
| 2024 | -13,541 | -15,171 | -29,101 | QUENTE/QUENTE | 454/588/582 | ROBUSTO_DESCRITIVO |
| 2025 | 18,701 | 30,721 | 27,454 | FRIO/FRIO | 535/550/520 | ROBUSTO_DESCRITIVO |

Anos com direção diferente entre A e B: 2019, 2022. Magnitudes, consistência e elegibilidade completas estão nas saídas anuais; o gráfico não substitui essas bases.


![07_resultados_anuais](../data/validation/auditoria_definicao_temperatura/figuras/07_resultados_anuais.png)

Estabilidade da exposição de B ao longo dos anos, usando o mesmo baseline:

| Ano | % frio B | % quente B | Municípios elegíveis B |
|---|---|---|---|
| 2015 | 20,61 | 17,42 | 603 |
| 2016 | 26,56 | 16,90 | 565 |
| 2017 | 21,94 | 16,90 | 568 |
| 2018 | 18,99 | 14,62 | 575 |
| 2019 | 17,19 | 22,30 | 634 |
| 2020 | 17,19 | 20,94 | 570 |
| 2021 | 19,66 | 25,61 | 464 |
| 2022 | 22,64 | 17,56 | 500 |
| 2023 | 21,53 | 20,31 | 629 |
| 2024 | 12,76 | 26,60 | 588 |
| 2025 | 21,26 | 21,59 | 550 |

Extremos fixos do período completo podem variar de frequência entre anos. A comparação não separa tendências temporais, mudanças na cobertura meteorológica ou a pandemia de outros padrões associados às temperaturas.


## 9. Diagnósticos CID e ranking

A e B foram calculados para todos os 2.077 capítulos/grupos/categorias da dimensão, sem excluir raros. As 20 categorias com mais internações no período foram selecionadas para leitura detalhada por contagem e desempate pelo código, independentemente do resultado térmico. C cobre todos os capítulos e os dois grupos respiratórios prioritários.


| Diagnóstico | D100 A | D100 B | D100 C | R A (%) | R B (%) | % frio A/B | N absoluto A/B | Robustez A/B |
|---|---|---|---|---|---|---|---|---|
| 10 — Capítulo X - Doenças do aparelho respiratório | 4,3219 | 4,5164 | -0,9330 | 17,950 | 17,500 | 83,12/85,27 | 640/645 | ROBUSTO_DESCRITIVO |
| 15 — Capítulo XV - Gravidez, parto e puerpério | -0,3699 | -0,6273 | -0,2705 | -0,913 | -1,852 | 42,34/38,14 | 640/645 | ROBUSTO_DESCRITIVO |
| 9 — Capítulo IX - Doenças do aparelho circulatório | 0,9769 | 1,3534 | -0,3983 | 3,194 | 4,760 | 57,81/62,95 | 640/645 | ROBUSTO_DESCRITIVO |
| J09-J18 — Influenza [gripe] e pneumonia | 2,7078 | 2,8097 | -0,6305 | 22,655 | 23,254 | 81,09/82,33 | 640/645 | ROBUSTO_DESCRITIVO |
| J20-J22 — Outras infecções agudas das vias aéreas inferiores | 0,4499 | 0,5089 | -0,0009 | 53,890 | 50,769 | 75,34/75,96 | 640/645 | ROBUSTO_DESCRITIVO |
| O80-O84 — Parto | 0,1045 | 0,0753 | fora do escopo | 0,929 | 0,248 | 49,84/47,60 | 640/645 | ROBUSTO_DESCRITIVO |

![05_diagnosticos_principais](../data/validation/auditoria_definicao_temperatura/figuras/05_diagnosticos_principais.png)

### Respiratórias: comparação explícita


| Recorte | Método | Direção | D100 | R (%) | % frio | % quente | % pequena | N absoluto | N relativo |
|---|---|---|---|---|---|---|---|---|---|
| 10 — Capítulo X - Doenças do aparelho respiratório | A | FRIO | 4,3219 | 17,950 | 83,12 | 14,06 | 2,81 | 640 | 640 |
| 10 — Capítulo X - Doenças do aparelho respiratório | B | FRIO | 4,5164 | 17,500 | 85,27 | 12,56 | 2,17 | 645 | 645 |
| 10 — Capítulo X - Doenças do aparelho respiratório | C | QUENTE | -0,9330 | -4,798 | 35,50 | 58,60 | 5,89 | 645 | 645 |
| J09-J18 — Influenza [gripe] e pneumonia | A | FRIO | 2,7078 | 22,655 | 81,09 | 15,78 | 3,12 | 640 | 640 |
| J09-J18 — Influenza [gripe] e pneumonia | B | FRIO | 2,8097 | 23,254 | 82,33 | 14,73 | 2,95 | 645 | 645 |
| J09-J18 — Influenza [gripe] e pneumonia | C | QUENTE | -0,6305 | -5,954 | 35,66 | 59,22 | 5,12 | 645 | 645 |
| J20-J22 — Outras infecções agudas das vias aéreas inferiores | A | FRIO | 0,4499 | 53,890 | 75,34 | 23,99 | 0,68 | 640 | 592 |
| J20-J22 — Outras infecções agudas das vias aéreas inferiores | B | FRIO | 0,5089 | 50,769 | 75,96 | 16,38 | 7,67 | 645 | 574 |
| J20-J22 — Outras infecções agudas das vias aéreas inferiores | C | QUENTE | -0,0009 | -10,000 | 36,64 | 53,94 | 9,42 | 645 | 584 |

**O padrão respiratório persiste em A/B nos três recortes prioritários, mas não em C.** Capítulo X, J09–J18 e J20–J22 têm medianas positivas em A/B e negativas em C. Em J20–J22, D100 de C é −0,000929, quase zero; a classificação de direção usa o valor sem arredondamento. A consistência no frio do Capítulo X passa de 83,13% em A para 85,27% em B e 35,50% em C. Portanto, o achado é robusto descritivamente entre A/B e sensível quando a comparação se restringe à excepcionalidade dentro do mês. Isso não demonstra que a estação explica toda a diferença nem identifica associação causal.


| Nível | Itens | Spearman de ranks | Mesma direção | Direção diferente | Posição diferente | Deslocamento P50/P90/máximo |
|---|---|---|---|---|---|---|
| Capítulo | 22 | 0,9589 | 19 | 3 | 11 | 0,2/2,5/5,0 |
| Grupo | 258 | 0,8403 | 226 | 32 | 252 | 15,5/71,5/194,0 |

O ranking usa D100, do maior contraste no frio para o maior no quente. Empates recebem rank médio. Spearman é a correlação de Pearson desses ranks. Rank pode mudar mesmo com a direção preservada; zeros frequentes e empates também influenciam a correlação. Não foi criado corte arbitrário de deslocamento substancial: são apresentados a distribuição e os maiores deslocamentos observados.


| CID | Rank A | Rank B | Deslocamento | D100 A/B | Direção A/B |
|---|---|---|---|---|---|
| K20-K31 — Doenças do esôfago, do estômago e do duodeno | 217,0 | 23,0 | 194,0 | -0,0644/0,0003 | QUENTE/FRIO |
| N80-N98 — Transtornos não-inflamatórios do trato genital feminino | 198,0 | 19,0 | 179,0 | -0,0386/0,0013 | QUENTE/FRIO |
| K40-K46 — Hérnias | 185,0 | 10,0 | 175,0 | -0,0108/0,1287 | QUENTE/FRIO |
| F20-F29 — Esquizofrenia, transtornos esquizotípicos e transtornos delirantes | 234,5 | 117,0 | 117,5 | -0,1193/0,0000 | QUENTE/IGUAL |
| M70-M79 — Outros transtornos dos tecidos moles | 216,0 | 117,0 | 99,0 | -0,0643/0,0000 | QUENTE/IGUAL |
| C60-C63 — Neoplasias malignas dos órgãos genitais masculinos | 19,0 | 117,0 | 98,0 | 0,0198/0,0000 | FRIO/IGUAL |
| Q10-Q18 — Malformações congênitas do olho, do ouvido, da face e do pescoço | 20,0 | 117,0 | 97,0 | 0,0151/0,0000 | FRIO/IGUAL |
| Z30-Z39 — Pessoas em contato com os serviços de saúde em circunstâncias relacionadas com a reprodução | 212,0 | 117,0 | 95,0 | -0,0614/0,0000 | QUENTE/IGUAL |
| T00-T07 — Traumatismos envolvendo múltiplas regiões do corpo | 211,0 | 117,0 | 94,0 | -0,0603/0,0000 | QUENTE/IGUAL |
| E40-E46 — Desnutrição | 205,0 | 117,0 | 88,0 | -0,0485/0,0000 | QUENTE/IGUAL |
| H65-H75 — Doenças do ouvido médio e da mastóide | 205,0 | 117,0 | 88,0 | -0,0485/0,0000 | QUENTE/IGUAL |
| N70-N77 — Doenças inflamatórias dos órgãos pélvicos femininos | 205,0 | 117,0 | 88,0 | -0,0485/0,0000 | QUENTE/IGUAL |

### Categorias principais selecionadas por volume


| Categoria | Internações | D100 A | D100 B | Direção A/B | Robustez |
|---|---|---|---|---|---|
| O80 — Parto único espontâneo | 1.861.214 | 0,0714 | 0,0492 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| J18 — Pneumonia por microorganismo não especificada | 972.962 | 1,8822 | 2,1411 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| O82 — Parto único por cesariana | 581.408 | -0,0609 | -0,1180 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| K80 — Colelitíase | 494.043 | 0,0475 | 0,1290 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| N39 — Outros transtornos do trato urinário | 442.183 | -0,8983 | -0,8750 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| I50 — Insuficiência cardíaca | 436.051 | 0,5497 | 0,6313 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| B34 — Doenças por vírus, de localização não especificada | 424.915 | 0,6722 | 0,6220 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| I21 — Infarto agudo do miocárdio | 414.632 | 0,1067 | 0,1290 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| S82 — Fratura da perna, incluindo tornozelo | 399.154 | -0,0937 | -0,0008 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| Z30 — Anticoncepção | 391.978 | -0,0867 | 0,0002 | QUENTE/FRIO | SENSIVEL_AO_METODO |
| A41 — Outras septicemias | 378.008 | -0,2826 | -0,2503 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| I64 — Acidente vascular cerebral, não especificado como hemorrágico ou isquêmico | 376.549 | 0,1557 | 0,1285 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| K40 — Hérnia inguinal | 341.076 | -0,0004 | 0,0019 | QUENTE/FRIO | SENSIVEL_AO_METODO |
| S72 — Fratura do fêmur | 296.493 | 0,4191 | 0,5087 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| I83 — Varizes dos membros inferiores | 275.908 | 0,0045 | 0,0005 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| K92 — Outras doenças do aparelho digestivo | 272.560 | 0,0055 | 0,0835 | FRIO/FRIO | ROBUSTO_DESCRITIVO |
| S52 — Fratura do antebraço | 261.938 | -0,2605 | -0,2584 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| S06 — Traumatismo intracraniano | 255.027 | -0,1276 | -0,1266 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| I20 — Angina pectoris | 246.366 | -0,0131 | 0,0010 | QUENTE/FRIO | SENSIVEL_AO_METODO |
| K35 — Apendicite aguda | 236.521 | -0,2424 | -0,2535 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |

## 10. Demografia e distribuição territorial

Foram preservadas as cinco faixas etárias e os códigos SIH 1/3. Idade, sexo e idade×sexo têm comparações próprias; marginais são calculadas por contagens municipais antes das médias, sem somar medianas. Os resultados completos incluem todos os diagnósticos e suas combinações da Etapa 1.


| Marginal | Idade | Sexo | D100 A | D100 B | Direção A/B | N absoluto A/B |
|---|---|---|---|---|---|---|
| IDADE | 0-17 | Todos os sexos | -0,5722 | -0,3674 | QUENTE/QUENTE | 640/645 |
| IDADE | 18-39 | Todos os sexos | -2,5371 | -2,0443 | QUENTE/QUENTE | 640/645 |
| IDADE | 40-59 | Todos os sexos | -1,7067 | -0,9401 | QUENTE/QUENTE | 640/645 |
| IDADE | 60-79 | Todos os sexos | -0,7542 | -0,4403 | QUENTE/QUENTE | 640/645 |
| IDADE | 80+ | Todos os sexos | -0,0311 | 0,2379 | QUENTE/FRIO | 640/645 |
| SEXO | Todas as idades | Masculino | -2,0914 | -1,4784 | QUENTE/QUENTE | 640/645 |
| SEXO | Todas as idades | Feminino | -2,2863 | -2,1118 | QUENTE/QUENTE | 640/645 |

| Idade | Sexo | D100 A | D100 B | Direção A/B | N absoluto A/B | Robustez |
|---|---|---|---|---|---|---|
| 0-17 | Masculino | -0,3563 | -0,2128 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 0-17 | Feminino | -0,1031 | -0,1700 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 18-39 | Masculino | -1,2223 | -1,0068 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 18-39 | Feminino | -1,3347 | -1,1981 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 40-59 | Masculino | -0,6111 | -0,4813 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 40-59 | Feminino | -0,9775 | -0,5215 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 60-79 | Masculino | -0,1890 | -0,2508 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 60-79 | Feminino | -0,3720 | -0,2571 | QUENTE/QUENTE | 640/645 | ROBUSTO_DESCRITIVO |
| 80+ | Masculino | 0,0785 | 0,1586 | FRIO/FRIO | 640/645 | ROBUSTO_DESCRITIVO |
| 80+ | Feminino | -0,0591 | 0,0000 | QUENTE/IGUAL | 640/645 | SENSIVEL_AO_METODO |

Nas marginais de sexo, a direção permanece QUENTE em ambos. As quatro faixas até 79 anos também mantêm essa direção; 80+ muda de −0,0311 para +0,2379 por 100 município-dias. No cruzamento, mulheres 80+ passam de −0,0591 para zero; os outros nove cruzamentos conservam a direção. Esses contrastes próximos de zero exigem leitura da dispersão, sem descrever a mudança de sinal como diferença grande.


A comparação territorial usa quartis empíricos da média térmica municipal, incluindo empates, sem inventar grupos geográficos. Interior, litoral e regiões metropolitanas não receberam rótulos novos: os exemplos têm nomes/coordenadas e a análise prioriza o padrão térmico solicitado.


| Perfil térmico | Municípios | Mudança D100 B−A: Q1/mediana/Q3 | Direção diferente |
|---|---|---|---|
| QUARTIL_MAIS_FRIO | 163 | -4,5414/3,7586/19,2545 | 25 |
| QUARTIL_MAIS_QUENTE | 163 | -3,6661/0,5943/6,0314 | 33 |
| CENTRAL | 319 | -3,1404/0,2325/3,7121 | 28 |

| Município | Média térmica | D100 A | D100 B | B−A | Direção A/B |
|---|---|---|---|---|---|
| Jundiaí | 20,14 | -412,2228 | -129,8406 | 282,3822 | QUENTE/QUENTE |
| São José dos Campos | 20,73 | -288,8361 | -62,7718 | 226,0643 | QUENTE/QUENTE |
| Ubatuba | 19,19 | -634,4325 | -409,9672 | 224,4653 | QUENTE/QUENTE |
| Itu | 21,49 | -889,2504 | -675,8486 | 213,4018 | QUENTE/QUENTE |
| Potim | 15,22 | 173,4260 | -4,2156 | -177,6415 | FRIO/QUENTE |
| Carapicuíba | 20,14 | -559,9784 | -386,2058 | 173,7726 | QUENTE/QUENTE |
| Taboão da Serra | 20,14 | 174,7826 | 2,1152 | -172,6674 | FRIO/FRIO |
| Campos do Jordão | 15,22 | 124,0298 | -33,5358 | -157,5655 | FRIO/QUENTE |
| Campinas | 21,00 | 75,3499 | -79,7522 | -155,1020 | FRIO/QUENTE |
| Cotia | 20,14 | -264,3078 | -131,6008 | 132,7070 | QUENTE/QUENTE |

## 11. Diagnósticos raros, zeros e percentuais extremos


| Característica nos mesmos recortes | A | B | B−A |
|---|---|---|---|
| Recortes auditados | 37.403 | 37.403 | 0 |
| Mediana da média fria zero | 34.133 | 34.194 | 61 |
| Mediana da média quente zero | 33.855 | 34.100 | 245 |
| Mediana relativa −100% | 21.391 | 21.218 | -173 |
| Mediana relativa +100% | 0 | 15 | 15 |
| Algum município com denominador zero | 37.340 | 37.315 | -25 |
| Nenhum município relativo válido | 7.647 | 6.932 | -715 |
| N relativo 1–3, descrição do caso pedido | 5.248 | 5.439 | 191 |

As contagens de mediana zero não significam que todos os municípios têm média zero. Os detalhes e os contadores municipais mantêm essa distinção. A faixa N=1–3 apenas reproduz a questão levantada pelo exemplo de três municípios; não é regra de elegibilidade, exclusão ou significância. Histogramas completos de N relativo e total de eventos foram publicados, sem limiares de raridade. O volume total por recorte não muda ao reclassificar dias; a disponibilidade do denominador e a amostra percentual podem mudar. P20/P80 não cria eventos e não corrige automaticamente percentuais instáveis.


### Reavaliação do exemplo A43 — mulheres 60–79 anos


| Método | Eventos no período | N absoluto | N relativo | Mediana D100 absoluta | Mediana R (%) | Municípios frio zero | Municípios quente zero | Ambas zero |
|---|---|---|---|---|---|---|---|---|
| A | 12 | 640 | 3 | 0,000000 | -100,00 | 637 | 637 | 634 |
| B | 12 | 645 | 3 | 0,000000 | -100,00 | 643 | 642 | 640 |

Os 38 recortes 3/12 da Etapa 1 foram reavaliados nos mesmos CIDs/idades/sexos. Suas saídas e detalhes municipais estão em `audit_rare_results_*` e `audit_rare_municipal_*`. O resultado relativo continua complementar, com NULL quando quente=0, flags e base explícita. Mediana e IQR zero sobre a grande população com zeros não são evidência de ausência de associação em um diagnóstico raro.


## 12. Sazonalidade

Estações meteorológicas do hemisfério sul: verão DJF; outono MAM; inverno JJA; primavera SON. Não são datas astronômicas de equinócios. As proporções abaixo usam os dias válidos de cada condição; não são proporções de internações. Também foram publicadas distribuições por mês e ano e a proporção de cada condição dentro de cada mês.


| Método | % frio no inverno | % frio no verão | % quente no verão | % quente no inverno |
|---|---|---|---|---|
| Absoluto <20 / ≥25 °C | 57,14 | 3,34 | 39,87 | 5,62 |
| P20/P80 municipal | 65,07 | 0,76 | 41,93 | 4,08 |
| P20/P80 município × mês | 25,26 | 24,34 | 24,35 | 25,28 |

![06_sazonalidade](../data/validation/auditoria_definicao_temperatura/figuras/06_sazonalidade.png)

A distribuição observada permite quantificar a concentração de B em estações distintas. Não foi isolada a contribuição de vírus, feriados, dia da semana ou tendências. Estudos observacionais sobre temperatura e mortalidade usam ajustes temporais específicos; essa literatura contextualiza a limitação e não valida os limites 20/80 como ótimos para internações. [Estudo original: Temperature Variability and Mortality, seção de métodos](https://pmc.ncbi.nlm.nih.gov/articles/PMC5047764/).


## 13. Exploração município × mês

C calcula P20/P80 por município e mês do ano sobre os onze anos, comparando janeiros com janeiros, julhos com julhos. Reutiliza as mesmas métricas e a mesma elegibilidade por dias. Foram calculados cobertura, geral, anual, todos os capítulos e os dois grupos respiratórios. Não foi reconstruído CID×demografia completo para C.


C redistribui as exposições entre meses e responde a outra pergunta: anomalias relativamente frias/quentes para a época do ano. Isso reduz a separação inverno/verão visível na tabela, mas não corresponde a um ajuste estatístico completo de sazonalidade. Tendência entre anos, epidemias, dia da semana, diferenças dentro do mês, cobertura e seleção de estação permanecem possíveis componentes do contraste. Uma mudança nos resultados de C não identifica qual componente explica a mudança.


| Diagnóstico | D100 B | D100 C | Direção B/C | Robustez descritiva B/C |
|---|---|---|---|---|
| 10 — Capítulo X - Doenças do aparelho respiratório | 4,5164 | -0,9330 | FRIO/QUENTE | SENSIVEL_AO_METODO |
| 15 — Capítulo XV - Gravidez, parto e puerpério | -0,6273 | -0,2705 | QUENTE/QUENTE | ROBUSTO_DESCRITIVO |
| 9 — Capítulo IX - Doenças do aparelho circulatório | 1,3534 | -0,3983 | FRIO/QUENTE | SENSIVEL_AO_METODO |
| J09-J18 — Influenza [gripe] e pneumonia | 2,8097 | -0,6305 | FRIO/QUENTE | SENSIVEL_AO_METODO |
| J20-J22 — Outras infecções agudas das vias aéreas inferiores | 0,5089 | -0,0009 | FRIO/QUENTE | SENSIVEL_AO_METODO |

## 14. Sensibilidade e robustez

Regra descritiva declarada, sem margem percentual arbitrária: mesma direção e cada mediana dentro do IQR do outro método → ROBUSTO_DESCRITIVO; mesma direção sem essa compatibilidade central → PARCIALMENTE_ROBUSTO; direção diferente → SENSIVEL_AO_METODO. Ausência de base válida ou IQRs ambos inteiramente zero → INCONCLUSIVO para uma afirmação discriminante. O último caso evita chamar diagnósticos esparsos de robustos apenas porque suas medianas são zero.


Q1/Q3 são a distribuição municipal, não IC. O critério usa a dispersão observada solicitada pelo usuário; não é teste de equivalência, significância, ou garantia de magnitudes numericamente iguais. Sempre são apresentadas as magnitudes e diferenças reais. Em distribuições muito largas, compatibilidade entre IQRs é uma regra permissiva. Não foi imposto novo mínimo de eventos, população ou municípios.


| Achado prioritário | Avaliação documentada |
|---|---|
| Internações totais A/B | Robusto descritivo: mesma direção; −4,4000/−3,8619; compatibilidade com IQRs recíprocos. |
| Internações totais incluindo C | Parcialmente robusto na interpretação conjunta: mesma direção, porém −10,3190 em C, 2,35 vezes A; perguntas térmicas distintas. A regra automática de IQR é permissiva nesse caso. |
| Capítulo X, J09–J18 e J20–J22 A/B | Robustos descritivos, conforme regra e magnitudes nas tabelas. |
| Respiratórias B/C e circulatórias B/C | Sensíveis ao método: medianas mudam de sinal. |
| Gravidez/parto e puerpério A/B | Robusto pela regra descritiva; ambas medianas negativas e IQRs abrangem zero. |
| Sexo e idades até 79 anos A/B | Robustos pela regra; direção preservada, magnitudes próprias na seção 10. |
| 80+ e mulheres 80+ A/B | Sensíveis ao método, mas contrastes muito pequenos e dispersão atravessando zero. |
| Diagnósticos com medianas/IQRs inteiramente zero | Inconclusivos para uma afirmação discriminante; base percentual pode ser pequena. |

| Nível CID | Classificação A/B | Recortes |
|---|---|---|
| Categoria | ROBUSTO_DESCRITIVO | 522 |
| Capítulo | INCONCLUSIVO | 2 |
| Capítulo | ROBUSTO_DESCRITIVO | 17 |
| Capítulo | SENSIVEL_AO_METODO | 3 |
| Categoria | INCONCLUSIVO | 1.228 |
| Grupo | ROBUSTO_DESCRITIVO | 152 |
| Grupo | INCONCLUSIVO | 74 |
| Grupo | SENSIVEL_AO_METODO | 32 |
| Categoria | SENSIVEL_AO_METODO | 47 |

Os resultados gerais, os diagnósticos respiratórios e os demais achados devem ser avaliados separadamente. As tabelas das seções 7–10 e 13 identificam quais direções persistem e quais magnitudes mudam. Nenhuma conclusão sobre aumento de internações deve ser sustentada só pela preservação de um percentual ou de um rank.


## 15. Vantagens e limitações


| Método | Pergunta descritiva | Vantagem | Limitação |
|---|---|---|---|
| A — absoluto | Como diferem as médias abaixo de 20 e a partir de 25 °C? | Escala física comum e continuidade histórica | Frações de exposição desiguais, pouca condição quente em cinco municípios e composição sazonal |
| B — municipal | Como diferem as médias nos extremos relativamente frios e quentes locais? | Equilibra frações de exposição e recupera cinco municípios | Temperaturas físicas diferentes; concentração sazonal; percentuais raros continuam frágeis |
| C — município×mês | Como diferem as médias em dias excepcionais para o clima local e para o mês? | Reduz separação de exposições entre estações | Outro contraste; não elimina confundimento temporal e não foi auditado em toda CID×demografia |

Os métodos não fornecem classificação clínica universal de frio/quente nem limiar de conforto ou de internação. P20/P80 é uma escolha descritiva solicitada, não estimada por uma curva ótima. A série histórica tem dados faltantes e estações compartilhadas; não representa temperatura individual. Nenhuma normalização populacional, pseudocount, regressão ou linguagem de atribuição causal foi incorporada.


## 16. Recomendação metodológica

**Recomendação: cenário D — abordagens complementares, com P20/P80 municipal como referência quando a pergunta do TCC for explicitamente o contraste entre extremos térmicos locais.** O método B é mais alinhado a essa pergunta por oferecer frações comparáveis dentro de cada município. Os limites absolutos devem permanecer como análise de sensibilidade e como resultado principal quando a pergunta for literalmente <20/≥25 °C. A cobertura já elevada de A impede justificar a substituição apenas pelos cinco municípios recuperados.


A escolha de B não deve ser vendida como solução para diagnósticos raros ou para confundimento sazonal. Seus números de extremos e zeros estão publicados, incluindo resultados desfavoráveis. C deve acompanhar a interpretação como sensibilidade sazonal, porque sua pergunta e seus contrastes são diferentes; não há justificativa nesta auditoria descritiva para promovê-lo automaticamente como método único ou dizer que controla todos os fatores temporais.


A evidência decisiva para manter abordagens complementares é a mudança de sinal nos três recortes respiratórios em C, junto da concentração de 65,07% dos dias frios de B no inverno. A semelhança A/B sustenta a comparação de extremos locais, mas não uma afirmação geral de que dias frios em qualquer época tenham médias respiratórias maiores. A melhora de exposição não elimina essa limitação interpretativa.


Antes de fechar a redação do TCC, definir qual das três perguntas da seção 15 será a pergunta principal e explicitar a sensibilidade dos achados nas tabelas. A recomendação é local e fundamentada nas coberturas, distribuições e contrastes desta auditoria; a decisão final continua com o autor do TCC. Nenhuma produção foi substituída.


## 17. Alterações que seriam necessárias no dashboard — não executadas

Uma adoção posterior de B exigiria thresholds municipais versionados por período e definição explícita do baseline. Filtros de Ano não devem recalibrar automaticamente os percentis; recálculos por seleção responderiam a outra pergunta. Seria preciso incluir o método térmico, P20/P80 em °C, dias/exposição, N absoluto e N relativo, flags de zero, magnitude D100, quartis e consistência; títulos teriam de dizer “dias relativamente frios/quentes”. C exigiria ainda o mês do baseline. As tabelas experimentais não foram conectadas ao Power BI; DAX, páginas, visuais, PBIP/TMDL, bookmarks e filtros não foram alterados.


## Respostas objetivas às 14 perguntas


| Pergunta | Resposta baseada nas saídas |
|---|---|
| 1. Poucos ou nenhum dia frio absoluto? | Zero sem dias frios; 0 abaixo do critério de 30 dias. Distribuição completa na seção 2. |
| 2. Poucos ou nenhum dia quente absoluto? | Zero sem dias quentes; 5 abaixo de 30 dias; nomes e motivos na seção 4. |
| 3. Variação de P20/P80? | P20: 12,40–22,75 °C; P80: 17,88–27,58 °C. |
| 4. Mais municípios comparáveis? | 640 → 645; recuperados 5, perdidos 0. |
| 5. Geral muda? | D100 A/B/C: -4,4000/-3,8619/-10,3190; bases e dispersão na seção 7. |
| 6. Anual muda? | Sim, as magnitudes e as bases variam. Anos com direção A/B diferente: 2019, 2022. Tabela completa na seção 8. |
| 7. Respiratórias mudam? | A/B mantêm medianas positivas nos três recortes. C muda para negativas: Cap. X −0,9330; J09–J18 −0,6305; J20–J22 −0,000929 por 100 município-dias. Sensível à definição dentro do mês. |
| 8. Rankings CID mudam? | Sim; Spearman, direções, deslocamentos e exemplos nas tabelas da seção 9. |
| 9. Idade e sexo mudam? | Sexo e faixas até 79 mantêm direção. 80+ passa de −0,0311 para +0,2379; mulheres 80+ de −0,0591 para zero. Magnitudes muito próximas de zero. |
| 10. ±100% ficam mais/menos frequentes? | −100%: 21391 → 21218; +100%: 0 → 15. |
| 11. B fica confundido com sazonalidade? | 65,07% dos dias frios estão no inverno; 41,93% dos quentes, no verão. Há forte concentração, sem identificar ou ajustar todo confundimento. |
| 12. C resolve parte do problema? | Redistribui exposições por meses e reduz a separação inverno/verão; não é controle estatístico completo. |
| 13. Método mais defensável? | Para extremos térmicos locais, B. Para temperaturas físicas 20/25, A. Recomenda-se a análise complementar e a sensibilidade C. |
| 14. Manter A como sensibilidade? | Sim. Preserva a pergunta física e a continuidade; a metodologia oficial segue A até decisão do autor. |

## Testes, Data Quality, preservação e performance

44 testes executados: 44 aprovados, 0 falhas, 0 erros. Foram preservados os 24 anteriores e acrescentados 20: período técnico excluído, percentis exatos/ordenados, limites válidos, ausência/NaN/infinito/flag meteorológico, inclusividade, empates, degeneração, partição dos dias, reprodutibilidade com repartição, mês próprio, CID esparsa, peso igual, Spearman com empates, direção sensível, bloqueio de escrita fora da auditoria, recuperação sintética, baseline anual fixo, rejeição de duplicatas, exemplos reproduzíveis e rótulos demográficos oficiais.


DQ final PASS para fontes, thresholds, classificação diária, partição dos dias, chaves, fórmulas, NULL/NaN/infinito, zeros, direção e resumos. A comparação do método A com a produção passou no geral municipal e nos resumos CID, demografia e marginais. As duas falhas de integração encontradas durante desenvolvimento eram de colunas/rótulos nas novas saídas, foram corrigidas e cobertas por testes; nenhum número de produção foi reescrito.


| Etapa final medida | Segundos |
|---|---|
| thresholds_cobertura_classificacao | 58,292 |
| resultados_gerais_anuais_territoriais | 26,470 |
| numeradores_cid_demografia_compartilhados | 81,978 |
| cid_resultados_dq_auditoria | 56,068 |
| demografia_resultados_dq_auditoria | 217,261 |
| estratos_resultados_dq_auditoria | 180,814 |
| extremos_sazonalidade_verificacao | 49,973 |
| Total da execução final | 671,917 |

Os tempos acima são da execução final bem-sucedida, sem somar tentativas anteriores de desenvolvimento, suíte de testes ou renderização. Spark usa funções nativas; não há UDF nem loops Python nas fatos. Numeradores A/B e grãos demográficos são compartilhados, os resumos compactos são persistidos antes das verificações e Python recebe somente saídas pequenas selecionadas. O método C tem escopo limitado e não calcula o cruzamento demográfico inteiro.


Preservação da produção: 36 arquivos da área oficial de analytics tiveram hashes SHA-256 idênticos antes/depois. A verificação externa adicional de Power BI e código oficial está em `verificacao_etapa1b.json` e `protected_hashes.json`.


## Arquivos e reprodução

Arquivos novos de implementação:

- `src/validation/auditoria_definicao_temperatura.py` — runner Spark, testes de DQ e saídas experimentais.
- `src/validation/relatorio_definicao_temperatura.py` — relatório e gráficos a partir das saídas compactas.
- `tests/test_temperature_definition_audit.py` — 20 testes novos.
- `docs/Relatorio_Auditoria_Definicao_Temperatura.md` — este relatório.

Dados: `data/validation/auditoria_definicao_temperatura`, com Parquets e CSVs de thresholds, exposições, elegibilidade, exemplos, classificação diária, concordância, geral, anual, base comum, CID, rankings, demografia, marginais, raros, extremos, sazonalidade e a exploração mensal. `resultados_auditoria.json` reúne os resultados numéricos compactos. Logs de execução e testes, hashes e `manifesto_arquivos.json` registram os arquivos físicos.

Gráficos: sete PNGs e sete SVGs em `figuras/`, todos estáticos e fora do Power BI. Para repetir: executar o runner com `spark-submit` no Spark existente; depois executar o gerador de relatório no Python do mesmo ambiente, que já contém NumPy, Pandas e Matplotlib. Nenhum download, rebuild de imagens, Bronze/Silver/Gold ou publicação foi necessário.
