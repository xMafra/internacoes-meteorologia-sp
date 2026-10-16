# Revisão das métricas frio × quente — Etapa 1

Data de execução: 05/10/2026. Escopo: código, Consumption analítica, testes, Data Quality e documentação. Nenhum arquivo de apresentação do Power BI foi alterado nesta etapa.

## Problema e auditoria do comportamento anterior

O TCC descreve associações entre temperatura e internações no Estado de São Paulo entre 2015 e 2025. O percentual intramunicipal permanecia útil, mas recebia destaque sem uma medida de magnitude suficientemente explícita. Uma média quente muito pequena pode produzir percentual elevado; média quente zero torna a divisão indefinida. Poucos eventos ou municípios não tornam o resultado incorreto aritmeticamente, mas restringem sua interpretação.

`src/consumption/dashboard_analytics.py` já calculava médias municipais, diferença absoluta e relativa em `_adicionar_metricas`. `_resumo` sintetizava principalmente os municípios com percentual válido. `criar_cid_resumo` e `criar_demografia_resumo` cruzavam os municípios com as dimensões CID/demográficas e preenchiam contagens ausentes com zero. Os denominadores eram os dias do painel denso, incluindo dias sem eventos. As tabelas compactas existentes incluíam `contraste_municipio`, `cid_resumo`, `demografia_resumo`, `temporal_ano` e `lag_resumo`.

Os testes anteriores eram `test_dashboard_analytics.py` (2 testes) e `test_analytic_metrics.py` (6 testes). A validação histórica estava em `dq_dashboard_analytics.py`. Os módulos `metric_audit.py` e `lag_intramunicipal_audit.py` permanecem como referências de validação, sem alteração.

## Definições preservadas e fórmulas

Frio: temperatura média diária <20 °C. Quente: temperatura média diária ≥25 °C. A faixa intermediária não integra essa comparação. Elegibilidade municipal: pelo menos 30 dias em cada condição. Período, fontes, fatos, sexo, idade, hierarquia CID e lags foram preservados. Não há pseudocount, normalização populacional, exclusão de CID raro nem regressão.

Para cada município m, Fm = internações no frio / dias frios e Qm = internações no quente / dias quentes. Com dias positivos:

- Diferença absoluta: Dm = Fm − Qm, em internações por município-dia.
- Diferença por 100: D100m = 100 × Dm, em internações por 100 município-dias. É uma reescala, sem denominador populacional.
- Diferença relativa complementar: Rm = (Fm − Qm) / Qm quando Qm >0; NULL quando Qm=0. O campo armazena uma razão: 1 equivale a 100%, −1 equivale a −100%.
- Direção: FRIO se Dm>0; QUENTE se Dm<0; IGUAL se Dm=0; NAO_APLICAVEL quando não existe comparação.

## Zeros e bases distintas

| Frio | Quente | D | D100 | R | Direção e interpretação municipal |
|---:|---:|---:|---:|---:|---|
| 10 | 8 | 2 | 200 | 25% | FRIO |
| 0 | 2 | −2 | −200 | −100% | QUENTE; eventos observados somente na condição quente |
| 2 | 0 | 2 | 200 | NULL | FRIO; eventos observados somente na condição fria |
| 0 | 0 | 0 | 0 | NULL | IGUAL; ambas as médias zero |
| 0,02 | 0,01 | 0,01 | 1 | 100% | FRIO; percentual elevado e magnitude pequena |

Ausência de dias de exposição resulta em média NULL, não zero. Contagem zero com dias observados resulta em média zero real. Não se converte NULL em zero nem se calcula infinito.

Há duas populações explícitas: `municipios_validos_absoluto` inclui todos os municípios elegíveis com ambas as médias conhecidas, inclusive ambas zero; `municipios_validos` conserva o significado histórico de municípios elegíveis com percentual definido. Portanto, o N da análise absoluta pode exceder muito o N relativo em CIDs raros. `municipios_elegiveis` conserva a elegibilidade por dias. Nenhum município foi removido por raridade.

Os campos históricos, inclusive `mediana_diferenca_absoluta`, mantêm sua população relativa original. Os novos campos com sufixo `_elegiveis` usam a população absoluta. Não se deve apresentar o N relativo como se fosse o N de uma nova mediana absoluta.

## Hierarquia e resumo municipal

A leitura recomendada é: magnitude absoluta e reescala por 100, distribuição (Q1/mediana/Q3), base e zeros, consistência territorial e percentual complementar. Não inferir causalidade nem intensidade apenas do percentual.

As medianas são calculadas depois das comparações municipais, atribuindo o mesmo peso a cada município elegível. A mediana das diferenças não é necessariamente a diferença das medianas das médias. Os quartis usam `percentile_approx(..., accuracy=10000)`, preservando a convenção empírica do projeto. Para N par, a mediana pode ser a observação central inferior, e não a média dos dois valores centrais. [Referência Spark](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.functions.percentile_approx.html).

Os novos resumos fornecem Q1/mediana/Q3 de D e de D100, medianas de F e Q e direção pela mediana D. Somatórios de eventos e dias são disponibilizados para todos os municípios e separadamente para cada população válida. Somar os totais entre capítulos, grupos e categorias contaria os mesmos eventos várias vezes; somar dias entre CIDs também duplicaria exposições.

## Consistência territorial

A regra validada de −1% a +1%, inclusive, foi preservada. As novas classes são mutuamente exclusivas: PEQUENA nessa faixa; FRIO acima dela; QUENTE abaixo dela. A base é `municipios_validos`, pois a regra depende de R. Casos Q=0 têm classe NAO_APLICAVEL e continuam na distribuição absoluta. Os três percentuais somam 100% quando há base relativa.

Os campos históricos de sinais estritos e aproximadamente zero continuam disponíveis; suas contagens podem se sobrepor. Usar os novos `*_consistencia` para gráficos de partes de um todo. Para sinais absolutos estritos, há `municipios_maior_frio_absoluto`, `municipios_maior_quente_absoluto` e `municipios_medias_iguais_absoluto`, cuja soma é o N absoluto.

## Flags de zero

No detalhe municipal, `media_frio_zero`, `media_quente_zero` e `ambas_medias_zero` descrevem as médias daquele município. No resumo, os mesmos nomes descrevem as medianas das médias da população absoluta. Uma mediana zero NÃO significa ausência de eventos em todos os municípios. Para isso, consultar as contagens `municipios_frio_zero`, `municipios_quente_zero`, `municipios_ambas_medias_zero`, `municipios_so_frio`, `municipios_so_quente` e os volumes observados. `municipios_frio_zero_relativo` identifica os municípios com R=−100% entre os válidos relativos.

## Tabelas e campos

`cid_resumo` e `demografia_resumo` foram estendidas preservando todas as colunas e chaves antigas. `contraste_municipio`, `temporal_ano` e `lag_resumo` receberam a mesma camada de métricas, sem mudança na lógica temporal ou de defasagem.

Duas tabelas novas têm grãos distintos e necessários: `geral_resumo` contém uma comparação geral; `estratos_resumo` contém marginais de idade OU sexo, por TOTAL/capítulo/grupo/categoria. As marginais somam contagens dentro de cada município antes de calcular médias e diferenças; não somam medianas dos estratos conjuntos. `nivel_demografico` distingue IDADE/SEXO. Idade marginal usa sexo TODOS; sexo marginal usa Todas as idades. Os códigos de sexo e as cinco faixas etárias originais foram preservados. `demografia_resumo` continua a representar o cruzamento idade × sexo.

### Campos para a futura Etapa 2

| Uso | Campos exatos |
|---|---|
| Magnitude principal | `mediana_diferenca_absoluta_elegiveis`, `mediana_diferenca_por_100` |
| Dispersão municipal | `q1_diferenca_absoluta_elegiveis`, `q3_diferenca_absoluta_elegiveis`, `q1_diferenca_por_100`, `q3_diferenca_por_100` |
| Médias e direção | `mediana_media_frio_elegiveis`, `mediana_media_quente_elegiveis`, `direcao_diferenca` |
| Bases | `municipios_validos_absoluto`, `municipios_validos`, `municipios_elegiveis`, `municipios_total` |
| Volumes | `internacoes_total`, `dias_frios_total`, `dias_quentes_total`; para a mesma população da magnitude, `internacoes_total_validos_absoluto`, `internacoes_frio_validos_absoluto`, `internacoes_quente_validos_absoluto`, `dias_frios_validos_absoluto`, `dias_quentes_validos_absoluto`; equivalentes `_validos_relativo` para o percentual |
| Percentual complementar | `mediana_diferenca_relativa`, com base `municipios_validos`; os antigos `mediana_media_frio`, `mediana_media_quente` e `mediana_diferenca_absoluta` descrevem essa mesma população |
| Consistência | `municipios_frio_consistencia`, `municipios_quente_consistencia`, `municipios_pequena_consistencia`, `percentual_frio_consistencia`, `percentual_quente_consistencia`, `percentual_pequena_consistencia` |
| Zeros | flags e contagens descritas na seção anterior; não interpretar flag de mediana como ausência de eventos estadual |
| Intervalos, onde calculados | `ic95_diferenca_por_100_inferior`, `ic95_diferenca_por_100_superior`, `bootstrap_status`, `bootstrap_replicas`, `bootstrap_seed` |
| Novos recortes marginais | `estratos_resumo`: `chave_alvo`, `nivel_demografico`, `faixa_etaria`, `sexo`; resumo geral em `geral_resumo` |

O detalhe municipal fornece `diferenca_absoluta`, `diferenca_por_100_municipio_dias`, `diferenca_relativa`, médias, contagens, dias, flags, validade e classe. Nenhuma média/mediana pré-calculada deve ser somada para produzir novos recortes. Para filtros futuros de ano/município, será preciso calcular no grão adequado ou usar uma tabela específica; um resumo 2015–2025 por CID não oferece dinamicamente um filtro anual inexistente em seu grão. Nenhum DAX foi alterado nesta etapa.

## Auditoria de extremos e bases pequenas

Saída: `data/validation/auditoria_metricas_extremas`. `resumos_auditoria` e `resumos_csv` incluem TODOS os recortes e bases quantitativas. Não foi inventado limiar de “poucos eventos” ou “média pequena”. A tabela `casos_extremos` identifica ±100%, proximidade até 1 ponto percentual de ±100% e municípios com médias zero. Essa janela serve apenas para localizar o exemplo aproximado; não altera elegibilidade nem exclui resultados. A tolerância 1e−10 dos DQs serve à aritmética de ponto flutuante e nunca entra nas fórmulas como pseudocount.

Os detalhes `municipal_cid`, `municipal_demografia` e `municipal_estratos` permitem conferir distribuições completas, incluindo municípios com zero eventos. Há cópias do histórico anterior e arquivos próprios do caso 3/12, permitindo reprodução da explicação matemática.

## Resultados executados e cardinalidade

Status final: PASS. Foram usados 2.591.610 município-dias e 27.671.456 internações da Consumption existente; mínimo/máximo de ano confirmados: 2015/2025. Nenhuma fato foi regravada.

| Tabela | Linhas | Preservação |
|---|---:|---|
| `cid_resumo` | 2077 | 32 colunas antigas e conjunto de chaves preservados |
| `demografia_resumo` | 20780 | 37 colunas antigas e conjunto de chaves preservados |
| `estratos_resumo` | 14546 | Novo grão compacto |
| `geral_resumo` | 1 | Novo grão compacto |
| `contraste_municipio` | 645 | 16 colunas antigas e conjunto de chaves preservados |
| `temporal_ano` | 11 | 23 colunas antigas e conjunto de chaves preservados |
| `lag_resumo` | 16 | 27 colunas antigas e conjunto de chaves preservados |

CID: 2.077 chaves únicas. Demografia: (2.077+1) × 5 idades × 2 sexos = 20.780. Marginais: (2.077+1) × (5 idades+2 sexos) = 14.546. Sem duplicatas. A comparação do recálculo com as referências históricas passou; todas as colunas antigas foram conservadas literalmente na publicação.

## Testes e Data Quality

Rodaram 24 testes: 24 aprovados, 0 falhas, 0 erros; duração da suíte: 31,619 s. Os 8 testes anteriores foram preservados. Foram adicionados 16 testes: cinco casos numéricos obrigatórios; bases absoluta/relativa separadas; mediana das diferenças; peso igual dos municípios; fronteiras inclusivas de ±1%; exposição CID esparsa; estratos conjuntos zero; marginais calculadas por contagens; rejeição DQ de reescala corrompida; ausência de dias distinta de média zero; bootstrap reproduzível e rejeição de NaN; integração do bootstrap demográfico e exclusão explícita de recortes fora de escopo.

DQ final: `DQ_DASHBOARD_ANALYTICS=PASS`. As novas verificações validaram detalhes municipais e resumos: fórmulas, contagens, dias, médias, NULL/infinito/NaN, flags, direção, quartis, exclusividade das classes, somas percentuais, limites dos intervalos e chaves. Os DQs históricos de geral, CID, anos 2020/2025 e 16 lags passaram. O teste de corrupção deliberada produz uma rejeição esperada, não uma falha da suíte. Nenhuma inconsistência foi corrigida silenciosamente.

## Resultado geral, como verificação de escala

Entre os 640 municípios válidos absolutos, a mediana D é -0,044000 internações por município-dia, equivalente a -4,400002 por 100 município-dias. Q1/Q3 de D100: -20,690183/4,372566. Mediana relativa histórica: -1,8928%. IC exploratório de D100: [-5,658716; -3,232580]. Esses números descrevem diferenças observadas, não efeitos causais.

## Auditoria quantitativa dos extremos

Foram materializados 37403 recortes e 37359 recortes com pelo menos um motivo de auditoria. Há 21391 medianas exatamente −100% e 0 exatamente +100%. 30763 recortes possuem pelo menos um município elegível com quente zero e frio positivo; 29331 possuem pelo menos um com frio zero e quente positivo. As classes de auditoria se sobrepõem. Todos os demais recortes e suas bases continuam no arquivo completo, inclusive os com zero internações.

## Caso real: 3 municípios / 12 internações / 100%

A combinação não identifica um único recorte: foram localizados 38, dos quais 25 em `demografia_resumo` e 13 nas novas marginais. TODOS têm mediana técnica −100%, não +100%. Não é possível afirmar qual dos 38 estava selecionado na captura do dashboard somente a partir de N=3 e total=12. Todos foram documentados na tabela abaixo e nos arquivos próprios da auditoria.

**Exemplo completo: A43 — Nocardiose; sexo feminino (código 3); 60–79 anos.** Total do recorte: 12 internações, distribuídas por 10 municípios com algum evento. São 3 no frio, 3 no quente e 6 na faixa intermediária. Os 640 municípios elegíveis permanecem na análise absoluta; 634 apresentam ambas as médias zero. Somente 3 municípios possuem média quente positiva e entram no percentual histórico.

| Município (IBGE) | Internações no período | Frio | Quente | Intermediária | Dias frios | Dias quentes | Média frio | Média quente | D | D100 | R |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| 3501905 | 1 | 0 | 1 | 0 | 1275 | 523 | 0,000000000 | 0,001912046 | -0,001912046 | -0,191204589 | -100,00% |
| 3507506 | 1 | 0 | 0 | 1 | 791 | 1314 | 0,000000000 | 0,000000000 | 0,000000000 | 0,000000000 | NULL |
| 3509502 | 2 | 1 | 0 | 1 | 1465 | 409 | 0,000682594 | 0,000000000 | 0,000682594 | 0,068259386 | NULL |
| 3527405 | 1 | 0 | 1 | 0 | 472 | 1769 | 0,000000000 | 0,000565291 | -0,000565291 | -0,056529112 | -100,00% |
| 3529401 | 1 | 0 | 0 | 1 | 1972 | 222 | 0,000000000 | 0,000000000 | 0,000000000 | 0,000000000 | NULL |
| 3541000 | 1 | 1 | 0 | 0 | 1878 | 257 | 0,000532481 | 0,000000000 | 0,000532481 | 0,053248136 | NULL |
| 3544103 | 1 | 0 | 1 | 0 | 837 | 1047 | 0,000000000 | 0,000955110 | -0,000955110 | -0,095510984 | -100,00% |
| 3550308 | 2 | 0 | 0 | 2 | 1619 | 356 | 0,000000000 | 0,000000000 | 0,000000000 | 0,000000000 | NULL |
| 3552601 | 1 | 0 | 0 | 1 | 536 | 1594 | 0,000000000 | 0,000000000 | 0,000000000 | 0,000000000 | NULL |
| 3554102 | 1 | 1 | 0 | 0 | 1616 | 473 | 0,000618812 | 0,000000000 | 0,000618812 | 0,061881188 | NULL |

Nos três municípios válidos relativos (3501905, 3527405, 3544103), F=0 e Q é respectivamente 1/523, 1/1769 e 1/1047. Assim, (0−Q)/Q = −1 = −100% em cada um. A mediana também é −100%. Os dias dessa base são 2.584 frios e 3.339 quentes; nela há apenas 3 internações no período. As 12 internações mostradas no resumo abrangem todos os municípios e condições, portanto não são o numerador de um único percentual agregado.

Na população relativa de 3 municípios, mediana F=0; mediana Q=0,000955109838; mediana D=−0,000955109838; mediana D100=−0,095510983763. Portanto, uma mediana técnica −100% corresponde aqui a menos de 0,10 internação de diferença por 100 município-dias nessa população. Não significa “100% mais internações no quente”. A observação de eventos somente no quente aplica-se a esses três municípios e condições; o recorte completo também registra eventos no frio em outros municípios.

Na nova população absoluta, N=640, medianas F=Q=D=D100=0 e direção da mediana IGUAL. Há três municípios com D positivo, três com D negativo e 634 com D zero. Os dias dos elegíveis absolutos somam 597.931 frios e 675.310 quentes; os totais de todos os 645 municípios são 616.741 e 675.345. A mediana zero é consequência da distribuição esparsa, não prova de ausência de diferenças municipais. Essas bases distintas estão disponíveis explicitamente.

### Todos os recortes que reproduzem N=3 e total=12

| Origem | CID | Idade | Sexo | R | D da população relativa | D100 da população relativa | Dias frio/quente dessa população |
|---|---|---|---|---:|---:|---:|---|
| demografia_resumo | A07 — Outras doenças intestinais por protozoários | 80+ | Feminino | -100,00% | -0,001269036 | -0,126903553 | 2983/2339 |
| demografia_resumo | A43 — Nocardiose | 60-79 | Feminino | -100,00% | -0,000955110 | -0,095510984 | 2584/3339 |
| demografia_resumo | A56 — Outras infecções causadas por clamídias transmitidas por via sexual | 40-59 | Feminino | -100,00% | -0,001377410 | -0,137741047 | 2874/2886 |
| demografia_resumo | A93 — Outras febres por vírus transmitidas por artrópodes não classificadas em outra parte | 18-39 | Feminino | -100,00% | -0,001377410 | -0,137741047 | 2886/2099 |
| demografia_resumo | B90 — Seqüelas de tuberculose | 0-17 | Masculino | -100,00% | -0,001141553 | -0,114155251 | 3071/2333 |
| demografia_resumo | D24 — Neoplasia benigna da mama | 60-79 | Masculino | -100,00% | -0,000659631 | -0,065963061 | 1670/4367 |
| demografia_resumo | D67 — Deficiência hereditária do fator IX | 60-79 | Masculino | -100,00% | -0,001319261 | -0,131926121 | 2893/2653 |
| demografia_resumo | E80 — Distúrbios do metabolismo da porfirina e da bilirrubina | 80+ | Masculino | -100,00% | -0,001573658 | -0,157365831 | 3350/2338 |
| demografia_resumo | F44 — Transtornos dissociativos [de conversão] | 80+ | Feminino | -100,00% | -0,000897666 | -0,089766607 | 1992/3772 |
| demografia_resumo | F82 — Transtorno específico do desenvolvimento motor | 0-17 | Feminino | -100,00% | -0,000761035 | -0,076103501 | 2190/3802 |
| demografia_resumo | H31 — Outros transtornos da coróide | 40-59 | Masculino | -100,00% | -0,000935454 | -0,093545370 | 1939/3910 |
| demografia_resumo | H36 — Transtornos da retina em doenças classificadas em outra parte | 80+ | Masculino | -100,00% | -0,000659631 | -0,065963061 | 2292/3657 |
| demografia_resumo | H67 — Otite média em doenças classificadas em outra parte | 18-39 | Feminino | -100,00% | -0,000621504 | -0,062150404 | 1549/4819 |
| demografia_resumo | K00 — Distúrbios do desenvolvimento e da erupção dos dentes | 40-59 | Masculino | -100,00% | -0,000627353 | -0,062735257 | 3044/3410 |
| demografia_resumo | L26 — Dermatite esfoliativa | 0-17 | Masculino | -100,00% | -0,001495353 | -0,149535302 | 3563/2630 |
| demografia_resumo | L71 — Rosácea | 40-59 | Feminino | -100,00% | -0,000954198 | -0,095419847 | 3016/3230 |
| demografia_resumo | M40 — Cifose e lordose | 18-39 | Masculino | -100,00% | -0,001620746 | -0,162074554 | 2815/2078 |
| demografia_resumo | M42 — Osteocondrose da coluna vertebral | 40-59 | Feminino | -100,00% | -0,001748252 | -0,174825175 | 3188/2166 |
| demografia_resumo | M90 — Osteopatias em doenças classificadas em outra parte | 80+ | Feminino | -100,00% | -0,001201923 | -0,120192308 | 1958/3256 |
| demografia_resumo | M91 — Osteocondrose juvenil do quadril e da pelve | 40-59 | Masculino | -100,00% | -0,002145923 | -0,214592275 | 3350/2338 |
| demografia_resumo | P23 — Pneumonia congênita | 40-59 | Masculino | -100,00% | -0,001910220 | -0,191021968 | 3484/1988 |
| demografia_resumo | Q81 — Epidermólise bolhosa | 60-79 | Feminino | -100,00% | -0,002114165 | -0,211416490 | 4072/1876 |
| demografia_resumo | R21 — Eritema e outras erupções cutâneas não especificadas | 18-39 | Masculino | -100,00% | -0,001064963 | -0,106496273 | 2427/2841 |
| demografia_resumo | R92 — Achados anormais, de exames para diagnóstico por imagem, da mama | 18-39 | Feminino | -100,00% | -0,001079800 | -0,107980006 | 3999/2072 |
| demografia_resumo | Z60 — Problemas relacionados com o meio social | 18-39 | Masculino | -100,00% | -0,000955993 | -0,095599309 | 3921/1812 |
| estratos_resumo | A22 — Carbúnculo | Todas as idades | Masculino | -100,00% | -0,000997009 | -0,099700897 | 2019/3738 |
| estratos_resumo | A37 — Coqueluche | 40-59 | Todos os sexos | -100,00% | -0,001748252 | -0,174825175 | 3079/2157 |
| estratos_resumo | B23 — Doença pelo vírus da imunodeficiência humana [HIV] resultando em outras doenças | 80+ | Todos os sexos | -100,00% | -0,001573658 | -0,157365831 | 3646/2206 |
| estratos_resumo | B67 — Equinococose | Todas as idades | Masculino | -100,00% | -0,000659631 | -0,065963061 | 1469/4693 |
| estratos_resumo | B82 — Parasitose intestinal não especificada | 40-59 | Todos os sexos | -100,00% | -0,002808989 | -0,280898876 | 3848/2283 |
| estratos_resumo | G05 — Encefalite, mielite e encefalomielite em doenças classificadas em outra parte | 80+ | Todos os sexos | -100,00% | -0,000575705 | -0,057570524 | 2918/3728 |
| estratos_resumo | L41 — Parapsoríase | Todas as idades | Masculino | -100,00% | -0,000659631 | -0,065963061 | 2230/3800 |
| estratos_resumo | L56 — Outras alterações agudas da pele devidas a radiação ultravioleta | Todas as idades | Feminino | -100,00% | -0,000955110 | -0,095510984 | 2637/3260 |
| estratos_resumo | M49 — Espondilopatias em doenças classificadas em outra parte | 80+ | Todos os sexos | -100,00% | -0,000996016 | -0,099601594 | 3074/2727 |
| estratos_resumo | R92 — Achados anormais, de exames para diagnóstico por imagem, da mama | 18-39 | Todos os sexos | -100,00% | -0,001079800 | -0,107980006 | 3999/2072 |
| estratos_resumo | T41 — Intoxicação por anestésicos e gases terapêuticos | 60-79 | Todos os sexos | -100,00% | -0,001748252 | -0,174825175 | 3188/2166 |
| estratos_resumo | Y71 — Dispositivos (aparelhos) cardiovasculares, associados a incidentes adversos | Todas as idades | Feminino | -100,00% | -0,002114165 | -0,211416490 | 4026/2143 |
| estratos_resumo | P50-P61 — Transtornos hemorrágicos e hematológicos do feto e do recém-nascido | 80+ | Todos os sexos | -100,00% | -0,000693963 | -0,069396253 | 2603/3625 |

D100 relativo nesta tabela é a reescala direta de `mediana_diferenca_absoluta` histórica, não a nova mediana sobre 640 municípios. As médias e flags de ambas as populações constam de `caso_3_12.json` e os cálculos municipais completos dos Parquets e `caso_3_12_municipios.json`.

## Performance medida

| Etapa | Segundos |
|---|---:|
| numeradores_compartilhados | 108,630 |
| cid_metricas_dq_bootstrap | 34,379 |
| demografia_metricas_dq_bootstrap | 228,987 |
| estratos_metricas_dq_bootstrap | 156,399 |
| geral_temporal_lags | 90,346 |
| auditoria | 40,049 |
| publicacao | 3,954 |
| total_segundos | 663,693 |
| verificacao_final_com_benchmark | 28,663 |

Bootstrap CID: 280 recortes em 3,358 s. Bootstrap demográfico: 40 recortes em 6,495 s, incluindo leitura/materialização do resumo. Geral: 1 recorte. São 321 recortes × 1.000 réplicas, sem bootstrap nas milhares de categorias.

Benchmark sobre o detalhe CID já materializado e em cache (1.339.665 linhas; 2.077 resumos), com sequência legado/ampliado/ampliado/legado: legado 1,197 s em média; ampliado 2,361 s em média; acréscimo 1,163 s. Foi forçado o cálculo de todas as estatísticas, coletando somente os 2.077 resumos, nunca as fatos. O custo da agregação isolada aproximadamente dobrou, mas o acréscimo absoluto foi cerca de 1 s. Não existe medição do pipeline completo antigo nas mesmas condições; não é correto extrapolar esse percentual para toda a Consumption.

A execução principal durou cerca de 11 minutos; a checagem final focada, cerca de 29 s. A contagem principal não inclui essa checagem nem testes/DQ executados separadamente. A maior parte do custo está na preparação dos numeradores, nos detalhes municipais para auditoria e nas verificações/reconciliações dos estratos. O runner final persiste também os resumos compactos antes das várias ações de validação, evitando repetir suas agregações. O tempo de 11 minutos foi medido antes dessa otimização; não foi atribuída a ela uma redução sem medição.

## Lista completa de arquivos de implementação e documentação

- `src/consumption/dashboard_analytics.py`
- `src/consumption/fq_bootstrap.py`
- `src/consumption/revisao_frio_quente.py`
- `src/quality/consumption/dq_dashboard_analytics.py`
- `src/quality/consumption/fq_checks.py`
- `tests/test_fq_revision.py`
- `docs/Relatorio_Revisao_Metricas_Frio_Quente.md`

### Dados materializados

Em `data/consumption/dashboard_analytics`: `cid_resumo`, `demografia_resumo`, `contraste_municipio`, `temporal_ano`, `lag_resumo` foram estendidos; `geral_resumo` e `estratos_resumo` foram criados. Os dois outros datasets existentes não foram regenerados.

Em `data/validation/auditoria_metricas_extremas`: detalhes municipais dos três grãos; `resumos_auditoria`; `resumos_csv`; `casos_extremos`; `caso_3_municipios_12_internacoes`; detalhes `caso_3_12_municipal_cid`, `_demografia`, `_estratos`; `caso_3_12.json`; `caso_3_12_municipios.json`; `historico_antes` das cinco tabelas preexistentes; `resultado_execucao.json`; `testes.log`, `dq.log`, `execucao.log`, `verificacao_final.log`; `verificacao_etapa1.json`, `powerbi_hashes_preservados.json` e `manifesto_arquivos_etapa1.json`. O manifesto lista todos os arquivos físicos derivados, incluindo as partes Parquet.

Preservação Power BI verificada por SHA-256: 52 arquivos PBIP/TMDL/JSON, com conjunto de caminhos e conteúdo idênticos à fotografia anterior à Etapa 1. Nenhum campo de apresentação foi modificado.

### Colunas novas dos resumos CID/demográficos

Campos comuns adicionados a `cid_resumo` (também disponíveis no grão conjunto/marginal, geral, temporal e lag, quando aplicável):

- `municipios_validos_absoluto`
- `mediana_media_frio_elegiveis`
- `mediana_media_quente_elegiveis`
- `municipios_frio_zero`
- `municipios_quente_zero`
- `municipios_ambas_medias_zero`
- `municipios_frio_zero_relativo`
- `municipios_so_quente`
- `municipios_so_frio`
- `municipios_maior_frio_absoluto`
- `municipios_maior_quente_absoluto`
- `municipios_medias_iguais_absoluto`
- `municipios_frio_consistencia`
- `municipios_quente_consistencia`
- `municipios_pequena_consistencia`
- `internacoes_total_validos_absoluto`
- `internacoes_total_validos_relativo`
- `internacoes_frio_validos_absoluto`
- `internacoes_frio_validos_relativo`
- `internacoes_quente_validos_absoluto`
- `internacoes_quente_validos_relativo`
- `dias_frios_validos_absoluto`
- `dias_frios_validos_relativo`
- `dias_quentes_validos_absoluto`
- `dias_quentes_validos_relativo`
- `q1_diferenca_absoluta_elegiveis`
- `mediana_diferenca_absoluta_elegiveis`
- `q3_diferenca_absoluta_elegiveis`
- `q1_diferenca_por_100`
- `mediana_diferenca_por_100`
- `q3_diferenca_por_100`
- `direcao_diferenca`
- `media_frio_zero`
- `media_quente_zero`
- `ambas_medias_zero`
- `percentual_frio_consistencia`
- `percentual_quente_consistencia`
- `percentual_pequena_consistencia`

Campos adicionais do detalhe municipal:

- `diferenca_por_100_municipio_dias`
- `diferenca_zero`
- `media_frio_zero`
- `media_quente_zero`
- `ambas_medias_zero`
- `direcao_diferenca`
- `contraste_absoluto_valido`
- `classe_consistencia`

Campos bootstrap adicionais onde calculado: `ic95_diferenca_por_100_inferior`, `ic95_diferenca_por_100_superior`, `bootstrap_replicas`, `bootstrap_seed`, `bootstrap_status`. Marginais acrescentam `nivel_demografico`; dimensões e ordens permanecem explícitas.

## Bootstrap e limitações

Foi implementado bootstrap exploratório por município com reposição, 1.000 réplicas e seed 20261005. Cada réplica estima a mediana D100 sobre os municípios válidos absolutos. Os limites são os percentis 2,5% e 97,5% das estimativas; não representam Q1/Q3 da distribuição municipal. A subseed por chave e a ordenação por município tornam o resultado reproduzível independentemente da partição de entrada. A estatística usa a mesma mediana empírica inferior do resumo Spark. [Referência metodológica para reamostragem e intervalo percentil](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.bootstrap.html).

Escopo: geral; capítulos e grupos CID; os dez cruzamentos idade × sexo de TOTAL, capítulo respiratório 10, grupos J09–J18 e J20–J22. Categorias individuais e marginais adicionais ficam fora do bootstrap para limitar custo e evitar a apresentação automática de milhares de intervalos pouco informativos. Isso não exclui suas métricas ou diagnósticos. Status e limites NULL identificam o escopo não calculado.

Os cálculos regulares usam funções nativas Spark, sem UDF e sem loops Python nas fatos. Apenas pequenos vetores municipais dos recortes selecionados chegam ao NumPy. A reamostragem é vetorizada em lotes de 64 réplicas. O runner compartilha os numeradores CID/demográficos e persiste intermediários para evitar leituras/agregações repetidas.

Intervalos degenerados [0;0] podem ocorrer quando muitos municípios têm zero eventos. Eles não demonstram ausência de associação ou certeza. O bootstrap não modela dependência espacial, dependência temporal dentro de cada município, confundimento, sazonalidade ou incerteza de medição. Municípios são unidades observacionais, não sorteados em experimento. O resultado não é causal; bases pequenas e seleção por denominador exigem cautela. Esses limites permanecem mesmo quando os DQs passam.

## Reprodução e preservação

Executar `src/consumption/revisao_frio_quente.py` no Spark existente, depois `src/quality/consumption/dq_dashboard_analytics.py`. A revisão usa apenas a Consumption disponível e publica as comparações depois das validações e da reconciliação histórica. `temperatura_faixa` e `sazonalidade_mes` não precisaram ser regeneradas. Bronze, Silver, Gold, downloads, fatos principais e apresentação Power BI não foram gravados.

O gerador regular `dashboard_analytics.py` também produz as novas métricas e os dois novos grãos. Para renovar a auditoria, reconciliação histórica e o bootstrap controlado, executar o runner desta revisão. Ele compara a recomputação com a cópia histórica antes de publicar, e conserva literalmente as colunas antigas aprovadas; divergência interrompe a publicação. Mudanças deliberadas de fonte/período exigiriam uma revisão separada dessa referência histórica, sem sobrescrever silenciosamente o histórico.

Esta etapa termina aqui. A futura Etapa 2 deverá adaptar medidas e apresentações aos campos acima, informar as bases de cada estatística e evitar converter −100% em “100% mais no quente”.
