# Metodologia analítica final — TCC Saúde e Clima

Decisão do autor na Etapa 1C, 05/10/2026. Método principal: **B — P20/P80 municipal**. Métrica principal: **D100**. A auditoria histórica da Etapa 1B permanece preservada em `Relatorio_Auditoria_Definicao_Temperatura.md`.

## Pergunta principal

Como as internações hospitalares diferem entre períodos relativamente mais frios e mais quentes nos municípios do Estado de São Paulo entre 2015 e 2025?

## Fontes, período e baseline

Utiliza-se o painel materializado `fato_municipio_dia`, com calendário município-data e contagens de internações, e a fato CID para detalhamento de diagnóstico, idade e sexo. As temperaturas já consolidadas mantêm a associação histórica município–estação; não são recalculadas a partir de internações. Estações podem atender mais de um município.

O baseline climático é **01/01/2015 a 31/12/2025**, fixo. Os percentis são calculados sobre dias com temperatura conhecida e finita e disponibilidade meteorológica válida. Registros duplicados município-data são rejeitados. O período técnico de dezembro de 2014 não entra nos percentis. Filtros de ano, CID, sexo, idade ou município selecionam dias/recortes da classificação pronta; não recalibram os thresholds.

`src/consumption/metodologia_termica.py` contém a regra única extraída da implementação auditada. A produção e a auditoria a importam. O cálculo exato usa a função Spark nativa `percentile`, interpolada na posição (N−1)p. Os resumos municipais mantêm `percentile_approx` com precisão 10000 e a convenção empírica da etapa anterior. As duas operações têm finalidades distintas.

## Definição térmica oficial

Para cada município m: relativamente frio se T≤P20m; relativamente quente se T≥P80m; intermediário nos demais dias válidos. Empates nos thresholds são incluídos integralmente, sem sorteio para forçar 20%. Se P20=P80, o estrato é classificado como intermediário e sinalizado; não há dupla classificação. Dias sem temperatura válida recebem `SEM_TEMPERATURA_VALIDA` e não compõem os numeradores ou denominadores térmicos.

Os thresholds são materializados em `data/consumption/dashboard_analytics/dim_threshold_temperatura_municipio`; a classificação oficial em `classificacao_termica_dia`. A dimensão contém estatísticas de temperatura e baseline, sem internações.

## Unidades, elegibilidade e ordem de cálculo

Unidade observacional: **município-dia**. Unidade primária de comparação: **município**. Exige-se pelo menos 30 dias em cada condição para o município/recorte temporal. A elegibilidade é reavaliada nos anos e nos lags; os thresholds permanecem fixos. A ausência de internação em um dia observado é zero; ausência de meteorologia não é imputada como zero de temperatura. Nas fatos CID esparsas, o denominador provém do calendário, incluindo dias sem eventos do diagnóstico.

Calculam-se Fm=eventos relativamente frios/dias relativamente frios e Qm=eventos relativamente quentes/dias relativamente quentes. Em seguida, Dm=Fm−Qm e **D100m=100Dm**. Só depois calculam-se mediana, Q1 e Q3 entre municípios elegíveis, sem ponderação por tamanho municipal. Mediana das diferenças não é diferença das medianas.

**D100 é diferença de internações por 100 município-dias; não é taxa populacional.** Valores positivos indicam média observada maior nos dias relativamente frios; negativos, nos relativamente quentes; zero indica nenhuma diferença na média observada. Não representa atribuição causal nem diferença entre riscos populacionais.

## Resumo e bases

A magnitude principal usa `mediana_diferenca_por_100`, `q1_diferenca_por_100`, `q3_diferenca_por_100` e a base `municipios_validos_magnitude`/`municipios_validos_absoluto`. “Absoluto” nesse nome histórico significa diferença absoluta D, não Método A. Q1/Q3 descrevem a distribuição municipal e não são intervalos de confiança.

As médias principais são `mediana_media_frio_elegiveis` e `mediana_media_quente_elegiveis`. Contagens, dias, municípios válidos e flags de zero ficam disponíveis. Colunas históricas são mantidas por compatibilidade; sua semântica térmica nas tabelas principais passa a ser B. A base do percentual, `municipios_validos`, pode ser menor do que a base de D100.

## Diferença relativa e zeros

Rm=(Fm−Qm)/Qm quando Qm>0; caso contrário, NULL. A mediana relativa é complementar, sem substituir D100. Frio zero e quente positivo produz D/D100 válidos e R=−100%; quente zero e frio positivo mantém D/D100 e R=NULL; ambas médias zero produz D=D100=0 e R=NULL. Não se adicionam pseudocounts e não se geram infinitos. Flags e bases devem acompanhar resultados esparsos.

## Consistência territorial

Na base com R definido, as classes são exclusivas: FRIO se R>+1%; QUENTE se R<−1%; PEQUENA se −1%≤R≤+1%, inclusive. Essa regra descreve consistência territorial, não magnitude nem relevância clínica. Disponibilizam-se também contagens dos sinais estritos de D na base absoluta. `FRIO` e `QUENTE`, nas colunas históricas de B, significam condições relativamente frias/quentes; `direcao_termica` torna essa interpretação explícita.

## Lags

D0, D−1, D−3 e D−7 usam a temperatura atribuída ao município na data civil da exposição: lookup município + data-alvo, nunca linha anterior. Dentro do período, a classificação consulta a série diária e sua validade. Nos dias técnicos de 2014, somente como exposição defasada, utiliza-se o lag já materializado pela lógica histórica existente. Em ambos os casos, compara-se a temperatura com o mesmo P20/P80 de 2015–2025. Não há percentis por lag nem mudança na fato original. A preparação dos lags usa entradas mínimas materializadas, independentes da linhagem dos cruzamentos CID. D0 deve reconciliar magnitudes, bases e eventos com o geral e os resumos respiratórios; a publicação é bloqueada diante de divergência.

## Sensibilidades e análises complementares

A — `<20/≥25 °C` — preserva os resultados oficiais anteriores em tabelas `sensibilidade_absoluto_*`, inclusive bootstrap histórico. C — P20/P80 município×mês — preserva/reproduz geral, anual, capítulos e os dois grupos respiratórios em `sensibilidade_mes_*`; não recalcula toda CID×demografia.

Foi realizada análise de sensibilidade definindo extremos térmicos dentro do município e do mês do ano. Isso reduz a separação entre inverno e verão, mas **não constitui ajuste estatístico completo de sazonalidade**. As inversões B/C nos recortes respiratórios permanecem documentadas; C não substitui B. As diferenças anuais A/B em 2019 e 2022 também são resultados, não erros.

`temperatura_faixa` continua com as cinco faixas físicas em °C. `sazonalidade_mes` permanece uma descrição mensal. Nenhuma delas define a comparação binária principal. O bootstrap exploratório da mediana D100 foi recalculado para B no mesmo escopo da Etapa 1: geral, capítulos/grupos CID e quatro alvos demográficos prioritários, 1000 réplicas e seed 20261005. Seus intervalos continuam separados dos quartis municipais.

## Limitações

Estudo descritivo, sem ajuste populacional, modelos de regressão ou inferência causal. Sazonalidade, circulação de vírus, poluição, oferta hospitalar, fatores socioeconômicos, epidemias, calendário, tendências e outros fatores não são isolados pelos contrastes. A cobertura meteorológica é incompleta, estações são compartilhadas e podem mudar historicamente. Temperatura atribuída não é exposição individual. Diagnósticos raros/esparsos podem apresentar muitos zeros, denominadores percentuais inválidos e bases relativas pequenas.

## Validação e apresentação futura

A promoção exige testes anteriores e de integração, DQ de thresholds/classificação/fórmulas/zeros/bases/direção, reconciliação integral com a auditoria B e C e preservação de A. O relatório de execução registra os valores, tempos e arquivos. O manifesto para a próxima etapa informa os campos exatos. Nenhum arquivo do Power BI é modificado nesta etapa; suas medidas e títulos ainda precisarão de atualização separada.
