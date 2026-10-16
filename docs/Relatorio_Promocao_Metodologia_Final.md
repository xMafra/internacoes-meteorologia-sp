# Relatório da promoção da metodologia final — Etapa 1C

05/10/2026. Decisão explícita do autor: método B principal, D100 como magnitude. Status final: **PASS**. Nenhum arquivo do Power BI foi alterado.

## 1. Arquivos alterados

- `src/consumption/metodologia_termica.py`
- `src/consumption/promocao_metodologia.py`
- `src/consumption/dashboard_analytics.py`
- `src/consumption/revisao_frio_quente.py`
- `src/quality/consumption/thermal_checks.py`
- `src/quality/consumption/dq_metodologia_final.py`
- `src/quality/consumption/dq_dashboard_analytics.py`
- `src/validation/auditoria_definicao_temperatura.py`
- `tests/test_methodology_promotion.py`

Novos documentos: `Metodologia_Analitica_Final.md`, `Manifesto_Atualizacao_PowerBI_Metodologia_Final.md` e este relatório. O relatório e os dados históricos da Etapa 1B foram preservados por SHA-256.

## 2. Código promovido da auditoria

Foram extraídas, com a mesma lógica validada, funções de período, validade, percentis, classificação, exposição, médias municipais, numeradores CID, cruzamentos/marginais e rótulos. A regra agora reside em `metodologia_termica.py`, importada tanto pela auditoria quanto pela produção. Os DQs térmicos/reconciliação são compartilhados em `thermal_checks.py`. A auditoria usa as sensibilidades A como referência histórica após a promoção. Suas saídas anteriores não foram regravadas.

`dashboard_analytics.main` e `gerar_saidas` usam a implementação oficial B. Auxiliares absolutos antigos permanecem para APIs históricas/testes; não definem o caminho oficial. `revisao_frio_quente.py`, runner histórico da Etapa 1, bloqueia uma sobrescrita acidental de B por A após a promoção.

## 3. Tabelas oficiais regeneradas

| Tabela | Linhas | Semântica final |
|---|---|---|
| contraste_municipio | 645 | B, principal D100 |
| geral_resumo | 1 | B, principal D100 |
| temporal_ano | 11 | B, principal D100 |
| cid_resumo | 2077 | B, principal D100 |
| demografia_resumo | 20780 | B, principal D100 |
| estratos_resumo | 14546 | B, principal D100 |
| lag_resumo | 16 | B, principal D100 |
| temperatura_faixa | 5 | Descrição complementar preservada |
| sazonalidade_mes | 12 | Descrição complementar preservada |

Todas as colunas históricas foram mantidas. Nas primeiras sete tabelas, as condições térmicas passam de A para B; médias, eventos, dias, D, D100, relativo, quartis, direção, bases, zeros e consistência são recalculados. Os ICs exploratórios no escopo existente são recalculados para B. Os dois complementos preservam seus valores físicos/mensais. Nenhuma fato original foi substituída.

| Tabela | Colunas adicionadas |
|---|---|
| contraste_municipio | dias_temperatura_valida, dias_sem_temperatura, anos_frios, anos_quentes, dias_intermediarios, anos_intermediarios, dias_empatados_p20, dias_empatados_p80, possui_threshold_degenerado, percentual_dias_frios, percentual_dias_quentes, percentual_dias_intermediarios, elegivel_exposicao, motivo_nao_elegivel, equilibrio_exposicao, chave_alvo, p20, p80, threshold_degenerado, metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, direcao_termica |
| geral_resumo | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, municipios_validos_magnitude, direcao_termica |
| temporal_ano | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, municipios_validos_magnitude, direcao_termica |
| cid_resumo | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, municipios_validos_magnitude, direcao_termica |
| demografia_resumo | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, municipios_validos_magnitude, direcao_termica |
| estratos_resumo | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, municipios_validos_magnitude, direcao_termica |
| lag_resumo | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia, municipios_validos_magnitude, direcao_termica |
| temperatura_faixa | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia |
| sazonalidade_mes | metodo_termico, papel_metodologico, metrica_principal, unidade_magnitude, baseline_inicio, baseline_fim, versao_metodologia |

Novas tabelas na pasta oficial:

| Tabela | Linhas |
|---|---|
| dim_threshold_temperatura_municipio | 645 |
| sensibilidade_mes_thresholds | 7740 |
| classificacao_termica_dia | 2591610 |
| sensibilidade_mes_geral_resumo | 1 |
| sensibilidade_mes_temporal_ano | 11 |
| sensibilidade_mes_cid_resumo | 24 |
| sensibilidade_absoluto_contraste_municipio | 645 |
| sensibilidade_absoluto_geral_resumo | 1 |
| sensibilidade_absoluto_temporal_ano | 11 |
| sensibilidade_absoluto_cid_resumo | 2077 |
| sensibilidade_absoluto_demografia_resumo | 20780 |
| sensibilidade_absoluto_estratos_resumo | 14546 |
| sensibilidade_absoluto_lag_resumo | 16 |

## 4. Classificação principal

`classificacao_termica_dia.classificacao_termica`: RELATIVAMENTE_FRIO se T≤P20; RELATIVAMENTE_QUENTE se T≥P80; INTERMEDIARIO nos demais dias válidos; SEM_TEMPERATURA_VALIDA nos inválidos. Empates incluídos. Estratos degenerados são intermediários, com flag. A classificação interna de A/B/C continua disponível como diagnóstico, sem promover A/C à condição principal.

## 5. Thresholds oficiais

`dim_threshold_temperatura_municipio`: código/nome do município, baseline, N de temperaturas válidas, p20/p80, mediana/média, min/max, demais percentis e desvio-padrão. Sem internações. Baseline fixo 2015–2025, excluindo os sete dias técnicos de 2014. Foram reconciliados todos os municípios e todos os thresholds comuns com a auditoria. Não há P20=P80 nos dados reais auditados. Os filtros não recalibram percentis.

## 6. D100

Primeiro médias municipais, depois D=F−Q e D100=100D; somente então mediana/Q1/Q3 entre municípios. `metrica_principal=D100`; base explícita `municipios_validos_magnitude`. Não é taxa populacional. Os aliases e campos exatos estão no manifesto para Power BI.

## 7. Diferença relativa complementar

R=D/Q quando Q>0, senão NULL. Preservada para comparação histórica, tooltip e consistência; não é a principal magnitude. `municipios_validos` é a base percentual. `municipios_validos_absoluto` e seu alias `municipios_validos_magnitude` são a base D100 e podem ser maiores.

## 8. Tratamento de zero

Frio zero/quente positivo: D/D100 válidos, R=−100% e flag. Quente zero/frio positivo: D/D100 válidos e R=NULL. Ambas zero: D=D100=0 e R=NULL. Sem pseudocount, NaN ou infinito; flags, contagens e fórmulas verificados no detalhe e nos resumos. Valores raros não são corrigidos manualmente para tornar a apresentação mais agradável.

## 9. Consistência territorial

Mantida a regra exclusiva na base percentual válida: FRIO se R>+1%, QUENTE se R<−1%, PEQUENA entre −1%/+1%, inclusive. D100 define a direção da magnitude. `direcao_diferenca` conserva FRIO/QUENTE/IGUAL por compatibilidade; `direcao_termica` explicita relativamente frio/quente. Contagens de sinais absolutos também estão disponíveis.

## 10. Método A preservado

Sete tabelas `sensibilidade_absoluto_*`, com contagens, números e bootstrap históricos idênticos às tabelas anteriores, verificados por chaves e comparação NULL-safe. São resumos compactos, sem duplicar fatos. As mudanças anuais A/B em 2019 e 2022 continuam nas tabelas. Backup pré-promoção em `data/validation/promocao_metodologia_final/producao_antes`.

## 11. Sensibilidade município × mês preservada

`SENSIBILIDADE_SAZONAL` identifica thresholds mensais, geral, anual e CID de capítulos/grupos respiratórios. Recalculados com a regra compartilhada e reconciliados integralmente com C da auditoria. Não houve reconstrução completa CID×demografia para C. A mudança de direção respiratória B/C permanece: Capítulo X −0,9330; J09–J18 −0,6305; J20–J22 −0,000929 em C. Isso reduz separação inverno/verão, sem ajuste estatístico completo de sazonalidade.

## 12. Resultado geral oficial

| Métrica | Valor |
|---|---|
| municipios_validos_magnitude | 645 |
| mediana_diferenca_por_100 | -3,8619 |
| q1_diferenca_por_100 | -19,2922 |
| q3_diferenca_por_100 | 4,8750 |
| mediana_diferenca_relativa | -0,0140 |
| direcao_termica | RELATIVAMENTE_QUENTE |
| percentual_frio_consistencia | 33,4884 |
| percentual_quente_consistencia | 54,1085 |
| percentual_pequena_consistencia | 12,4031 |
| internacoes_total | 27671456 |
| dias_frios_total | 506971 |
| dias_quentes_total | 507444 |

Percentual relativo é armazenado como fração; multiplicar por 100 apenas para exibição. As medidas foram reconciliadas integralmente com a saída B auditada, sem hardcode em produção.

## 13. Resultados respiratórios oficiais

| Recorte | D100 | R (%) | Direção | % frio | N D100 | N R |
|---|---|---|---|---|---|---|
| 10 — Capítulo X - Doenças do aparelho respiratório | 4,5164 | 17,5004 | RELATIVAMENTE_FRIO | 85,2713 | 645 | 645 |
| J09-J18 — Influenza [gripe] e pneumonia | 2,8097 | 23,2535 | RELATIVAMENTE_FRIO | 82,3256 | 645 | 645 |
| J20-J22 — Outras infecções agudas das vias aéreas inferiores | 0,5089 | 50,7692 | RELATIVAMENTE_FRIO | 75,9582 | 645 | 574 |

J20–J22 tem base relativa menor e muitos zeros; seu percentual não substitui a magnitude D100. Os resultados B foram reconciliados em todas as categorias, não só nas três linhas prioritárias.

## 14. Resultados demográficos

| Idade | Sexo | D100 | Direção | N magnitude | N relativo |
|---|---|---|---|---|---|
| 0-17 | Masculino | -0,2128 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 0-17 | Feminino | -0,1700 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 18-39 | Masculino | -1,0068 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 18-39 | Feminino | -1,1981 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 40-59 | Masculino | -0,4813 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 40-59 | Feminino | -0,5215 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 60-79 | Masculino | -0,2508 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 60-79 | Feminino | -0,2571 | RELATIVAMENTE_QUENTE | 645 | 645 |
| 80+ | Masculino | 0,1586 | RELATIVAMENTE_FRIO | 645 | 645 |
| 80+ | Feminino | 0,0000 | IGUAL | 645 | 645 |

Reconciliação integral de demografia e marginais com B da auditoria. Mulheres 80+ mantêm o resultado mediano zero. Marginais de sexo e idade são calculadas por contagens municipais antes das médias, nunca somando medianas.

## 15. Testes

61 executados/aprovados; 0 falhas e 0 erros. Preservados os 44 testes das Etapas 1 e 1B; acrescentados 17 testes de integração: baseline/2014, ano fixo, limites/empates, intermediários, D100 e mediana municipal, papéis A/C, terminologia oficial, lags por data exata e técnicos, D0, degeneração e separação das bases D100/percentual. Log: `testes.log`.

## 16. Data Quality

PASS antes da publicação oficial, nas tabelas físicas preparadas e na leitura final publicada. Thresholds ordenados/não nulos/dentro de min/max; baseline fixo; exclusividade e chaves diárias; fórmulas D/D100/R; NULLs, zeros e flags; direção; elegibilidade; cardinalidades e compatibilidade de colunas. Reconciliados geral/anual/CID/demografia/marginais B e C com a auditoria, e A com o snapshot pré-promoção. D0 também reconcilia com geral e os três recortes respiratórios. Log: `dq_final.log`.

## 17. Performance e publicação

| Etapa final | Segundos |
|---|---|
| baseline_classificacao_geral_anual | 45,308 |
| numeradores_compartilhados | 74,131 |
| cid_dq_resumo_bootstrap | 26,014 |
| demografia_dq_resumo_bootstrap | 196,712 |
| estratos_dq_resumo_bootstrap | 159,677 |
| sensibilidade_mensal_lags_complementares | 54,556 |
| calculo_total | 557,482 |
| materializacao_validada | 21,437 |
| publicacao | 16,776 |
| Total da execução final | 642,881 |

Tempos da execução bem-sucedida, sem testes/DQ externo/documentação. Spark nativo, sem UDF ou loops por município. Numeradores A/B compartilhados; intermediários e resumos persistidos; C com escopo limitado. Todos os resultados passam DQ nos arquivos preparados, independentes da linhagem compartilhada das consultas, antes de publicar. Escrita atômica por tabela com backup anterior e restauração das tabelas antigas em falha de publicação. Não é uma transação multitabular de banco de dados. Consumidores devem ler após a conclusão do lote. Logs e esquema exato no diretório de evidências.

Durante desenvolvimento, a reconciliação entre D0 e CID apontou divergência nos recortes respiratórios tanto em memória quanto nos arquivos preparados e bloqueou a publicação nas duas tentativas. As saídas gerais/CID/demografia/marginais e as sensibilidades já reconciliavam. O diagnóstico isolado reproduziu os resultados respiratórios esperados com ambas as fontes. A preparação dos lags foi então separada da linhagem compartilhada dos cruzamentos CID: usa a fato CID e a dimensão originais, entradas mínimas materializadas e a mesma classificação fixa. A execução completa posterior e a leitura física independente passaram todos os checks, incluindo totais de internações em D0. O teste de integração cobre magnitudes, zeros e eventos. Nenhum número foi corrigido manualmente, nenhum threshold foi ajustado e nenhuma regra de DQ foi relaxada. A causa interna específica da diferença na execução compartilhada não foi atribuída a um defeito confirmado do Spark. Registros: `tentativa_inicial.log`, `tentativa_fisica.log`, `diagnostico_D0.log` e `diagnostico_fonte_lag.log`.

## 18. Limitações

Análise descritiva intramunicipal, sem população como denominador, regressão, GAM, DLNM ou inferência causal. Sazonalidade, vírus, poluição, oferta hospitalar, fatores socioeconômicos e outros componentes não foram isolados. Cobertura meteorológica incompleta, estações compartilhadas/históricas e dados raros/esparsos limitam interpretação. IQR municipal não é IC. Bootstrap permanece exploratório no escopo já existente.

## 19. Pendências para Power BI e preservação

Nenhum arquivo de Power BI foi alterado. Os 453 arquivos protegidos (Power BI, fatos, evidências históricas e relatório da Etapa 1B) têm SHA-256 idêntico. Bronze/Silver/Gold não foram reconstruídos; nenhuma fonte foi baixada. As medidas e títulos atuais do PBIP ainda requerem a Etapa 2: D100 com base magnitude, quartis, classe relativa, thresholds fixos, zeros e percentual complementar. Algumas medidas DAX ainda contêm 20/25 e não são convertidas por esta promoção. Não considerar a apresentação atual atualizada apenas por trocar Parquets.

A lista de fontes/campos/grãos e sensibilidades está em `Manifesto_Atualizacao_PowerBI_Metodologia_Final.md`. A metodologia técnica está em `Metodologia_Analitica_Final.md`. Esta etapa termina com os dados oficiais e a documentação prontos; nenhuma página foi reconstruída.
