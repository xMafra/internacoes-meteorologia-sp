# Manifesto para atualização posterior do Power BI — Etapa 2

Etapa 1C, decisão do autor em 05/10/2026. **Este documento orienta a próxima etapa; nenhum arquivo do Power BI foi editado nesta promoção.**

## Fontes e método

Pasta oficial: `C:\Users\Guilherme\Desktop\TCC\data\consumption\dashboard_analytics`.

Método principal: `P20_P80_MUNICIPIO`, campo `metodo_termico`; papel `PRINCIPAL`, campo `papel_metodologico`. Baseline fixo, campos `baseline_inicio` e `baseline_fim`: 2015-01-01/2025-12-31. Versão: `versao_metodologia=P20_P80_MUNICIPIO_2015_2025_V1`. Métrica principal: `metrica_principal=D100`.

Há sete fontes analíticas referenciadas nas duas versões locais inspecionadas do PBIP: `contraste_municipio`, `temperatura_faixa`, `temporal_ano`, `sazonalidade_mes`, `cid_resumo`, `demografia_resumo`, `lag_resumo`. `geral_resumo` e `estratos_resumo` também existem oficialmente e devem ser importadas na próxima etapa se forem necessárias à apresentação. O esquema detalhado e as colunas adicionadas por tabela estão no relatório da promoção e em `data/validation/promocao_metodologia_final/resultado_execucao.json`.

## Classificação e thresholds — campos exatos

| Tabela | Campos e significado |
|---|---|
| `dim_threshold_temperatura_municipio` | `codigo_municipio`, `nome_municipio`, `baseline_inicio`, `baseline_fim`, `dias_temperatura_valida`, `p20`, `p80`, `mediana`, `temperatura_media`, `temperatura_minima`, `temperatura_maxima`, `threshold_degenerado` |
| `classificacao_termica_dia` | `municipio_dia_id`, `codigo_municipio`, `data`, `ano`, `mes`, `temperatura_media_dia_c`, `temperatura_valida`, `p20`, `p80`, **`classificacao_termica`** |
| Classificação principal | `RELATIVAMENTE_FRIO`, `RELATIVAMENTE_QUENTE`, `INTERMEDIARIO`, `SEM_TEMPERATURA_VALIDA` |

Relacionar a dimensão por `codigo_municipio`; a classificação diária pode se relacionar pela chave `municipio_dia_id` à fato já existente. Não contar dias duplicados por expansão de CID. A dimensão não tem internações. `classe_A/B/C`, presentes na classificação, são códigos internos diagnósticos; **`classificacao_termica` é o campo principal para apresentação**. `p20_mes`, `p80_mes` e `classe_C` referem-se somente à sensibilidade mensal, nunca à definição principal. Thresholds não devem ser calculados novamente quando um slicer de ano/diagnóstico/demografia mudar.

## Magnitude, médias e bases — campos exatos

| Conceito | Detalhe `contraste_municipio` | Resumos geral/anual/CID/demografia/estratos/lag |
|---|---|---|
| D100 principal | `diferenca_por_100_municipio_dias` | **`mediana_diferenca_por_100`** |
| Dispersão municipal | Não aplicável ao ponto municipal | **`q1_diferenca_por_100`, `q3_diferenca_por_100`** |
| D absoluto | `diferenca_absoluta` | `mediana_diferenca_absoluta_elegiveis` |
| Média fria principal | `media_frio_municipio` | `mediana_media_frio_elegiveis` |
| Média quente principal | `media_quente_municipio` | `mediana_media_quente_elegiveis` |
| Percentual complementar | `diferenca_relativa` | `mediana_diferenca_relativa` |
| Base D100 | `contraste_absoluto_valido`, `elegivel_amostra` | **`municipios_validos_magnitude`** (alias de `municipios_validos_absoluto`) |
| Base percentual | `contraste_relativo_valido` | `municipios_validos` |
| Eventos no período | `internacoes_total` | `internacoes_total` |
| Eventos térmicos | `internacoes_frio`, `internacoes_quente` | `internacoes_frio_validos_absoluto`, `internacoes_quente_validos_absoluto` |
| Dias por condição | `dias_frios`, `dias_quentes` | `dias_frios_total`, `dias_quentes_total`; bases elegíveis em `dias_frios_validos_absoluto`, `dias_quentes_validos_absoluto` |
| Direção compatível | `direcao_diferenca` | `direcao_diferenca` |
| Direção explícita | **`direcao_termica`** | **`direcao_termica`** |

`direcao_diferenca=FRIO/QUENTE/IGUAL` preserva os códigos históricos. Nas tabelas principais, FRIO/QUENTE significam **relativamente frio/quente**. `direcao_termica` explicita isso com `RELATIVAMENTE_FRIO/RELATIVAMENTE_QUENTE/IGUAL`. A direção usa D/D100, não o percentual. NULL é mantido quando não há base para uma métrica.

Colunas históricas `mediana_media_frio`, `mediana_media_quente` e `mediana_diferenca_absoluta` continuam usando a base com percentual definido. Para magnitude principal, usar as versões `_elegiveis` e D100 acima. `q1`/`q3`, `mediana_diferenca_relativa`, `minimo`/`maximo` são percentuais complementares; não renomeá-los como D100. “Absoluto” nos nomes de base significa D absoluto válido, não Método A.

## Consistência e zeros

| Conceito | Campos |
|---|---|
| Consistência, contagens | `municipios_frio_consistencia`, `municipios_quente_consistencia`, `municipios_pequena_consistencia` |
| Consistência, percentuais | `percentual_frio_consistencia`, `percentual_quente_consistencia`, `percentual_pequena_consistencia` |
| Sinais absolutos | `municipios_maior_frio_absoluto`, `municipios_maior_quente_absoluto`, `municipios_medias_iguais_absoluto` |
| Flags de zero | `media_frio_zero`, `media_quente_zero`, `ambas_medias_zero` |
| Contadores de zero | `municipios_frio_zero`, `municipios_quente_zero`, `municipios_ambas_medias_zero`, `municipios_so_frio`, `municipios_so_quente` |
| Detalhe municipal | `classe_consistencia`, `diferenca_zero`, flags e elegibilidade |

A consistência usa R definido e diferença pequena entre −1%/+1%, inclusive. Seus percentuais somam 100% nessa base. Os sinais estritos usam a base D. As bases não são intercambiáveis. Flags nos resumos referem-se à mediana das médias; os contadores mostram quantos municípios possuem cada situação. D100 zero com R=NULL deve ser apresentado sem criar percentual artificial. Percentuais técnicos de −100% e bases pequenas continuam visíveis como informação complementar.

## Sensibilidades

Na mesma pasta oficial, separadas por nomes e papel:

- **A:** `sensibilidade_absoluto_contraste_municipio`, `sensibilidade_absoluto_geral_resumo`, `sensibilidade_absoluto_temporal_ano`, `sensibilidade_absoluto_cid_resumo`, `sensibilidade_absoluto_demografia_resumo`, `sensibilidade_absoluto_estratos_resumo`, `sensibilidade_absoluto_lag_resumo`; `papel_metodologico=SENSIBILIDADE_ABSOLUTA`.
- **C:** `sensibilidade_mes_thresholds`, `sensibilidade_mes_geral_resumo`, `sensibilidade_mes_temporal_ano`, `sensibilidade_mes_cid_resumo`; `papel_metodologico=SENSIBILIDADE_SAZONAL`. CID cobre capítulos e J09–J18/J20–J22.
- **Histórico experimental:** `data/validation/auditoria_definicao_temperatura`; permanece evidência, sem substituir as fontes oficiais.

Conservar a discussão das direções anuais A/B diferentes em 2019 e 2022 e das mudanças B/C respiratórias. Não escrever que C “controla a sazonalidade”; é sensibilidade por município e mês, sem ajuste estatístico completo.

## Uso posterior nas Páginas 1–6

Atualizar os contrastes, rankings e cards para D100, com quartis e `municipios_validos_magnitude`, usando dias relativamente frios/quentes. Mostrar o percentual somente em tooltip/complemento, com sua base própria. Para recortes CID/idade/sexo, usar os resumos existentes conforme seu grão; não somar medianas. Para filtros anuais específicos de CID/demografia, os resumos globais atuais não possuem esse grão: calcular a partir das fatos + classificação pronta ou materializar um recorte adicional validado, sem recalibrar thresholds.

As medidas DAX atuais sobre fatos ainda contêm limites físicos 20/25 e não são automaticamente convertidas pela regeneração dos Parquets. Importar a dimensão/classificação e revisar essas medidas na Etapa 2. A atualização dos dados isoladamente não certifica a coerência das páginas ainda antigas.

`temperatura_faixa` mantém <15, 15–19,9, 20–24,9, 25–29,9 e ≥30 °C, como análise física complementar. `sazonalidade_mes` mantém descrição mensal; não é classificação binária. Lags usam os dados oficiais B de `lag_resumo`, com `defasagem`, `ordem_defasagem`, `chave_alvo`, D100 e quartis. Intervalos exploratórios do bootstrap, quando calculados, ficam em `ic95_diferenca_por_100_inferior/superior` e não substituem Q1/Q3.

D100 é diferença de internações por 100 município-dias, **não taxa populacional**. Não atribuir causalidade nem interpretar a mudança de direção de C como correção definitiva de B. Todas as mudanças de apresentação descritas aqui ficam para a próxima etapa.
