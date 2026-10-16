# Relatório de reformulação do dashboard — seis perguntas de pesquisa

## 1. Escopo

Este documento registra a implementação da nova versão do dashboard do TCC no
projeto Power BI `Dashboard/Dash_TCC.pbip`. O relatório foi reorganizado em
seis páginas, cada uma vinculada diretamente a uma pergunta de pesquisa. A
evidência principal passou a ser a comparação intramunicipal; indicadores
estaduais agregados foram mantidos apenas como contexto secundário.

Não foram alterados os limiares analíticos, o período do estudo ou a arquitetura
em camadas. A DAG existente recebeu somente duas tarefas no TaskGroup
`consumption`: materialização das tabelas compactas do dashboard e seu gate de
Data Quality.

## 2. Regras metodológicas preservadas

- Período analítico: 01/01/2015 a 31/12/2025.
- Dia frio: temperatura média diária `< 20 °C`.
- Dia quente: temperatura média diária `>= 25 °C`.
- Elegibilidade: pelo menos 30 dias frios e 30 dias quentes.
- Para lags, a elegibilidade é recalculada independentemente em D0, D-1, D-3 e
  D-7.
- Unidade do contraste: município elegível.
- Resumo principal: distribuição dos contrastes municipais, sem ponderação
  pelo volume de internações.
- Diferença absoluta: `média_frio_município - média_quente_município`.
- Diferença relativa: `diferença_absoluta / média_quente_município`.
- Aproximadamente zero: diferença relativa entre -1% e +1%, inclusive.

As saídas descrevem associações observacionais. Nenhuma página ou medida foi
construída para sustentar interpretação causal.

## 3. Camada analítica materializada

O script `src/consumption/dashboard_analytics.py` gera sete tabelas Parquet em
`data/consumption/dashboard_analytics`:

| Tabela | Linhas | Finalidade |
|---|---:|---|
| `contraste_municipio` | 645 | Métricas frio/quente e contraste de cada município |
| `temperatura_faixa` | 5 | Faixas de temperatura, visão municipal e agregada |
| `temporal_ano` | 11 | Distribuição intramunicipal anual |
| `sazonalidade_mes` | 12 | Contexto mensal de temperatura e internações |
| `cid_resumo` | 2.077 | Capítulos, grupos e categorias CID-10 |
| `demografia_resumo` | 20.780 | CID opcional × cinco faixas etárias × dois sexos |
| `lag_resumo` | 16 | Quatro alvos analíticos × quatro defasagens |

Os numeradores CID e demográficos são completados com zero sobre o painel de
exposição município-dia. Assim, dias sem evento continuam no denominador. As
estatísticas de distribuição são calculadas depois do contraste municipal,
evitando que municípios de maior volume dominem o resultado.

As tabelas foram adicionadas ao modelo semântico como tabelas Import em TMDL.
Uma única relação nova foi necessária: `analitico_contraste_municipio` para
`dim_municipio`, pela chave municipal. Os demais resumos são autocontidos e
usam medidas DAX próprias.

## 4. Páginas implementadas

### 4.1 Temperatura e internações

Pergunta: qual é a associação observada entre temperatura e internações?

A página apresenta mediana, Q1, Q3, municípios válidos, distribuição por
classe de contraste e comparação por faixa de temperatura. O indicador
estadual agregado está identificado como visão secundária. A métrica principal
é a mediana dos contrastes intramunicipais.

### 4.2 Consistência ao longo do tempo

Pergunta: a associação é consistente entre os anos?

A página usa o ano no eixo, sem um slicer que esconda a sequência temporal.
Mostra Q1, mediana, Q3, linha de referência zero e percentuais de municípios
com contraste positivo e negativo. O contexto sazonal mensal é exibido em um
visual separado.

### 4.3 Diagnósticos / CID

Pergunta: quais grupos de diagnóstico apresentam diferenças mais marcantes?

Foram implementados seletores de capítulo, grupo e categoria; matriz com
hierarquia de drill-down; dispersão entre contraste e volume; e ranking. O
contraste é intramunicipal e o volume aparece apenas como contexto.

### 4.4 Faixa etária e sexo

Pergunta: a associação difere por faixa etária e sexo?

A página compara cinco faixas etárias e os rótulos legíveis Masculino e
Feminino. A seleção CID é opcional e segue a mesma hierarquia. Sem seleção, as
medidas usam todas as internações.

### 4.5 Defasagem temporal

Pergunta: a temperatura de dias anteriores apresenta associação diferente da
temperatura do próprio dia?

Os visuais comparam D0, D-1, D-3 e D-7 por mediana e intervalo interquartil,
percentuais positivos/negativos e quadro auditável. O seletor contém:

- todas as internações;
- Capítulo X — doenças do aparelho respiratório;
- J09-J18 — influenza e pneumonia;
- J20-J22 — outras infecções agudas das vias aéreas inferiores.

Cada combinação alvo × lag é resumida a partir de municípios elegíveis para
aquela defasagem, sem ponderação por volume.

### 4.6 Análise territorial

Pergunta: como a associação se distribui entre os municípios?

O mapa usa latitude e longitude do município, classe de contraste na legenda
e volume no tamanho. Contraste, médias frio/quente, diferença absoluta e dias
elegíveis estão nos tooltips. A página também contém dispersão e tabela
ordenável para auditoria.

## 5. Evidências descritivas usadas para conferir o painel

Os valores abaixo são referências de reconciliação, não conclusões causais:

- 645 municípios na tabela territorial; 640 com contraste relativo válido no
  recorte geral.
- Distribuição geral: Q1 `-6,29%`, mediana `-1,89%`, Q3 `2,05%`;
  `39,22%` positivos e `60,78%` negativos.
- Mediana anual de 2020: `-15,30%`; de 2025: `7,91%`.
- Medianas CID: Capítulo X `17,95%`; J09-J18 `22,66%`; J20-J22 `53,89%`.

Medianas por defasagem:

| Alvo | D0 | D-1 | D-3 | D-7 |
|---|---:|---:|---:|---:|
| Todas as internações | -1,89% | -1,51% | 1,75% | 1,80% |
| Capítulo X | 17,95% | 18,03% | 22,21% | 26,11% |
| J09-J18 | 22,66% | 23,56% | 28,95% | 33,79% |
| J20-J22 | 53,89% | 57,62% | 47,98% | 56,64% |

## 6. Validação realizada

### 6.1 Dados e regras analíticas

- `DQ_DASHBOARD_ANALYTICS=PASS`.
- Contagens das sete saídas reconciliadas.
- 2.077 chaves CID únicas.
- 20.780 combinações demográficas, sem duplicatas.
- Nenhum município válido violou o mínimo de 30 dias frios e 30 dias quentes.
- Todos os 16 valores de referência alvo × lag foram reconciliados.
- Dois testes novos de `tests/test_dashboard_analytics.py` aprovados.
- Seis testes de `tests/test_analytic_metrics.py` aprovados.

### 6.2 Modelo e relatório Power BI

- Estrutura PBIP/TMDL: `Strong`, 100/100.
- Boas práticas do modelo: 100 pontos, zero achados.
- Esquema dos visuais: zero achados.
- 106 vínculos de campos do relatório conferidos contra o TMDL, sem campo ou
  tabela ausente.
- Seis páginas na ordem prevista, com 9 a 12 visuais por página.
- Todos os visuais permanecem dentro do canvas 1280 × 720.
- O Power BI Desktop iniciou o PBIP e carregou o modelo em um workspace local
  do Analysis Services. A metadata carregada contém as sete tabelas
  `analitico_*` e nenhuma medida analítica com erro de expressão.

A revisão automática de densidade marcou um aviso médio porque o validador
conta os visuais do relatório inteiro. A inspeção por página confirma no máximo
12 visuais, portanto o aviso não representa sobreposição nem extrapolação do
canvas. A validação live por ADOMD.NET não foi executada porque essa biblioteca
não está instalada no computador; a carga real do modelo no Desktop e a
inspeção do metadata local substituíram essa verificação.

## 7. Execução e manutenção

Após reconstruir a camada Consumption, execute:

```powershell
docker compose exec -T spark spark-submit src/consumption/dashboard_analytics.py
docker compose exec -T spark spark-submit src/quality/consumption/dq_dashboard_analytics.py
```

Na DAG `tcc_pipeline`, essas etapas já são executadas entre o DQ principal de
Consumption e a validação final das perguntas de pesquisa. Depois da geração,
abra `Dashboard/Dash_TCC.pbip` e use **Página Inicial > Atualizar** no Power BI
Desktop. Essa primeira atualização é obrigatória porque o PBIP armazena a
definição do modelo, mas não inclui o cache das tabelas Import. Antes dela, é
esperado que o Desktop sinalize tabelas incompletas e objetos calculados ainda
não processados.

## 8. Limitações

O desenho não ajusta os contrastes por população, estrutura etária municipal,
sazonalidade epidemiológica, circulação viral, poluição, oferta hospitalar ou
outras variáveis de confusão. As médias são por município-dia e a interpretação
deve permanecer descritiva e associativa.
