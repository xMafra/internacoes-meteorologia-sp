# Roteiro para a reunião com a orientadora

**Mensagem principal:** a solução técnica está concluída, incluindo pipeline de dados, orquestração e dashboard. A etapa atual é escrever o TCC e organizar a apresentação acadêmica dos resultados.

## Abertura sugerida

“Concluí a parte prática do trabalho, desde a coleta e o tratamento dos dados até a orquestração e o dashboard. A solução integra internações hospitalares e informações meteorológicas do estado de São Paulo. Hoje quero mostrar a arquitetura, percorrer as análises disponíveis no painel e alinhar a organização da escrita do TCC.”

## Percurso sugerido: 15 a 20 minutos

| Tempo | Conteúdo | Slides |
|---|---|---|
| 2 minutos | Tema, objetivo e fontes | 1–3 |
| 5 minutos | Arquitetura de dados e tecnológica, camadas e qualidade | 4–10 |
| 8 a 10 minutos | Dashboard e demonstração no Power BI | 11–17 |
| 3 minutos | Estrutura do texto e decisões para a escrita | 18–20 |

Os slides 12–17 explicam as seis páginas do painel. Durante a reunião, você pode alternar entre eles e o Power BI. A apresentação contém descrições das funcionalidades e indicadores documentados, sem capturas de tela do dashboard. As notas do PowerPoint incluem detalhes para apoiar a fala.

## Arquitetura tecnológica: slides 5 e 6

**Fala sugerida:** “O Docker Compose organiza os serviços do ambiente. O Airflow coordena a sequência das tarefas e aciona os processamentos Python e Spark no worker. O Spark faz o tratamento, os cruzamentos e as agregações. Os dados ficam nas pastas locais compartilhadas com os contêineres, em arquivos brutos na Bronze e Parquet nas camadas tratadas. O Power BI importa os Parquets do consumo, monta as relações e calcula os indicadores com DAX.”

No desenho, PostgreSQL mantém os metadados do Airflow e Redis atende a fila de tarefas do Celery. JupyterLab, junto ao ambiente Spark de desenvolvimento, apoia exploração e testes. As execuções documentadas de Spark usam modo local no ambiente do worker Airflow. O Power BI Desktop aparece fora dos contêineres e lê os arquivos de consumo por Power Query.

O diagrama distingue o controle das tarefas do caminho dos dados. A DAG salva tem disparo sob demanda (`schedule=None`), e o material não pressupõe atualização automática do Power BI pelo Airflow.

## Demonstração do dashboard

Abra [Dash_TCC.pbip](../../Dashboard/Dash_TCC.pbip) no Power BI Desktop. Mantenha o projeto e suas pastas de relatório e modelo juntos. Use um recorte consistente nas páginas comparadas e confira os filtros de cada página ao navegar.

### 1. Visão geral

Comece pelo período completo. Mostre internações, óbitos, proporção de óbitos, permanência média e valor registrado. Depois percorra a evolução mensal, os municípios com maior volume e a distribuição por capítulo CID-10.

**Fala sugerida:** “Esta página apresenta a dimensão da base e permite entender como o volume e os indicadores mudam conforme o período e o município selecionados.”

**Interação:** selecione um ano e um município e mostre a atualização dos indicadores. Retorne ao recorte que será utilizado nas demais páginas.

### 2. Meteorologia × internações

Mostre a série mensal de internações e temperatura. Explique a média de internações por município-dia observado, as faixas de temperatura e a comparação municipal entre frio e calor.

**Fala sugerida:** “O painel usa a quantidade de município-dias observados como denominador para comparar médias em condições térmicas diferentes. No modelo atual, frio significa temperatura média abaixo de 20 °C e quente significa 25 °C ou mais.”

A faixa intermediária aparece na análise por faixas. Ela fica fora da comparação específica entre frio e quente. A variação relativa usa a média dos dias quentes como referência: `(média no frio − média no quente) / média no quente`.

### 3. CID × temperatura

Apresente o detalhamento por capítulo, grupo e categoria CID-10. A página destaca grupos de doenças respiratórias e categorias de infecções agudas das vias aéreas inferiores.

**Fala sugerida:** “Aqui a mesma comparação térmica ganha detalhamento diagnóstico. É possível observar se a distribuição muda entre classes de doenças e categorias específicas.”

Explique um exemplo com o filtro visível, mostrando as médias e os denominadores, além da variação percentual.

### 4. Defasagem da temperatura

Mostre as comparações para o mesmo dia e para 1, 3 e 7 dias anteriores.

**Fala sugerida:** “As internações permanecem no dia observado. O que muda é a data da temperatura associada: o próprio dia, um, três ou sete dias antes.”

Os lags consultam datas civis exatas da mesma estação. A ausência de medição permanece nula. A maior variação exibida é um resultado descritivo do recorte selecionado.

### 5. Outras variáveis meteorológicas

Percorra os gráficos de umidade, precipitação, radiação, vento e amplitude da pressão atmosférica. Todos comparam a média de internações por município-dia observado.

**Fala sugerida:** “A base permite ampliar a exploração para outras condições meteorológicas, usando a mesma unidade de análise e deixando as faixas de cada variável explícitas.”

### 6. Análise territorial

Apresente o mapa, o ranking municipal e os detalhes ao passar o cursor sobre um município.

**Fala sugerida:** “A leitura territorial permite localizar os padrões observados e comparar municípios. A interpretação considera as médias de internação e a quantidade de dias em cada condição.”

Os municípios têm uma estação meteorológica de referência. A distância até essa estação faz parte das limitações espaciais a documentar.

## Números para ter à mão

Valores provenientes dos relatórios locais de implementação. Os cartões do painel respondem ao contexto de filtros.

| Item | Valor |
|---|---:|
| Registros na Gold | 8.409.047 |
| Internações de 2023–2025 por data de internação | 8.236.990 |
| Óbitos registrados nessas internações | 422.557 |
| Proporção de óbitos nas internações | 5,13% |
| Permanência média | 4,84 dias |
| Valor total registrado | R$ 14.389.294.434,48 |
| Municípios | 645 |
| Estações INMET | 40 |
| Linhas na fato município-dia | 578.832 |
| Linhas na fato detalhada por CID e perfil | 6.597.066 |
| Categorias CID no consumo | 1.658 |

**Por que há dois totais de internações?** A extração SIH segue as competências dos arquivos. A camada analítica usa a data de internação entre 01/01/2023 e 31/12/2025. Os 172.057 registros da diferença têm data anterior à janela meteorológica, conforme os relatórios. Eles permanecem na Gold.

**Por que há duas fatos?** A primeira mantém a meteorologia no grão municipal diário. A segunda detalha os indicadores de saúde por diagnóstico, sexo e faixa etária. Os totais de saúde reconciliam entre elas e não devem ser somados entre si.

**Qual é o papel da qualidade?** Conferir chegada e integridade na Bronze, padronização na Silver, integração na Gold e reconciliação na camada de consumo. O Airflow respeita essas dependências e bloqueia o avanço quando ocorre falha crítica.

**O que o desempenho documentado mostra?** Uma execução até a Gold terminou em aproximadamente 33min26s, com 24 tasks em sucesso. Essa medição é anterior à inclusão das duas tasks de consumo. O relatório registrou redução observada de 19,98% em relação ao baseline, sem desenho experimental controlado. Não apresentar esse tempo como duração medida de toda a DAG atual.

## Enquadramento dos resultados no texto

O trabalho entrega uma solução de Engenharia de Dados e um ambiente de análise descritiva. A discussão pode apresentar padrões e associações observados, com período, município, filtros e denominadores registrados. Esses resultados não constituem, por si só, estimativas causais do efeito do clima.

Os registros representam internações, não pacientes únicos. A proporção de óbitos do painel refere-se às internações analisadas, não à mortalidade populacional. Os valores financeiros são os registrados no SIH. A base contém apenas combinações municipais diárias observadas, e uma linha ausente não é automaticamente equivalente a zero internações. Um vínculo com estação e dia também não garante preenchimento de todas as variáveis meteorológicas.

## Proposta de sequência para escrever

1. **Metodologia e desenvolvimento:** aproveitar os relatórios existentes para documentar fontes, recorte, camadas, integração, qualidade, Airflow e modelo do dashboard.
2. **Resultados:** selecionar exemplos do painel, registrar os filtros e inserir as evidências de reconciliação e desempenho pertinentes.
3. **Introdução e fundamentação:** consolidar a pergunta, os objetivos e a literatura que sustenta as decisões do trabalho.
4. **Discussão e conclusão:** relacionar os resultados ao objetivo, explicitar limitações e delimitar a contribuição entregue.

Essa é uma proposta de ordem de redação. A ordem dos capítulos no texto pode seguir introdução, fundamentação, metodologia, desenvolvimento, resultados e conclusão.

## Decisões para sair da reunião

- Título e formulação final do objetivo.
- Peso da contribuição de Engenharia de Dados e da análise de saúde e clima.
- Quais páginas e resultados do dashboard entrarão como figuras e exemplos.
- Estrutura de capítulos e norma institucional.
- Próximo capítulo a entregar e datas de revisão.

**Encerramento sugerido:** “A solução técnica está concluída e já tenho os relatórios e o dashboard como base. Quero alinhar agora a estrutura do texto, os resultados que vale destacar e a sequência de entregas para a revisão.”

## Fontes do material

- [Implementação da camada de consumo](../Relatorio_Implementacao_Camada_Consumo_Power_BI.md).
- [Resultados dos experimentos de desempenho](../Relatorio_Resultados_Performance_Experimentos_1_2.md).
- [Arquitetura e integração das fontes](../Relatorio_Status_Arquitetura_Pipeline_TCC.md), usado para características técnicas e volumes, não para o status antigo de Airflow/Power BI.
- [Orientações do dashboard](../../Dashboard/LEIA-ME.txt), definição do relatório e modelo semântico na pasta Dashboard.
- Confirmação do autor em 30/09/2026 de que pipeline, orquestração e dashboard estão concluídos.
