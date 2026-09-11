---
title: "Integração Bronze e Data Quality ao Airflow"
date: "9 de setembro de 2026"
lang: pt-BR
geometry: margin=2.2cm
fontsize: 11pt
toc-title: "Sumário"
---

# Resultado e baseline

A DAG tcc_pipeline passou de 16 para 24 tasks, mantendo os cinco TaskGroups inmet, ibge, cid10, sih e gold. Foram adicionadas oito PythonOperators nos quatro grupos de fonte. O grupo Gold permaneceu estruturalmente igual. Não houve execução da DAG completa.

O baseline operacional informado pelo usuário é uma execução das 16 tasks anteriores em SUCCESS, com cerca de 45 minutos. Não se repetiu essa execução. As configurações e dependências entre essas 16 tasks foram comparadas programaticamente com a DAG em HEAD e preservadas.

Antes da implementação, foram relidos os coletores, DQs, DAG e paths dos consumidores. Confirmou-se que Silver SIH lê diretamente o ZIP Bronze IBGE/2022. Os relatórios de auditoria Bronze já existentes como arquivos não rastreados foram preservados.

# Arquivos e implementação

Arquivo existente modificado: dags/tcc_pipeline.py. Foram acrescentados import de PythonOperator, acesso ao módulo de wrappers pela raiz montada do projeto, oito operadores, retries e nove novas arestas. Nenhuma configuração Spark existente foi alterada.

Arquivos de código criados:

- src/orchestration/__init__.py: identificação do pacote.
- src/orchestration/bronze.py: quatro wrappers de aquisição/preparação, quatro de DQ e um helper de aprovação.
- tests/test_bronze_orchestration.py: cinco testes automatizados leves.

Também foram criados este relatório Markdown e sua versão PDF. Não houve alteração em scripts Bronze originais, regras DQ, Silver, Gold, Docker, requirements, connection ou permissões.

# Execução e parâmetros efetivos

| Wrapper | Função reutilizada | Parâmetros |
|:--|:--|:--|
| bronze_sih | download_sih_periodo | ano_inicio=2023, mes_inicio=1, ano_fim=2025, mes_fim=12 |
| bronze_inmet | download_inmet | Chamadas para 2023, 2024 e 2025 |
| bronze_ibge | download_malha_sp_2022 | Referência fixa do coletor: SP/2022 |
| bronze_cid10 | extrair_cid10, quando necessário | ZIP local provisionado; somente destinos ausentes |
| dq_bronze_sih | validar_sih | 2023-01 até 2025-12, todos os argumentos explícitos |
| dq_bronze_inmet | validar_inmet | ano_inicio=2023, ano_fim=2025 |
| dq_bronze_ibge | validar_ibge | ano=2022 |
| dq_bronze_cid10 | validar_cid10 | Raiz atual definida pelo validador |

Imports dos coletores e validadores são locais às funções dos wrappers. Nenhuma aquisição ou validação é chamada no parse da DAG. Os retornos da aquisição são strings ou listas de strings de caminhos, adequados à serialização. Os DQs imprimem o relatório e retornam normalmente apenas quando aprovados.

Cada wrapper DQ chama a função real de validação, imprime os checks e lança ValueError quando report.aprovado é falso. Não utiliza o retorno inteiro de main como mecanismo de falha. Com retries=0 e all_success nos consumidores, um DQ reprovado impede a execução da Silver dependente. A transição de estado em uma DAG Run real ainda será observada pelo usuário.

# Arquivos existentes e particularidade CID-10

SIH, INMET e IBGE mantêm o comportamento dos coletores: destino existente é reutilizado, sem novo download. Nenhuma recuperação automática foi adicionada. Arquivo existente inválido permanece disponível para diagnóstico e pode reprovar no DQ.

O extrator CID-10 original abre CSVs com wb. Chamá-lo diretamente sobre a Bronze contrariaria a restrição de não sobrescrever. O wrapper implementa a adaptação mínima:

1. Verifica se o ZIP local existe; se não existir, lança FileNotFoundError com mensagem explícita.
2. Preserva CSVs existentes, inclusive inválidos, para o gate posterior.
3. Quando há CSVs ausentes, reutiliza o extrator original em diretório temporário isolado.
4. Copia somente os ausentes para a Bronze com abertura exclusiva xb, impedindo sobrescrita de destino que surja nesse intervalo.

Quando os quatro CSVs existem, não há reextração. Não foi criada URL ou aquisição externa CID-10. O comportamento do extrator original fora deste wrapper permanece inalterado.

A publicação de arquivos novos não é atômica para o conjunto inteiro: uma interrupção durante a cópia pode deixar um arquivo novo parcial. O wrapper não o apaga ou recupera automaticamente; ele permanece para diagnóstico. O staging temporário é limpo pelo gerenciador de contexto, sem mover ou limpar datasets existentes.

# Retries e paralelismo

| Task | Retries | Retry delay configurado |
|:--|--:|:--|
| sih.bronze_sih | 3 | 2 minutos |
| inmet.bronze_inmet | 2 | 2 minutos |
| ibge.bronze_ibge | 2 | 1 minuto |
| cid10.bronze_cid10 | 0 | Sem override; sem retry |
| Quatro DQs Bronze | 0 | Sem override; sem retry |
| Transformações Silver/Gold | 1 | Configuração anterior preservada |
| DQs Silver/Gold | 0 | Configuração anterior preservada |

As quatro aquisições não possuem upstreams e podem começar independentemente. Não foram adicionados backoff, pools, tuning ou dependências artificiais. Uma única task Bronze SIH cobre as 36 competências, mantendo interrupção na primeira falha do coletor de período. Não foi utilizado dynamic task mapping.

# Grafo completo

```text
inmet:
  bronze_inmet -> dq_bronze_inmet
  -> silver_inmet -> dq_silver_inmet
  -> silver_inmet_diario -> dq_silver_inmet_diario

ibge:
  bronze_ibge -> dq_bronze_ibge
  -> silver_ibge -> dq_silver_ibge

cid10:
  bronze_cid10 -> dq_bronze_cid10
  -> silver_cid10 -> dq_silver_cid10

sih:
  bronze_sih -> dq_bronze_sih
  -> silver_sih -> dq_silver_sih

Dependência cruzada adicional:
  ibge.dq_bronze_ibge -> sih.silver_sih

gold:
  [dq_silver_ibge, dq_silver_inmet]
    -> gold_municipio_estacao
    -> dq_gold_municipio_estacao

  [dq_silver_sih, dq_silver_ibge, dq_silver_cid10,
   dq_gold_municipio_estacao]
    -> gold_fato_internacao
    -> dq_gold_fato_internacao

  [dq_gold_fato_internacao, dq_silver_inmet_diario]
    -> gold_fato_internacao_meteorologia
    -> dq_gold_fato_internacao_meteorologia
```

Silver SIH exige simultaneamente DQ Bronze SIH e DQ Bronze IBGE. A dependência vem da leitura direta de data/bronze/ibge/2022/SP_Municipios_2022.zip. Todos os trigger rules foram verificados como all_success. O grafo não possui ciclos.

# Testes e resultados

Comando principal no worker:

```text
docker compose -f docker-compose.airflow.yaml exec -T
  -w /home/jovyan/work airflow-worker
  python -B -m unittest discover -s tests
  -p test_bronze_orchestration.py -v
```

As quebras são apenas de apresentação. Resultado: cinco testes executados, todos OK, sem skips.

| Teste | Resultado |
|:--|:--|
| Períodos de aquisição | 36 chamadas mensais SIH exatas; INMET 2023, 2024, 2025; função IBGE correta |
| Gates | Quatro reprovações simuladas lançam ValueError; quatro aprovações concluem; parâmetros DQ conferidos |
| CID-10 em fixture temporária | CSV divergente preservado; ausentes criados; segunda chamada não reextrai; ZIP ausente falha claramente |
| Acervo real existente | Wrappers de aquisição executados com chamadas de rede bloqueadas por mocks; extrator CID-10 também bloqueado; tamanho e mtime dos arquivos preservados |
| DAG | Cinco grupos, 24 tasks, retries/delays, upstreams/downstreams exatos, all_success e ordenação topológica aprovados; wrappers não chamados no parse |

Verificações adicionais:

- compile em memória da DAG, pacote de wrappers e teste: PASS.
- Imports no worker, exercitados pelos testes e importação da DAG: PASS.
- Comparação com a DAG em HEAD: 16 tasks anteriores mantêm application, conf, retries, retry_delay, connection, name, trigger_rule e arestas entre si: PASS.
- airflow dags list-import-errors: No data found.
- git diff --check: sem erros; somente aviso de normalização LF/CRLF.
- Revisão integral do diff e dos novos arquivos: escopo restrito à integração, testes e documentação.

Os testes de aquisição não apagaram arquivos para provocar downloads. O teste CID-10 que cria CSVs usa exclusivamente um diretório temporário de teste. Os DQs reais de grande volume não foram executados; suas funções foram substituídas por relatórios simulados apenas no processo do teste, sem editar as regras.

# Estatísticas e resumo do diff

O comando git diff --stat apresentou:

```text
dags/tcc_pipeline.py | 66 ++++++++++++++++++++++++++++++++++++++++++++++++++++
1 file changed, 66 insertions(+)
```

Arquivos novos não rastreados não entram nessa estatística: pacote src/orchestration, teste e relatórios. Os documentos da auditoria anterior já estavam presentes e não foram alterados nesta implementação.

As 66 linhas da DAG acrescentam imports/acesso ao módulo, oito operadores e nove arestas. O wrapper concentra recorte fixo, execução runtime, reprovação DQ por exceção e proteção contra sobrescrita CID-10. Os testes verificam o contrato da integração. Não houve modificação de algoritmos, paths dos datasets, granularidade, regras DQ, shuffle, broadcast, cache, persist ou actions.

# Riscos e limitações

Os coletores continuam confiando em existência, sem recuperação de conteúdo inválido. Seus timeouts, tratamento de rede e comportamento de falha não foram reescritos. O lote Bronze SIH continua interrompendo na primeira falha.

CID-10 continua dependendo do ZIP provisionado, sem origem externa automatizada. Se o ZIP for inválido e todos os CSVs já existirem, a preparação preserva os arquivos e o DQ deve diagnosticar a inconsistência. Se faltarem CSVs, a extração pode falhar antes do gate.

Os DQs Bronze mantêm seus custos anteriores: conversão temporária de DBC, CRC e hashes. Nenhuma otimização ou estimativa nova de duração foi aplicada. O baseline de 45 minutos não inclui estas oito tasks.

Não foi disparada a DAG de 24 tasks, nem testada aquisição real de arquivo ausente na rede. O estado FAILED no Airflow, resultados integrais dos DQs, tempo total e comportamento operacional de retries serão observados na execução manual posterior. Nenhum commit ou push foi feito.
