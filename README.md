# Pipeline de Dados – TCC

Pipeline de Engenharia de Dados para integração e análise de dados públicos de
internações hospitalares, meteorologia e população no estado de São Paulo,
abrangendo o período de 2023 a 2025.

## Status

🚧 Em desenvolvimento.

## Arquitetura

O projeto utiliza uma arquitetura em camadas:

- Bronze
- Silver
- Gold

Tecnologias principais:

- Apache Spark / PySpark
- Docker
- Airflow
- Python
- Power BI

## Fontes de dados

- DATASUS / SIH
- INMET
- IBGE

## Execução

As instruções completas de instalação e execução serão documentadas conforme
o ambiente e o pipeline forem implementados.

## Data quality da camada Bronze

Os controles técnicos dos arquivos brutos estão centralizados em
`src/quality/bronze`. Cada comando retorna código `0` quando todos os checks
passam e código `1` quando há falhas, permitindo seu uso em DAGs e CI.

Exemplos de execução no contêiner Spark:

```bash
python src/quality/bronze/dq_sih.py --ano-inicio 2023 --mes-inicio 1 --ano-fim 2025 --mes-fim 12
python src/quality/bronze/dq_inmet.py --ano-inicio 2023 --ano-fim 2025
python src/quality/bronze/dq_ibge.py --ano 2022
python src/quality/bronze/dq_cid10.py
```

As verificações cobrem chegada e quantidade esperada, arquivos vazios,
integridade e legibilidade do formato, estrutura técnica mínima, coerência do
período, conteúdo duplicado e arquivos temporários de downloads interrompidos.
Não são aplicadas regras de negócio ou de qualidade analítica na Bronze.
