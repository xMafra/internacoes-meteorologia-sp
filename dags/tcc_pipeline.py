from datetime import datetime, timedelta
import sys

from airflow.sdk import DAG, TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

BASE_PATH = "/home/jovyan/work"

# O projeto é montado neste caminho também nos workers.
if BASE_PATH not in sys.path:
    sys.path.insert(0, BASE_PATH)

from src.orchestration import bronze as bronze_runtime

SPARK_CONNECTION = "spark_default"

SPARK_CONF = {
    "spark.ui.enabled": "false",
}

# Preserva o tuning dos scripts, sem impor shuffle aos jobs CID10 e SIH.
SPARK_CONF_SHUFFLE_4 = {
    **SPARK_CONF,
    "spark.sql.shuffle.partitions": "4",
}

SPARK_CONF_INMET = {
    **SPARK_CONF_SHUFFLE_4,
    "spark.sql.execution.arrow.pyspark.enabled": "false",
    # Resolve o Python do ambiente Airflow pelo PATH da imagem.
    "spark.pyspark.python": "python3",
    "spark.pyspark.driver.python": "python3",
}

SPARK_CONF_INMET_DIARIO = {
    **SPARK_CONF,
    "spark.sql.shuffle.partitions": "40",
}

# Evita pressão de memória no driver ao materializar dimensões da fato.
SPARK_CONF_FATO_INTERNACAO = {
    **SPARK_CONF,
    "spark.sql.autoBroadcastJoinThreshold": "-1",
}

SPARK_CONF_FATO_INTERNACAO_METEOROLOGIA = {
    **SPARK_CONF,
    "spark.sql.shuffle.partitions": "40",
}


# ============================================================
# CAMINHOS DOS SCRIPTS - INMET
# ============================================================

SILVER_INMET_SCRIPT = (
    f"{BASE_PATH}/src/silver/inmet.py"
)

DQ_SILVER_INMET_SCRIPT = (
    f"{BASE_PATH}/src/quality/silver/dq_inmet.py"
)

SILVER_INMET_DIARIO_SCRIPT = (
    f"{BASE_PATH}/src/silver/inmet_diario.py"
)

DQ_SILVER_INMET_DIARIO_SCRIPT = (
    f"{BASE_PATH}/src/quality/silver/dq_inmet_diario.py"
)


# ============================================================
# CAMINHOS DOS SCRIPTS - IBGE
# ============================================================

SILVER_IBGE_SCRIPT = (
    f"{BASE_PATH}/src/silver/ibge.py"
)

DQ_SILVER_IBGE_SCRIPT = (
    f"{BASE_PATH}/src/quality/silver/dq_ibge.py"
)


# ============================================================
# CAMINHOS DOS SCRIPTS - CID-10
# ============================================================

SILVER_CID10_SCRIPT = (
    f"{BASE_PATH}/src/silver/cid10.py"
)

DQ_SILVER_CID10_SCRIPT = (
    f"{BASE_PATH}/src/quality/silver/dq_cid10.py"
)


# ============================================================
# CAMINHOS DOS SCRIPTS - SIH
# ============================================================

SILVER_SIH_SCRIPT = (
    f"{BASE_PATH}/src/silver/processar_sih_lote.py"
)

DQ_SILVER_SIH_SCRIPT = (
    f"{BASE_PATH}/src/quality/silver/dq_sih.py"
)


# ============================================================
# CAMINHOS DOS SCRIPTS - GOLD
# ============================================================

GOLD_MUNICIPIO_ESTACAO_SCRIPT = f"{BASE_PATH}/src/gold/municipio_estacao.py"
DQ_GOLD_MUNICIPIO_ESTACAO_SCRIPT = (
    f"{BASE_PATH}/src/quality/gold/dq_municipio_estacao.py"
)
GOLD_FATO_INTERNACAO_SCRIPT = f"{BASE_PATH}/src/gold/fato_internacao.py"
DQ_GOLD_FATO_INTERNACAO_SCRIPT = (
    f"{BASE_PATH}/src/quality/gold/dq_fato_internacao.py"
)
GOLD_FATO_INTERNACAO_METEOROLOGIA_SCRIPT = (
    f"{BASE_PATH}/src/gold/fato_internacao_meteorologia.py"
)
DQ_GOLD_FATO_INTERNACAO_METEOROLOGIA_SCRIPT = (
    f"{BASE_PATH}/src/quality/gold/dq_fato_internacao_meteorologia.py"
)


# ============================================================
# DEFINIÇÃO DA DAG
# ============================================================

with DAG(
    dag_id="tcc_pipeline",
    description="Pipeline de dados do TCC - Bronze, Silver, Gold e Data Quality",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "tcc",
        "spark",
        "data-quality",
    ],
) as dag:

    # ========================================================
    # TASK GROUP - INMET
    # ========================================================

    with TaskGroup(
        group_id="inmet",
        tooltip="Processamento e Data Quality dos dados do INMET",
    ) as inmet:

        bronze_inmet = PythonOperator(
            task_id="bronze_inmet",
            python_callable=bronze_runtime.bronze_inmet,
            retries=2,
            retry_delay=timedelta(minutes=2),
        )

        dq_bronze_inmet = PythonOperator(
            task_id="dq_bronze_inmet",
            python_callable=bronze_runtime.dq_bronze_inmet,
            retries=0,
        )

        silver_inmet = SparkSubmitOperator(
            task_id="silver_inmet",
            application=SILVER_INMET_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-inmet",
            conf=SPARK_CONF_INMET,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_silver_inmet = SparkSubmitOperator(
            task_id="dq_silver_inmet",
            application=DQ_SILVER_INMET_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-inmet",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        silver_inmet_diario = SparkSubmitOperator(
            task_id="silver_inmet_diario",
            application=SILVER_INMET_DIARIO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-inmet-diario",
            conf=SPARK_CONF_INMET_DIARIO,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_silver_inmet_diario = SparkSubmitOperator(
            task_id="dq_silver_inmet_diario",
            application=DQ_SILVER_INMET_DIARIO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-inmet-diario",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        (
            silver_inmet
            >> dq_silver_inmet
            >> silver_inmet_diario
            >> dq_silver_inmet_diario
        )

    # ========================================================
    # TASK GROUP - IBGE
    # ========================================================

    with TaskGroup(
        group_id="ibge",
        tooltip="Processamento e Data Quality dos dados do IBGE",
    ) as ibge:

        bronze_ibge = PythonOperator(
            task_id="bronze_ibge",
            python_callable=bronze_runtime.bronze_ibge,
            retries=2,
            retry_delay=timedelta(minutes=1),
        )

        dq_bronze_ibge = PythonOperator(
            task_id="dq_bronze_ibge",
            python_callable=bronze_runtime.dq_bronze_ibge,
            retries=0,
        )

        silver_ibge = SparkSubmitOperator(
            task_id="silver_ibge",
            application=SILVER_IBGE_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-ibge",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_silver_ibge = SparkSubmitOperator(
            task_id="dq_silver_ibge",
            application=DQ_SILVER_IBGE_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-ibge",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        silver_ibge >> dq_silver_ibge

    # ========================================================
    # TASK GROUP - CID-10
    # ========================================================

    with TaskGroup(
        group_id="cid10",
        tooltip="Processamento e Data Quality da classificação CID-10",
    ) as cid10:

        bronze_cid10 = PythonOperator(
            task_id="bronze_cid10",
            python_callable=bronze_runtime.bronze_cid10,
            retries=0,
        )

        dq_bronze_cid10 = PythonOperator(
            task_id="dq_bronze_cid10",
            python_callable=bronze_runtime.dq_bronze_cid10,
            retries=0,
        )

        silver_cid10 = SparkSubmitOperator(
            task_id="silver_cid10",
            application=SILVER_CID10_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-cid10",
            conf=SPARK_CONF,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_silver_cid10 = SparkSubmitOperator(
            task_id="dq_silver_cid10",
            application=DQ_SILVER_CID10_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-cid10",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        silver_cid10 >> dq_silver_cid10

    # ========================================================
    # TASK GROUP - SIH
    # ========================================================

    with TaskGroup(
        group_id="sih",
        tooltip="Processamento e Data Quality dos dados do SIH",
    ) as sih:

        bronze_sih = PythonOperator(
            task_id="bronze_sih",
            python_callable=bronze_runtime.bronze_sih,
            retries=3,
            retry_delay=timedelta(minutes=2),
        )

        dq_bronze_sih = PythonOperator(
            task_id="dq_bronze_sih",
            python_callable=bronze_runtime.dq_bronze_sih,
            retries=0,
        )

        silver_sih = SparkSubmitOperator(
            task_id="silver_sih",
            application=SILVER_SIH_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-sih",
            conf=SPARK_CONF,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_silver_sih = SparkSubmitOperator(
            task_id="dq_silver_sih",
            application=DQ_SILVER_SIH_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-sih",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        silver_sih >> dq_silver_sih

    # ========================================================
    # TASK GROUP - GOLD
    # ========================================================

    with TaskGroup(
        group_id="gold",
        tooltip="Integração Gold e seus gates de Data Quality",
    ) as gold:

        gold_municipio_estacao = SparkSubmitOperator(
            task_id="gold_municipio_estacao",
            application=GOLD_MUNICIPIO_ESTACAO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-gold-municipio-estacao",
            conf=SPARK_CONF,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_gold_municipio_estacao = SparkSubmitOperator(
            task_id="dq_gold_municipio_estacao",
            application=DQ_GOLD_MUNICIPIO_ESTACAO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-gold-municipio-estacao",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        gold_fato_internacao = SparkSubmitOperator(
            task_id="gold_fato_internacao",
            application=GOLD_FATO_INTERNACAO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-gold-fato-internacao",
            conf=SPARK_CONF_FATO_INTERNACAO,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_gold_fato_internacao = SparkSubmitOperator(
            task_id="dq_gold_fato_internacao",
            application=DQ_GOLD_FATO_INTERNACAO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-gold-fato-internacao",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        gold_fato_internacao_meteorologia = SparkSubmitOperator(
            task_id="gold_fato_internacao_meteorologia",
            application=GOLD_FATO_INTERNACAO_METEOROLOGIA_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-gold-fato-internacao-meteorologia",
            conf=SPARK_CONF_FATO_INTERNACAO_METEOROLOGIA,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        dq_gold_fato_internacao_meteorologia = SparkSubmitOperator(
            task_id="dq_gold_fato_internacao_meteorologia",
            application=DQ_GOLD_FATO_INTERNACAO_METEOROLOGIA_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-gold-fato-internacao-meteorologia",
            conf=SPARK_CONF_SHUFFLE_4,
            retries=0,
            durable=False,
        )

        gold_municipio_estacao >> dq_gold_municipio_estacao
        gold_fato_internacao >> dq_gold_fato_internacao
        gold_fato_internacao_meteorologia >> dq_gold_fato_internacao_meteorologia

    # Cada transformação aguarda aprovação de todas as fontes consumidas.
    [dq_silver_ibge, dq_silver_inmet] >> gold_municipio_estacao
    [
        dq_silver_sih,
        dq_silver_ibge,
        dq_silver_cid10,
        dq_gold_municipio_estacao,
    ] >> gold_fato_internacao
    [
        dq_gold_fato_internacao,
        dq_silver_inmet_diario,
    ] >> gold_fato_internacao_meteorologia

    # Gates Bronze, incluindo o cadastro IBGE consumido diretamente pelo SIH.
    bronze_inmet >> dq_bronze_inmet >> silver_inmet
    bronze_ibge >> dq_bronze_ibge >> silver_ibge
    bronze_cid10 >> dq_bronze_cid10 >> silver_cid10
    bronze_sih >> dq_bronze_sih >> silver_sih
    dq_bronze_ibge >> silver_sih
