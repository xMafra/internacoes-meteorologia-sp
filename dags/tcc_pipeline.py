from datetime import datetime, timedelta

from airflow.sdk import DAG, TaskGroup

from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


# ============================================================
# CONFIGURAÇÕES GERAIS
# ============================================================

BASE_PATH = "/home/jovyan/work"

SPARK_CONNECTION = "spark_default"

SPARK_CONF = {
    "spark.ui.enabled": "false",
}


# ============================================================
# CAMINHOS DOS SCRIPTS
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

        # ----------------------------------------------------
        # SILVER INMET HORÁRIO
        # ----------------------------------------------------

        silver_inmet = SparkSubmitOperator(
            task_id="silver_inmet",
            application=SILVER_INMET_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-inmet",
            conf=SPARK_CONF,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        # ----------------------------------------------------
        # DATA QUALITY - SILVER INMET HORÁRIO
        # ----------------------------------------------------

        dq_silver_inmet = SparkSubmitOperator(
            task_id="dq_silver_inmet",
            application=DQ_SILVER_INMET_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-inmet",
            conf=SPARK_CONF,
            retries=0,
            durable=False,
        )

        # ----------------------------------------------------
        # SILVER INMET DIÁRIO
        # ----------------------------------------------------

        silver_inmet_diario = SparkSubmitOperator(
            task_id="silver_inmet_diario",
            application=SILVER_INMET_DIARIO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-silver-inmet-diario",
            conf=SPARK_CONF,
            retries=1,
            retry_delay=timedelta(minutes=1),
            durable=False,
        )

        # ----------------------------------------------------
        # DATA QUALITY - SILVER INMET DIÁRIO
        # ----------------------------------------------------

        dq_silver_inmet_diario = SparkSubmitOperator(
            task_id="dq_silver_inmet_diario",
            application=DQ_SILVER_INMET_DIARIO_SCRIPT,
            conn_id=SPARK_CONNECTION,
            name="tcc-dq-silver-inmet-diario",
            conf=SPARK_CONF,
            retries=0,
            durable=False,
        )

        # ----------------------------------------------------
        # DEPENDÊNCIAS DO INMET
        # ----------------------------------------------------

        (
            silver_inmet
            >> dq_silver_inmet
            >> silver_inmet_diario
            >> dq_silver_inmet_diario
        )