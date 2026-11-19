"""DAG quotidien : Bronze -> Silver (quality gate) -> Gold -> Marts.

Chaque couche est une tâche distincte : en cas d'échec, Airflow relance
uniquement l'étape fautive, et le quality gate de la couche Silver bloque
la publication des marts si les données du jour sont dégradées.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash import BashOperator

PROJET = "/opt/telecom-spark-pipeline"
MASTER = Variable.get("spark_master", default_var="local[*]")


def spark_submit(etape: str) -> str:
    return (f"cd {PROJET} && spark-submit --master '{MASTER}' --driver-memory 3g "
            f"src/main.py --step {etape} --config config/pipeline.yaml")


with DAG(
    dag_id="telecom_spark_pipeline",
    description="Ingestion CDR, recharges et Mobile Money vers le modèle en étoile et les marts Power BI",
    schedule="0 3 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
    },
    tags=["spark", "telecom", "reporting"],
) as dag:
    bronze = BashOperator(task_id="bronze_ingestion", bash_command=spark_submit("bronze"))
    silver = BashOperator(task_id="silver_nettoyage_qualite", bash_command=spark_submit("silver"))
    gold = BashOperator(task_id="gold_modele_etoile", bash_command=spark_submit("gold"))
    marts = BashOperator(task_id="marts_power_bi", bash_command=spark_submit("marts"))

    bronze >> silver >> gold >> marts
