import time
from airflow.models import Connection
from airflow import settings
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.utils.dates import days_ago
from airflow import DAG

def create_dag(dag_id, schedule, enriched_table_name, default_args):
    dag = DAG(dag_id,
              schedule_interval=schedule,
              default_args=default_args)
    bash_command=f"kinit -kt /info/keytab/info.keytab service && sh /scripts/run_bu_transfer_wrapper.sh /scripts/sorted_process_list"
    with dag:
        task_1 = SSHOperator(
            ssh_conn_id = 'ssh_report',
            task_id = "execute-bu-transfer",
            depends_on_past=False,
            command=bash_command,
            pool="cc",
            dag=dag)
    return dag

dag_id = 'bu_transfer'
default_args = {'owner': 'airflow', 'start_date' : days_ago(1),}
schedule = '30 23 * * *'

globals()[dag_id] = create_dag(dag_id,
                               schedule,
                               dag_id,
                               default_args)