from airflow import DAG
from airflow.utils.dates import  days_ago
from airflow.utils.dates import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow import settings
from airflow.models import Connection

dag = DAG(
dag_id='Manual_Export_Card_Txns',
#schedule_interval='0 */8 * * *',
schedule_interval=None,
start_date=days_ago(1)
)



def fetch_card_txns():
  command_one = f'cd Desktop'
  command_two = '{{ var.value.card_txns_export }}'
  return f'{command_one} && {command_two}'

export_card_txns_dt = SSHOperator(
 task_id='export_card_txns_dt',
 ssh_conn_id='cloudera',
 command=fetch_card_txns(),
 dag=dag
) 


export_card_txns
