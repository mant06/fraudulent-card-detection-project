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
dag_id='Member_Score',
schedule_interval='@weekly',
start_date=days_ago(1)
)



def fetch_member_score():
  command_one = f'cd Desktop'
  command_two = '{{ var.value.memberscore}}'
  return f'{command_one} && {command_two}'

import_member_score = SSHOperator(
 task_id='sqoop_member_score',
 ssh_conn_id='cloudera',
 command=fetch_member_score(),
 dag=dag
)


import_member_score
