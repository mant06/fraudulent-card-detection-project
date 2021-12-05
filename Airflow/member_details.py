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
dag_id='Load_Member_Details',
schedule_interval='0 */8 * * *',

start_date=days_ago(1)
)



def fetch_member_details():
  command_one = f'cd Desktop'
  command_two = '{{ var.value.memberdetails_cmd}}'
  return f'{command_one} && {command_two}'

member_details = SSHOperator(
 task_id='member_details',
 ssh_conn_id='cloudera',
 command=fetch_member_details(),
 dag=dag
) 



member_details
