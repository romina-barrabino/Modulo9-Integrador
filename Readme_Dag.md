from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
import requests

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

def descargar_archivo():
    url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv'
    destino = '/tmp/archivo_descargado.csv'
    response = requests.get(url)
    with open(destino, 'wb') as f:
        f.write(response.content)

def procesar_datos():
    with open('/tmp/archivo_descargado.csv', 'r') as f:
        datos = f.readlines()
    print(f"Líneas en archivo: {len(datos)}")

with DAG(
    dag_id='dag_de_prueba_1',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    tarea_descargar = PythonOperator(
        task_id='descargar_archivo',
        python_callable=descargar_archivo
    )

    tarea_esperar = FileSensor(
        task_id='esperar_archivo',
        filepath='/tmp/archivo_descargado.csv',
        poke_interval=10,
        timeout=60
    )

    tarea_procesar = PythonOperator(
        task_id='procesar_datos',
        python_callable=procesar_datos
    )

    tarea_almacenar = BashOperator(
        task_id='almacenar_resultados',
        bash_command='cp /tmp/archivo_descargado.csv /tmp/almacenar_resultados.csv'
    )

    tarea_descargar >> tarea_esperar >> tarea_procesar >> tarea_almacenar


localhost:8080
