
###Instalacion: 
#Paso 1:
#Descargar Python 3.10.11 (muchas versiones tienen complicaciones) y Visual Code 
#Paso 2: Verificar la version de python instalada
#py -3.10 --version
#Paso 3: Instalacion del entorno virtual
#py -m pip install --upgrade pip
#py -m pip install virtualenv
#Paso 4: Crear una Carpeta_airflow
#mkdir Carpeta_airflow
#Paso 5: Ingresar a la Carpeta_airflow
#cd Carpeta_airflow
#Paso 6: Desde Visual studio code abro el proyecto seleccionando la carpeta creada Carpeta_airflow
#Paso 7:Crear un entorno virtual llamado entorno_virtual_1
#py -m venv entorno_virtual_1
#Paso 8: Dentro de la carpeta_airflow activo el entorno
#.\Entorno_virtual_1\Scripts\Activate
#Paso 8: Instalar Airflow
#pip install --upgrade pip #dependiendo la version aveces no hace falta
#pip install "apache-airflow==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"
#Paso 9: Creo e ingreso a una carpeta llamada "dags"
#mkdir dags
#cd dags
#(para salir de la carpeta uso cd ..)
#Paso 10: Dentro de la carpeta "dags" creo desde visual code una archivo "dags.py" con las siguientes intrucciones:

#----------
#from datetime import datetime
#from airflow import DAG
#from airflow.operators.python import PythonOperator
#from airflow.operators.bash import BashOperator
#from airflow.sensors.filesystem import FileSensor
#import requests

##Configuración de default_arg
#default_args = {
#    'owner': 'airflow',
#    'start_date': datetime(2025, 1, 1),
#}

## 1) Descarga de datos de un archivo csv (es una simulacion con un csv de la web)
#def descargar_archivo():
#    url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv'
#    destino = '/tmp/archivo_descargado.csv'
#    response = requests.get(url)
#    with open(destino, 'wb') as f:
#        f.write(response.content)

## 2) Procesar esos datos (transformándolos o ejecutando una operación simple)
#def procesar_datos():
#    with open('/tmp/archivo_descargado.csv', 'r') as f:
#        datos = f.readlines()
#    print(f"Líneas en archivo: {len(datos)}")

## 3) Configuracion del DAG con las tareas_descargar, tarea_procesar, tarea_esperar y tarea_almacenar todas juntas

#with DAG(
#    dag_id='dag_de_prueba_1',
#    default_args=default_args,
#    schedule_interval='@daily',
#    catchup=False
#) as dag:

#    tarea_descargar = PythonOperator(
#        task_id='descargar_archivo',
#        python_callable=descargar_archivo
#    )

## (Incluir un sensor para esperar a que un archivo esté disponible antes de proceder a las siguientes tareas)
#    tarea_esperar = FileSensor(
#        task_id='esperar_archivo',
#        filepath='/tmp/archivo_descargado.csv',
#        poke_interval=10,
#        timeout=60
#    )

#    tarea_procesar = PythonOperator(
#        task_id='procesar_datos',
#        python_callable=procesar_datos
#    )

## (Almacenar los resultados en un archivo local .csv)
#    tarea_almacenar = BashOperator(
#        task_id='almacenar_resultados',
#        bash_command='cp /tmp/archivo_descargado.csv /tmp/almacenar_resultados.csv'
#    )

## 4) Ejecutacion de las tareas
#   tarea_descargar >> tarea_esperar >> tarea_procesar >> tarea_almacenar

## 5) Implementar la visualización del workflow a través de la interfaz gráfica de Airflow.

#localhost:8080

#--------

#Paso 11: Creo desde la terminal el archivo "docker-compose.yml" y le adjunto el siguiente instructivo:

#-----------
# #version: '3.8'

#services:
#  postgres:
#    image: postgres:13
#    environment:
#      POSTGRES_USER: airflow
#      POSTGRES_PASSWORD: airflow
#      POSTGRES_DB: airflow

#  airflow-webserver:
#    image: apache/airflow:2.8.1
#    depends_on:
#      - postgres
#    environment:
#      AIRFLOW__CORE__EXECUTOR: LocalExecutor
#      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
#    volumes:
#      - ./dags:/opt/airflow/dags
#    ports:
#      - "8080:8080"
#    command: webserver

#  airflow-scheduler:
#    image: apache/airflow:2.8.1
#    depends_on:
#      - airflow-webserver
#    volumes:
#      - ./dags:/opt/airflow/dags
#    command: scheduler
#----------

#Paso 12: Guardar todas las modificaciones en el visual code con el boton de "Save All"
#Paso 13: Busco y descargo desde la web Docker y lo inicio (importante que este corriendo para continuar)
#Paso 14: Creamos el usuario y la contraseña para Airflow
#docker-compose run airflow-webserver airflow users create --username admin --firstname Admin --lastname User --role Admin --email admin@example.com --password admin
#Usuario:admin 
#Contraseña:admin
#Paso 15: Guarda el archivo y reinicia el scheduler (opcional)
#docker-compose restart airflow-scheduler
#Paso 16: Iniciar la base de datos Airflow desde Visual Code 
#docker-compose run airflow-webserver airflow db init
#Paso 17: Ejecuta este comando en la raíz del proyecto para crear una base de datos y sus tablas
#docker-compose up airflow-webserver airflow-scheduler
#Paso 18: Acceder a la interfaz web de Airflow desde el navegador con el usuario y la contraseña
#http://localhost:8080

#Paso 19: Reiniciar todo para que Airflow cargue el Dag (En el caso de ser necesario)
#docker-compose down
#docker-compose up -d
