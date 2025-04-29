# Modulo9-Integrador


###Instalacion: 
#Paso 1:
#Descargar Python 3.10.11 (muchas versiones tienen complicaciones) y Visual Code 
#Paso 2: Verificar la version de python instalada
py -3.10 --version
#Paso 3: Instalacion del entorno virtual
py -m pip install --upgrade pip
py -m pip install virtualenv
#Paso 4: Crear una Carpeta_airflow
mkdir Carpeta_airflow
#Paso 5: Ingresar a la Carpeta_airflow
cd Carpeta_airflow
#Paso 6:Crear un entorno virtual llamado entorno_virtual_1
py -m venv entorno_virtual_1
#Paso 7: Dentro de la carpeta_airflow activo el entorno
.\Entorno_virtual_1\Scripts\Activate
#Paso 8: Instalar Airflow
pip install --upgrade pip
pip install "apache-airflow==2.8.1" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

###Creacion de DAGS
#Paso 1: Abrir la carpeta creada carpeta_airflow en el visual code studio
#Paso 2: Crear una carpeta llamada dags
mkdir dags
#Paso 3: Ingresar a la carpeta dags
cd dags
#Paso 4: Crear un nuevo archivo llamado "Codigo"

#Dentro de "Codigo" se anotaran todos los comandos utilizados para realizar las tareas solicitadas.


from datetime import timedelta,datetime
from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

#Configuración de default_arg
defaul_args={
            'owner':'airflow',
            'start_date':datetime (2025,1,1)
}

# 1) Descarga de datos de un archivo csv (puede ser una simulación)
def descargar_archivo():
    print("Descargando y guardando el archivo csv")
    url = 'https://www.stats.govt.nz/assets/Uploads/Annual-enterprise-survey/Annual-enterprise-survey-2023-financial-year-provisional/Download-data/annual-enterprise-survey-2023-financial-year-provisional.csv'
    destino = '/tmp/archivo_descargado.csv'
    print(f"Archivo {url} ya esta guardado en {destino}")

with DAG(
    dag_id='dag_de_prueba',
    default_args=defaul_args,
    schedule_interval='@daily'
) as dag:

    descargar_archivo = PythonOperator(
        task_id='descargar_archivo',
        python_callable=descargar_archivo
    )


# 2) Procesar esos datos (transformándolos o ejecutando una operación simple)

def procesar_datos():
    with open('/tmp/archivo_descargado.csv', 'r') as f:
        datos = f.readlines()
    print(f"Datos procesados son: {datos}")

procesar_datos = PythonOperator(
                            task_id='procesar_datos',
                            python_callable=procesar_datos,
                            dag=dag
    )

# 3) Almacenar los resultados en un archivo local .txt

almacenar_resultados = BashOperator(
                                task_id='almacenar_resultados',
                                bash_command='cp /tmp/archivo_descargado.csv /tmp/almacenar_resultados.txt'
)

# 4) Ejecutar los procesos

descargar_archivo >> procesar_datos >>almacenar_resultados

# 5) Implementar la visualización del workflow a través de la interfaz gráfica de Airflow.

localhost:8080

# 6) Incluir un sensor para esperar a que un archivo esté disponible antes de proceder a las siguientes tareas.

from airflow.sensors.filesystem import FileSensor

esperar_archivo = FileSensor(
                         task_id='esperar_archivo',
                         filepath='/tmp/archivo_descargado.csv',
                         poke_interval=10,
                         timeout=60,
                         dag=dag
)

esperar_archivo >> procesar_datos
