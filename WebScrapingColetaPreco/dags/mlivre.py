from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Substituído DummyOperator por EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

def process_data():
    """
    Função de exemplo que processa dados
    Adicione sua lógica de processamento aqui
    """
    print("Processando dados...")

def run_python_script():
    """
    Executa o script Python
    """
    script_path = "/opt/airflow/dags/script/WebScrapingColetaPreco/src/transformacao/main.py"
    result = subprocess.run(['python3', script_path], capture_output=True, text=True)

    if result.returncode != 0:
        print("Erro ao executar o script:")
        print(result.stderr)
    else:
        print("Script executado com sucesso:")
        print(result.stdout)

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 24),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'depends_on_past': False
}

with DAG(
    'dag_s',
    default_args=default_args,
    description='DAG para scraping na Amazon e Mercado Livre',
    schedule='*/10 * * * *',  # Executa a cada 10 minutos
    catchup=False,
    tags=['scraping', 'mercadolivre', 'amazon']
) as dag:
    
    start = EmptyOperator(  # Substituído para EmptyOperator
        task_id='start',
        dag=dag
    )
    
    process = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        dag=dag
    )

    run_scrapy = BashOperator(
        task_id='extracao',
        bash_command=(
            'cd /opt/airflow/dags/script/WebScrapingColetaPreco/src && '
            'scrapy crawl mercado_livre -o ../data/data1.jsonl'
        ),
        dag=dag
    )

    run_python = PythonOperator(
        task_id='transformacao_load',
        python_callable=run_python_script,
        dag=dag
    )

    end = EmptyOperator(  # Substituído para EmptyOperator
        task_id='end',
        dag=dag
    )
    
    # Dependências das tarefas
    start >> process >> run_scrapy >> run_python >> end
