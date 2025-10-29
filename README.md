# Creating a complete project folder with code, configs and a zip for download.
import os, textwrap, json, zipfile

project_root = "/mnt/data/netflix_data_pipeline"
os.makedirs(project_root, exist_ok=True)

files = {
    "README.md": textwrap.dedent("""\
        # Netflix Data Pipeline (Spark / PySpark / Airflow / Hadoop / S3 / PostgreSQL)
        
        Estrutura do projeto com exemplos de código para:
        - Ingestão e transformação com PySpark
        - Upload para Amazon S3 (script boto3)
        - Gravação em HDFS (script shell)
        - Carregamento em PostgreSQL (psycopg2)
        - Orquestração com Apache Airflow (DAG)
        
        **Observações**
        - Substitua valores de configuração em `config/config.yaml`.
        - Este projeto foi gerado automaticamente. Teste localmente antes de rodar em produção.
        - O arquivo `netflix_titles.csv` não está incluído — coloque-o em `data/` antes de rodar.
        """),

    "requirements.txt": textwrap.dedent("""\
        pyspark==3.4.1
        boto3
        psycopg2-binary
        apache-airflow==2.7.1
        pyyaml
        """),

    "config/config.yaml": textwrap.dedent("""\
        # Substitua pelos seus valores
        aws:
          access_key_id: YOUR_AWS_ACCESS_KEY_ID
          secret_access_key: YOUR_AWS_SECRET_ACCESS_KEY
          region: us-east-1
          s3_bucket: my-netflix-bucket

        postgres:
          host: localhost
          port: 5432
          db: netflix
          user: postgres
          password: postgres

        paths:
          raw_csv: data/netflix_titles.csv
          transformed_parquet: output/parquet
          transformed_csv_for_pg: output/for_postgres/netflix_clean.csv
          hdfs_target_dir: /user/hadoop/netflix/parquet
        """),

    "sql/create_table.sql": textwrap.dedent("""\
        CREATE TABLE IF NOT EXISTS netflix_titles (
          show_id TEXT PRIMARY KEY,
          type TEXT,
          title TEXT,
          director TEXT,
          cast TEXT,
          country TEXT,
          date_added DATE,
          release_year INT,
          rating TEXT,
          duration TEXT,
          listed_in TEXT,
          description TEXT
        );
        """),

    "spark_jobs/transform_netflix.py": textwrap.dedent("""\
        # PySpark job: lê CSV, limpa/transforma e salva em Parquet + CSV (para Postgres)
        import yaml
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import trim, col, to_date

        with open('config/config.yaml') as f:
            cfg = yaml.safe_load(f)

        spark = SparkSession.builder.appName('netflix_transform').getOrCreate()

        raw_path = cfg['paths']['raw_csv']
        out_parquet = cfg['paths']['transformed_parquet']
        out_csv_for_pg = cfg['paths']['transformed_csv_for_pg']

        df = spark.read.option('header', True).option('inferSchema', True).csv(raw_path)

        # Exemplo de limpeza simples
        df_clean = (df
          .withColumn('title', trim(col('title')))
          .withColumn('date_added', to_date(col('date_added'), 'MMMM d, yyyy'))
          .withColumn('release_year', col('release_year').cast('int'))
        )

        # Seleciona colunas no formato desejado
        selected = ['show_id','type','title','director','cast','country','date_added','release_year','rating','duration','listed_in','description']
        df_final = df_clean.select(*selected)

        # Salva Parquet particionado por release_year (exemplo)
        df_final.write.mode('overwrite').partitionBy('release_year').parquet(out_parquet)

        # Também salva um CSV simplificado para importar no Postgres (pequeno set)
        df_final.coalesce(1).write.mode('overwrite').option('header', True).csv(out_csv_for_pg)

        spark.stop()
        """),

    "scripts/upload_s3.py": textwrap.dedent("""\
        # Upload de arquivos (recursivo) para S3 usando boto3
        import os
        import boto3
        import yaml

        with open('config/config.yaml') as f:
            cfg = yaml.safe_load(f)

        s3 = boto3.client('s3',
            aws_access_key_id=cfg['aws']['access_key_id'],
            aws_secret_access_key=cfg['aws']['secret_access_key'],
            region_name=cfg['aws']['region']
        )

        def upload_dir(local_dir, bucket, s3_prefix=''):
            for root, dirs, files in os.walk(local_dir):
                for fname in files:
                    local_path = os.path.join(root, fname)
                    rel_path = os.path.relpath(local_path, local_dir)
                    s3_key = os.path.join(s3_prefix, rel_path).replace('\\\\','/')
                    print(f'Uploading {local_path} to s3://{bucket}/{s3_key}')
                    s3.upload_file(local_path, bucket, s3_key)

        if __name__ == '__main__':
            bucket = cfg['aws']['s3_bucket']
            # Ex.: subir parquet
            upload_dir('output/parquet', bucket, s3_prefix='netflix/parquet')
        """),

    "scripts/hdfs_put.sh": textwrap.dedent("""\
        #!/bin/bash
        # Copia arquivos Parquet para HDFS (assume hadoop client configurado)
        LOCAL_PARQUET_DIR="output/parquet"
        HDFS_TARGET_DIR="/user/hadoop/netflix/parquet"

        hdfs dfs -rm -r -f ${HDFS_TARGET_DIR} || true
        hdfs dfs -mkdir -p ${HDFS_TARGET_DIR}
        hdfs dfs -put ${LOCAL_PARQUET_DIR}/* ${HDFS_TARGET_DIR}/
        echo "Upload to HDFS complete."
        """),

    "scripts/load_postgres.py": textwrap.dedent("""\
        # Script para carregar o CSV gerado no Postgres usando psycopg2 COPY
        import glob
        import os
        import yaml
        import psycopg2

        with open('config/config.yaml') as f:
            cfg = yaml.safe_load(f)

        csv_dir = cfg['paths']['transformed_csv_for_pg']
        csv_files = glob.glob(os.path.join(csv_dir, '*.csv'))

        conn = psycopg2.connect(
            host=cfg['postgres']['host'],
            port=cfg['postgres']['port'],
            dbname=cfg['postgres']['db'],
            user=cfg['postgres']['user'],
            password=cfg['postgres']['password']
        )
        cur = conn.cursor()
        with open('sql/create_table.sql') as f:
            cur.execute(f.read())
            conn.commit()

        if not csv_files:
            print('Nenhum CSV encontrado em', csv_dir)
        else:
            # Usa o primeiro arquivo CSV (o PySpark produz um arquivo CSV único em coalesce(1))
            csv_path = csv_files[0]
            with open(csv_path, 'r', encoding='utf-8') as f:
                # Pula cabeçalho
                next(f)
                cur.copy_expert(\"COPY netflix_titles FROM STDIN WITH CSV\", f)
            conn.commit()
            print('Dados importados para Postgres com sucesso.')

        cur.close()
        conn.close()
        """),

    "airflow/dags/etl_dag.py": textwrap.dedent("""\
        # Exemplo de DAG do Airflow que orquestra as etapas:
        # 1) Rodar job Spark (pode usar SparkSubmitOperator)
        # 2) Upload para S3
        # 3) Copiar para HDFS
        # 4) Importar para Postgres

        from datetime import datetime, timedelta
        from airflow import DAG
        from airflow.operators.bash import BashOperator
        from airflow.operators.python import PythonOperator
        import os

        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        }

        dag = DAG(
            'netflix_etl',
            default_args=default_args,
            description='ETL pipeline: Spark -> S3 -> HDFS -> Postgres',
            schedule_interval=None,
            start_date=datetime(2025, 1, 1),
            catchup=False
        )

        run_spark = BashOperator(
            task_id='run_spark_transform',
            bash_command='spark-submit --master local[2] {pwd}/spark_jobs/transform_netflix.py'.format(pwd=os.environ.get('PWD','/usr/local/airflow')),
            dag=dag
        )

        upload_s3 = BashOperator(
            task_id='upload_s3',
            bash_command='python3 {pwd}/scripts/upload_s3.py'.format(pwd=os.environ.get('PWD','/usr/local/airflow')),
            dag=dag
        )

        put_hdfs = BashOperator(
            task_id='put_hdfs',
            bash_command='bash {pwd}/scripts/hdfs_put.sh'.format(pwd=os.environ.get('PWD','/usr/local/airflow')),
            dag=dag
        )

        load_pg = BashOperator(
            task_id='load_postgres',
            bash_command='python3 {pwd}/scripts/load_postgres.py'.format(pwd=os.environ.get('PWD','/usr/local/airflow')),
            dag=dag
        )

        run_spark >> upload_s3 >> put_hdfs >> load_pg
        """),

    "run_all_local.sh": textwrap.dedent("""\
        #!/bin/bash
        # Script para rodar tudo localmente (assume spark-submit, hadoop, python disponiveis)
        set -e
        echo "Rodando job PySpark..."
        spark-submit spark_jobs/transform_netflix.py

        echo "Subindo para S3..."
        python3 scripts/upload_s3.py

        echo "Copiando para HDFS..."
        bash scripts/hdfs_put.sh

        echo "Importando para Postgres..."
        python3 scripts/load_postgres.py

        echo "Pipeline completo."
        """)
}

# write files
for path, content in files.items():
    fullpath = os
