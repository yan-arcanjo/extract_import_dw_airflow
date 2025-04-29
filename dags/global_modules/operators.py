from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from global_modules.sqlserver_hook import SqlServerHook
from typing import Any
import time
import csv
import os
        
class SqlServerOperator(BaseOperator):
    def __init__ (self, task_id: str, source_conn_id: str, sql_path: str, filename, target_conn_id, target_table, **kwargs):
        super().__init__(task_id=task_id, **kwargs)
        self.conn_id = source_conn_id
        self.sql_path = sql_path
        self.filename = filename
        self.target_conn_id = target_conn_id
        self.target_table = target_table

    def execute(self, context: Any):
        hook = SqlServerHook(self.conn_id)

        # Pega o caminho da DAG que chamou esse hook
        dag_file_path = context['dag'].fileloc
        dag_dir = os.path.dirname(dag_file_path)

        # Constrói o caminho absoluto do arquivo SQL
        full_sql_path = os.path.join(dag_dir, self.sql_path)

        with open(full_sql_path, 'r', encoding='utf-8') as f:
            sql = f.read()

        rows, columns = hook.extract(sql)

        self.save_to_csv(rows, columns)

        self.copy_table()
        
    def save_to_csv(self, rows, columns, batch_size=10000):
        # Salvando no CSV

        path = "/datalake/extraction_routine/"
        file_name = self.filename
        file_path = os.path.join(path, self.filename)
        start_time = time.time()

        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(columns)  # Escreve os cabeçalhos

            for i in range(0, len(rows), batch_size):
                writer.writerows(rows[i:i+batch_size])
       
        print(f'---Arquivo {file_name} salvo com sucesso em {file_path}! -> Tempo decorido: {round(time.time() - start_time,2)} segundos')
        
    

    def copy_table(self):

        start_time = time.time()

        path = "/datalake/extraction_routine/"
        file_name = self.filename
        file_path = os.path.join(path, self.filename)

        hook = PostgresHook(postgres_conn_id=self.target_conn_id)
        
        conn = hook.get_conn()
        cursor = conn.cursor()

        with open(file_path, "r", encoding="utf-8") as f:
            cursor.copy_expert(f"COPY {self.target_table} FROM STDIN WITH CSV HEADER DELIMITER ','", f)

        print(f'---Arquivo {file_name} copiado com sucesso para a tabela {self.target_table}! -> Tempo decorido: {round(time.time() - start_time,2)} segundos')

        conn.commit()
        cursor.close()
        conn.close()
        os.remove(file_path)
