import pyodbc
import logging
import time
from airflow.hooks.base import BaseHook

class SqlServerHook(BaseHook):
    template_fields = ("sql",)

    def __init__(self, conn_id: str):
        super().__init__()
        self.conn_id = conn_id
        self.conn = self.get_connection(conn_id)

    def get_conn(self):
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={self.conn.host};"
            f"DATABASE={self.conn.schema};"
            f"UID={self.conn.login};"
            f"PWD={self.conn.password}"
        )
        
        try: 
            conn = pyodbc.connect(conn_str)
            print('---Sucesso ao se conectar ao banco')
            return conn
        
        except Exception as e:
            logging.error(f'---Erro ao se conectar ao banco: {e}')
            raise
        
           
    def extract(self, sql:str):

        conn = self.get_conn()

        cursor = conn.cursor()


        try:
            print(f'---Iniciado consulta: {sql}')

            start_time = time.time()
            cursor.execute(sql)
             
            print(f'---Consulta finalizada -> Tempo decorrido: {round(time.time() - start_time,2)} segundos')

            columns = [column[0] for column in cursor.description]
            
            rows = cursor.fetchall()

            return rows, columns
            
        except Exception as e:
            cursor.close()
            conn.close() 
            logging.error(f"Erro ao fazer a consulta: {e}")
            raise

        
        

