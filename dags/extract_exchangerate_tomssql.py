from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import json
import pyodbc
import logging
import requests
import pendulum

log = logging.getLogger(__name__)

#Example 'Driver={ODBC Driver 17 for SQL Server};Server=localhost,1433;Database=master;uid={ЗАМЕНИ МЕНЯ};pwd={ЗАМЕНИ МЕНЯ}'
ConnectionStringMSSQL = Variable.get("ConnectionStringMSSQL")

sql_create_database = f'''
if db_id(N'Stage') is null
begin
    create database Stage;
end
'''
# Sql запрос создания таблицы
sql_create_table = f'''
if object_id(N'Stage.dbo.CurrencyRate', N'U') is null
begin
    create table Stage.dbo.CurrencyRate (
        Id int identity(1,1) not null
            constraint PK_CurrencyRate primary key,
        CurrencyFrom nvarchar(max) not null,
        CurrencyTo nvarchar(max) not null,
        [Date] nvarchar(max) not null,
        Rate nvarchar(max) not null,
        LoadDatetime datetime2 default getdate()
    );
end
'''

def get_rate(date=None):
    """
    Функция возвращает отношене BTC к USD (Биткоин к доллару).
    :param date: Дата, за которую предполагается получить отношение. В случае, если ничего не указано,
    возвращается отношение на момент вызова функции.
    :return: JSON ответ от API https://api.exchangerate.host
    
    Example:
        data = get_rate('2022-03-17')
        print(data['query']['from'], data['query']['to'], data['date'], data['result'])
    """
    if date is None:
        url = 'https://api.exchangerate.host/convert?from=BTC&to=USD'
    else:
        url = f'https://api.exchangerate.host/convert?from=BTC&to=USD&date={date}'
    response = requests.get(url)
    return response.json()

def get_msssql_connection():
    """
    Установка соединения с Sql Server
    """
    connection = pyodbc.connect(ConnectionStringMSSQL, autocommit=True)
    return connection
    
def process_rate(**kwargs):
   ti = kwargs["ti"]
   ti.xcom_push("rate", get_rate())

def to_mssql(ti):
    """
    Запись в базу MS SQL 
    """
    data = ti.xcom_pull(task_ids="get_rate_operator", key="rate")
    with get_msssql_connection() as con:
        cursor = con.cursor()
        cursor.execute(sql_create_database)
        cursor.execute(sql_create_table)
        print(data['query']['from'], data['query']['to'], data['date'], data['result'])
        cursor.execute(f'''
        insert into Stage.dbo.CurrencyRate (CurrencyFrom, CurrencyTo, [Date], Rate)
            values(?, ?, ?, ?)
        ''',
        (data['query']['from'], data['query']['to'], data['date'], data['result']))
        con.commit()
        cursor.close()

with DAG(
    dag_id=f'extract_exchangerate',
    schedule_interval='0 */3 * * *',
    start_date=pendulum.datetime(2022, 3, 20, tz="UTC"),
    catchup=False,
) as dag:
    step_1 = DummyOperator(task_id='extract')
    get_rate_operator = PythonOperator(task_id='get_rate_operator', python_callable=process_rate, dag=dag)  
    to_mssql_operator = PythonOperator(task_id='to_mssql_operator', python_callable=to_mssql, dag=dag)
    
    step_1 >> get_rate_operator >> to_mssql_operator