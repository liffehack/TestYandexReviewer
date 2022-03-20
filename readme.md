# Что сделать
	Используя API [exchangerate.host](https://exchangerate.host/) подготовьте ETL процесс с помощью Airflow на python, для выгрузки данных по валютной паре BTC/USD (Биткоин к доллару), выгружать следует с шагом 3 часа и записывать данные в БД (валютная пара, дата, текущий курс).
	В качестве задания со звёздочкой можете учесть вариант заполнения базы историческими данными (API предоставляет специальный endpoint для выгрузки исторических данных). 
# Уточнения по способам решения
	Напишите код на Python.
	Для автоматического запуска выгрузки используйте Airflow любой версии.
	База может быть любая удобная вам: MongoDB, Postgres, Clickhouse.
	Оберните всё в контейнеры и запускайте с помощью docker-compose.
	Для запуска Airflow в docker-compose можно воспользоваться следующей инструкцией: [https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html)

## Запуск контейнера следующей командой:
	docker-compose up -- build

## Даг под ежедневный запуск:
	\dags\extract_exchangerate_tomssql.py

## Даг для загрузки истории:
	\dags\extract_history_exchangerate_tomssql.py

### Перед запуском дагов необходимо добавить Varable в Airflow под названием ConnectionStringMSSQL, которая будет хранить значение connectionstring'а для подключения к базе библиотекой pyodbc, например:
	Driver={ODBC Driver 17 for SQL Server};Server=localhost,1433;Database=Stage;uid={ЗАМЕНИ МЕНЯ};pwd={ЗАМЕНИ МЕНЯ}
# Для дага extract_history_exchangerate_tomssql поменять значения параметров date_from (Дата начала периода) и date_to (Дата окончания периода).
