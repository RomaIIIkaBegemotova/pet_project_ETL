# Выбор образа .... 
FROM apache/airflow:3.1.0-python3.12

# Переключаемся на root для установки пакетов
USER airflow

# Копируем DAGs и requirements
COPY dags/ /opt/airflow/dags/
COPY requirements.txt /requirements.txt


# Установка зависимостей Python из файла
RUN pip install --no-cache-dir -r /requirements.txt