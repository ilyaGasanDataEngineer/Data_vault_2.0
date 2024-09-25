# Пайплайн для данных из JSONPlaceholder API

## Описание
Этот проект демонстрирует простой пайплайн данных, который получает данные с REST API, обрабатывает их через Kafka и загружает в базу данных PostgreSQL. Пайплайн следует методологии **Data Vault 2.0** и реализован с использованием **Apache Airflow** для оркестрации.

## Этапы пайплайна

1. **Отправка данных в Kafka**:
    - Данные извлекаются из API JSONPlaceholder и отправляются в топик Kafka `jsonplaceholder_api`.

2. **Получение данных из Kafka**:
    - Консюмер Kafka читает сообщения из топика, обрабатывает их и вставляет в таблицу **staging** (STG) в базе данных PostgreSQL.

3. **Трансформация данных из STG в DDS (Data Vault)**:
    - Данные преобразуются из STG таблицы в структуру **Data Vault 2.0** и сохраняются в **хабы**, **ссылки** и **сателлиты** для дальнейшей обработки и анализа.

## Технологический стек

- **Apache Airflow**: Оркестрация пайплайна данных.
- **Kafka**: Платформа для потоковой передачи данных и обмена сообщениями.
- **PostgreSQL**: База данных для хранения и управления данными.
- **Data Vault 2.0**: Методология для управления историческими данными с гибкостью и масштабируемостью.

## Файлы

- **airflow_dag.py**: Основной файл DAG, определяющий пайплайн и задачи.
- **requirements.txt**: Содержит зависимости, необходимые для проекта (Kafka, psycopg2, requests и т.д.).

## Установка и использование

### Требования
1. **Kafka** и **PostgreSQL** должны быть установлены и запущены локально.
2. **Apache Airflow** должен быть установлен и настроен.

### Шаги для запуска

1. **Клонирование репозитория**:

   git clone https://github.com/ilyaGasanDataEngineer/Data_vault_2.0.git
   cd Data_vault_2.0
   ```

2. **Установка зависимостей**:
   pip install -r requirements.txt
   ```

3. **Запуск Airflow**:
   - Запустите планировщик и веб-сервер Airflow:
   airflow scheduler
   airflow webserver
   ```

4. **Настройка Kafka**:
   - Создайте топик Kafka `jsonplaceholder_api`.

5. **Создание БД и создание таблиц**
   - CREATE DATABASE tz_gazprom
   - CREATE TABLE stg_api_data (
    user_id INT,
    id INT PRIMARY KEY,
    title TEXT,
    body TEXT,
    load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    record_source TEXT
);
   - CREATE TABLE dds_hub_user (
    user_id INT PRIMARY KEY,  -- Бизнес-ключ (user_id)
    load_date TIMESTAMP,      -- Дата загрузки
    record_source TEXT        -- Источник данных
);
   - CREATE TABLE dds_hub_post (
    post_id INT PRIMARY KEY,  -- Бизнес-ключ (post_id)
    load_date TIMESTAMP,      -- Дата загрузки
    record_source TEXT        -- Источник данных
);
   - CREATE TABLE dds_link_user_post (
    user_id INT,              -- Бизнес-ключ пользователя
    post_id INT,              -- Бизнес-ключ поста
    load_date TIMESTAMP,      -- Дата загрузки
    record_source TEXT,       -- Источник данных
    PRIMARY KEY (user_id, post_id),  -- Первичный ключ по паре (user_id, post_id)
    FOREIGN KEY (user_id) REFERENCES dds_hub_user(user_id),
    FOREIGN KEY (post_id) REFERENCES dds_hub_post(post_id)
);
   - CREATE TABLE dds_sat_post (
    post_id INT,              -- Бизнес-ключ поста (ссылка на хаб)
    title TEXT,               -- Название поста
    body TEXT,                -- Тело поста
    load_date TIMESTAMP,      -- Дата загрузки
    record_source TEXT,       -- Источник данных
    PRIMARY KEY (post_id, load_date),  -- Первичный ключ по комбинации (post_id, load_date)
    FOREIGN KEY (post_id) REFERENCES dds_hub_post(post_id)
);
Для оптимизации запросов, в данный момент создание индексов не требуется, так как таблица не растет, но при обновлении 
данных и увеличении объемов данных, можно  создать индексы в stg слое, Хабов, ссылок и сателлитов, так как архитектура Data Vault
подразумевает, что схема бд будет находится в 3 нормальной форме и для быстрого построения витрин данных с использованием joins 
необходимо создать индексы.  Я бы использовал hash index.(быстрый поиск по точному совпадению)

6. **Запуск DAG**:
   - Откройте интерфейс Airflow по адресу `http://localhost:8080`.
   - Включите и запустите DAG `gazprom_pipeline`.

### Схема базы данных

- **stg_api_data**: Хранит необработанные данные, полученные из API.
- **dds_hub_user**: Хранит уникальные идентификаторы пользователей.
- **dds_hub_post**: Хранит уникальные идентификаторы постов.
- **dds_link_user_post**: Связи между пользователями и постами.
- **dds_sat_post**: Хранит детальную информацию о постах (заголовок, текст).



