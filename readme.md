# BigQuery Funnel

Библиотека для анализа воронок пользователей в Google BigQuery.

## Основные возможности

- Построение SQL-запросов для анализа воронок пользователей в BigQuery
- Добавление параметров для каждого события воронки
- Группировка результатов по различным параметрам (например, A/B-тестам)
- Расчет коэффициентов конверсии между шагами воронки
- Анализ оттока пользователей и определение критических точек
- Статистический анализ A/B-тестов
- Визуализация воронок и сравнение разных групп
- **Выполнение произвольных SQL-запросов с параметрами**

## Установка

```bash
pip install bq-funnel
```
Или установить из репозитория:
```bash
git clone https://github.com/Azizbek-Analyst/bq_funnel_and_ab.git
cd bq_funnel_and_ab
pip install -e bq-funnel


```

Для интерактивной аутентификации через браузер также установите:
```bash
pip install pydata-google-auth
```

## Требования

- Python 3.6+
- google-cloud-bigquery>=2.0.0
- pandas>=1.0.0
- numpy>=1.18.0
- matplotlib>=3.2.0 (для визуализации)
- seaborn>=0.10.0 (для визуализации)
- scipy>=1.4.0 (для статистического анализа)
- pydata-google-auth (опционально, для интерактивной аутентификации)

## Структура проекта
```
bq_funnel/
├── __init__.py
├── auth.py                # Аутентификация в BigQuery
├── core.py                # Основной класс BigQueryFunnel
├── query_builder.py       # Построение SQL-запросов для стандартных таблиц
├── query_builder_ga4.py   # Построение SQL-запросов для таблиц GA4
├── setup.py               # Настройка установки пакета
├── analysis/
│   ├── __init__.py
│   ├── ab_test.py         # Анализ A/B-тестов
│   ├── conversion.py      # Расчет конверсии
│   └── dropoff.py         # Анализ отказов
└── visualization/
    ├── funnel_plot.py     # Визуализация воронок
    └── comparison_plot.py # Сравнение воронок
```

## Методы аутентификации в Google Cloud

Библиотека поддерживает несколько способов аутентификации в Google Cloud:

### 1. Интерактивная аутентификация через браузер (pydata_google_auth)

```python
from bq_funnel import authenticate_via_pydata

# Этот метод запустит интерактивную аутентификацию в браузере
client = authenticate_via_pydata(project_id="your-project-id")

# Создание экземпляра BigQueryFunnel с аутентифицированным клиентом
from bq_funnel import BigQueryFunnel
bq = BigQueryFunnel(
    project_id=client.project,
    dataset_id="analytics",
    table_id="events",
    data_source="ga4",
    client=client  # Используем уже аутентифицированный клиент
)
```

### 2. Аутентификация через файл ключа сервисного аккаунта

```python
from bq_funnel import authenticate_with_service_account

client = authenticate_with_service_account(
    credentials_path="/path/to/your-credentials.json",
    project_id="your-project-id"  # Необязательно
)
```

### 3. Через переменную окружения `GOOGLE_APPLICATION_CREDENTIALS`

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-credentials.json" # Linux/Mac
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your-credentials.json # Windows
```

```python
from bq_funnel import setup_bigquery_client

client = setup_bigquery_client() # Будет использована переменная окружения
```

### 4. Через метаданные GCP (при запуске внутри GCP)

```python
from bq_funnel import setup_bigquery_client

client = setup_bigquery_client() # Будет использованы метаданные GCP
```

## Примеры использования

### Работа с данными Google Analytics 4 (GA4)


```python
from bq_funnel import BigQueryFunnel, authenticate_via_pydata

# Интерактивная аутентификация
client = authenticate_via_pydata()

# Инициализация BigQueryFunnel с указанием GA4 как источника данных
bq = BigQueryFunnel(
    project_id=client.project,
    dataset_id="analytics_12345",  # Набор данных GA4
    table_id="events_*",          # GA4 часто использует таблицы с подстановочными знаками для дат
    client=client,
    data_source="ga4"            # Указываем, что источник данных - GA4
)

# Определение воронки для GA4
events = [
    'page_view',
    {'name': 'view_item', 'params': {'page_location': '/products/%'}},
    'add_to_cart',
    'begin_checkout',
    'purchase'
]

date_range = ('2023-01-01', '2023-01-31')
window = '24h'

# Получение данных воронки
df = bq.optimized_funnel(
    events=events,
    date_range=date_range,
    window=window,
    timestamp_field="event_timestamp"  # Поле timestamp в GA4
)
```

Примечания по работе с GA4:
- Используется поле `event_date` для фильтрации по датам вместо преобразования timestamp
- Применяется `GROUP BY ALL` для агрегирования результатов без группировки
- Временные метки хранятся в микросекундах с начала эпохи
- Идентификатор пользователя находится в поле `user_pseudo_id` вместо `user_id`

### Простая воронка

```python
from bq_funnel import BigQueryFunnel, authenticate_via_pydata

# Интерактивная аутентификация через браузер
client = authenticate_via_pydata()

# Инициализация BigQueryFunnel
bq = BigQueryFunnel(
    project_id="your-project-id",
    dataset_id="analytics",
    table_id="events",
    client=client
)

# Определение параметров воронки
events = [
    'app_open',
    'search_initiated',
    'search_results_viewed',
    'item_clicked',
    'checkout_started',
    'purchase_completed'
]

date_range = ('2022-05-01', '2022-05-07')
window = '24h'  # Временное окно - 24 часа

# Получение данных воронки
df = bq.optimized_funnel(
    events=events,
    date_range=date_range,
    window=window
)

# Расчет коэффициентов конверсии
df_with_conv = bq.calculate_conversion_rates(df)


# Визуализация воронки
bq.visualize_funnel(df, title="Воронка покупок")
```



### Воронка с параметрами для событий

```python
# Определение параметров воронки с дополнительными параметрами для событий
events = [
    'login_screen_shown',  # Простое событие как строка
    {'name': 'login_initiated', 'params': {'page': 'main', 'method': 'email'}},  # Событие с параметрами
    'login_success'
]

# Добавление фильтров, применяемых ко всем событиям
filters = {'platform': 'iOS'}

# Получение данных воронки
df = bq.optimized_funnel(
    events=events,
    date_range=date_range,
    window='8h',
    filters=filters
)
```
### Пример 2: Сравнение воронок для разных платформ
```python
# Получение данных для мобильной платформы
mobile_df = funnel.optimized_funnel(
    events=events,
    date_range=("2024-04-01", "2024-04-30"),
    window="24h",
    filters={"platform": "mobile"}
)

# Получение данных для веб-платформы
web_df = funnel.optimized_funnel(
    events=events,
    date_range=("2024-04-01", "2024-04-30"),
    window="24h",
    filters={"platform": "web"}
)

# Сравнение воронок
funnel.compare_funnels(
    dfs=[mobile_df, web_df],
    labels=["Мобильная", "Веб"],
    title="Сравнение воронок по платформам"
)
```
### Произвольные SQL-запросы

Для выполнения произвольных SQL-запросов к BigQuery вы можете использовать метод `custom_query`:

```python
# Подготовка SQL-запроса с параметрами
query = """
SELECT
    event_name,
    COUNT(*) as event_count
FROM `{project}.{dataset}.{table}`
WHERE DATE(timestamp) BETWEEN @start_date AND @end_date
  AND platform = @platform
GROUP BY event_name
ORDER BY event_count DESC
LIMIT 10
"""

# Форматирование запроса с полными именами таблиц
formatted_query = query.format(
    project=bq.client.project,
    dataset=bq.dataset_id,
    table=bq.table_id
)

# Выполнение запроса с параметрами
result = bq.custom_query(
    query=formatted_query,
    params={
        'start_date': '2022-01-01',
        'end_date': '2022-01-31',
        'platform': 'iOS'
    }
)

print(result)
```

#### Оценка стоимости запроса (dry_run)

Перед выполнением дорогостоящих запросов вы можете оценить их стоимость:

```python
# Выполнение dry_run для оценки стоимости запроса
dry_run_result = bq.custom_query(
    query=formatted_query,
    params={
        'start_date': '2022-01-01',
        'end_date': '2022-01-31',
        'platform': 'iOS'
    },
    dry_run=True
)

# Вывод информации о запросе
bytes_processed = dry_run_result.total_bytes_processed
gb_processed = bytes_processed / (1024 ** 3)

print(f"Запрос обработает примерно {bytes_processed:,} байт ({gb_processed:.2f} ГБ)")
print(f"Приблизительная стоимость запроса: ${gb_processed * 5 / 1000:.6f} (при $5 за ТБ)")
```



### Анализ A/B-тестов

```python
# Определение конфигурации A/B-теста
ab_test_config = {
    'table_id': 'your-project-id.your_dataset.ab_tests_sessions',
    'test_code': 'TRAVELUAEAQ',  # код теста
    'user_id_field': 'googleID'  # поле ID пользователя
}

# Получение данных воронки с разбивкой по A/B-группам
funnel_results = funnel.funnel_with_ab_test(
    events=events,
    date_range=("2024-07-01", "2024-08-01"),
    ab_test_config=ab_test_config,
    window='24h'
)

# Доступ к результатам для контрольной и тестовой групп
control_df = funnel_results['control']
test_df = funnel_results['test']

# Статистический анализ значимости результатов
significance = funnel.analyze_ab_test_significance(
    control_df=control_df,
    test_df=test_df,
    first_step='step1_users',
    last_step='step4_users',
    confidence_level=0.95
)

print(f"Конверсия в контрольной группе: {significance['control_conversion']}%")
print(f"Конверсия в тестовой группе: {significance['test_conversion']}%")
print(f"Относительное улучшение: {significance['relative_lift']}%")
print(f"Статистическая значимость: {significance['is_significant']}")

# Сравнение воронок
funnel.compare_funnels(
    dfs=[control_df, test_df],
    labels=["Контроль", "Тест"],
    title="Сравнение воронок A/B-теста"
)
```


## Работа с разными источниками данных

Библиотека поддерживает работу с различными структурами данных:

### Стандартные таблицы событий:
```python
funnel = BigQueryFunnel(
    project_id="your-project-id",
    dataset_id="your_dataset",
    table_id="your_table",
    data_source="standard"  # по умолчанию
)
```

### Таблицы Google Analytics 4 (GA4):
```python
funnel = BigQueryFunnel(
    project_id="your-project-id",
    dataset_id="your_dataset",
    table_id="your_ga4_table",
    data_source="ga4"
)
```

## Документация

Подробная документация доступна по адресу: [docs/](docs/)

## Лицензия

MIT
