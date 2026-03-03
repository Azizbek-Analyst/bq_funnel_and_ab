# BigQuery Funnel

Library for analyzing user funnels in Google BigQuery.

## Key Features

- Building SQL queries to analyze user funnels in BigQuery
- Adding parameters for each funnel event
- Grouping results according to various parameters (for example, A/B-tests)
- Calculating conversion rates between funnel steps
- Analysis of user churn and identification of critical points
- Statistical analysis A/B-tests
- Funnel visualization and comparison of different groups
- **Executing arbitrary SQL queries with parameters**

## Installation

```bash
pip install bq-funnel
```
Or install from the repository:
```bash
git clone https://github.com/Azizbek-Analyst/bq_funnel_and_ab.git
cd bq_funnel_and_ab
pip install -e .
```

If you want to install only the required dependencies:
```bash
pip install -r requirements.txt
```

### Quick installation using scripts

For Linux/Mac:
```bash
chmod +x install.sh
./install.sh
```

For Windows (run as administrator):
```
install.bat
```

These scripts will create a Python virtual environment and install the library automatically.

For interactive authentication via browser, also set:
```bash
pip install pydata-google-auth
```

## Requirements

- Python 3.6+
- google-cloud-bigquery>=2.0.0
- pandas>=1.0.0
- numpy>=1.18.0
- matplotlib>=3.2.0 (for visualization)
- seaborn>=0.10.0 (for visualization)
- scipy>=1.4.0 (for statistical analysis)
- pydata-google-auth (optional, for interactive authentication)

## Project structure
```
bq_funnel/
├── __init__.py
├── auth.py                # Authentication in BigQuery
├── core.py                # BigQueryFunnel main class
├── query_builder.py       # Building SQL queries for standard tables
├── query_builder_ga4.py   # Building SQL queries for GA4 tables
├── setup.py               # Configuring package installation
├── analysis/
│   ├── __init__.py
│   ├── ab_test.py         # A/B test analysis
│   ├── conversion.py      # Conversion calculation
│   └── dropoff.py         # Failure Analysis
└── visualization/
    ├── funnel_plot.py     # Funnel visualization
    └── comparison_plot.py # Funnel comparison
```

## Google Cloud Authentication Methods

The library supports several authentication methods in Google Cloud:

### 1. Interactive authentication via browser (pydata_google_auth)

```python
from bq_funnel import authenticate_via_pydata

# This method will initiate interactive authentication in the browser
client = authenticate_via_pydata(project_id="your-project-id")

# Creating a BigQueryFunnel instance with an authenticated client
from bq_funnel import BigQueryFunnel
bq = BigQueryFunnel(
    project_id=client.project,
    dataset_id="analytics",
    table_id="events",
    data_source="ga4",
    client=client  # We use an already authenticated client
)
```

### 2. Authentication via service account key file

```python
from bq_funnel import authenticate_with_service_account

client = authenticate_with_service_account(
    credentials_path="/path/to/your-credentials.json",
    project_id="your-project-id"  # Optional
)
```

### 3. Via environment variable `GOOGLE_APPLICATION_CREDENTIALS`

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your-credentials.json" # Linux/Mac
set GOOGLE_APPLICATION_CREDENTIALS=C:\path\to\your-credentials.json # Windows
```

```python
from bq_funnel import setup_bigquery_client

client = setup_bigquery_client() # The environment variable will be used
```

### 4. Via GCP metadata (when running inside GCP)

```python
from bq_funnel import setup_bigquery_client

client = setup_bigquery_client() # GCP metadata will be used
```

## Examples of use

### Working with Google Analytics 4 (GA4) data)


```python
from bq_funnel import BigQueryFunnel, authenticate_via_pydata

# Interactive authentication
client = authenticate_via_pydata()

# Initializing BigQueryFunnel with GA4 as data source
bq = BigQueryFunnel(
    project_id=client.project,
    dataset_id="analytics_12345",  # GA4 Dataset
    table_id="events_*",          # GA4 often uses wildcard tables for dates
    client=client,
    data_source="ga4"            # We indicate that the data source is GA4
)

# Funnel definition for GA4
events = [
    'page_view',
    {'name': 'view_item', 'params': {'page_location': '/products/%'}},
    'add_to_cart',
    'begin_checkout',
    'purchase'
]

date_range = ('2023-01-01', '2023-01-31')
window = '24h'

# Getting Funnel Data
df = bq.optimized_funnel(
    events=events,
    date_range=date_range,
    window=window,
    timestamp_field="event_timestamp"  # Timestamp field in GA4
)
```

Notes on working with GA4:
- Field used `event_date` to filter by dates instead of timestamp conversion
- Applies `GROUP BY ALL` to aggregate results without grouping
- Timestamps are stored in microseconds since the start of the epoch
- The user ID is in the field `user_pseudo_id` instead of `user_id`

### Simple funnel

```python
from bq_funnel import BigQueryFunnel, authenticate_via_pydata

# Interactive authentication via browser
client = authenticate_via_pydata()

# Initializing BigQueryFunnel
bq = BigQueryFunnel(
    project_id="your-project-id",
    dataset_id="analytics",
    table_id="events",
    client=client
)

# Determining funnel parameters
events = [
    'app_open',
    'search_initiated',
    'search_results_viewed',
    'item_clicked',
    'checkout_started',
    'purchase_completed'
]

date_range = ('2022-05-01', '2022-05-07')
window = '24h'  # Time window - 24 hours

# Getting Funnel Data
df = bq.optimized_funnel(
    events=events,
    date_range=date_range,
    window=window
)

# Calculation of conversion rates
df_with_conv = bq.calculate_conversion_rates(df)


# Funnel visualization
bq.visualize_funnel(df, title="Purchase funnel")
```



### Funnel with parameters for events

```python
# Defining funnel parameters with additional parameters for events
events = [
    'login_screen_shown',  # Simple event as a string
    {'name': 'login_initiated', 'params': {'page': 'main', 'method': 'email'}},  # Event with parameters
    'login_success'
]

# Add filters that apply to all events
filters = {'platform': 'iOS'}

# Getting Funnel Data
df = bq.optimized_funnel(
    events=events,
    date_range=date_range,
    window='8h',
    filters=filters
)
```
### Example 2: Comparison of funnels for different platforms
```python
# Receiving data for the mobile platform
mobile_df = funnel.optimized_funnel(
    events=events,
    date_range=("2024-04-01", "2024-04-30"),
    window="24h",
    filters={"platform": "mobile"}
)

# Receiving data for the web platform
web_df = funnel.optimized_funnel(
    events=events,
    date_range=("2024-04-01", "2024-04-30"),
    window="24h",
    filters={"platform": "web"}
)

# Funnel comparison
funnel.compare_funnels(
    dfs=[mobile_df, web_df],
    labels=["Mobile", "Web"],
    title="Comparison of funnels by platform"
)
```
### Custom SQL queries

To run arbitrary SQL queries against BigQuery you can use the method `custom_query`:

```python
# Preparing an SQL query with parameters
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

# Formatting a Query with Fully Qualified Table Names
formatted_query = query.format(
    project=bq.client.project,
    dataset=bq.dataset_id,
    table=bq.table_id
)

# Executing a query with parameters
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

#### Request cost estimate (dry_run)

Before running expensive queries, you can estimate their cost:

```python
# Running dry_run to estimate the cost of the request
dry_run_result = bq.custom_query(
    query=formatted_query,
    params={
        'start_date': '2022-01-01',
        'end_date': '2022-01-31',
        'platform': 'iOS'
    },
    dry_run=True
)

# Displaying request information
bytes_processed = dry_run_result.total_bytes_processed
gb_processed = bytes_processed / (1024 ** 3)

print(f"The request will process approximately {bytes_processed:,} byte ({gb_processed:.2f} GB)")
print(f"Approximate cost of request: ${gb_processed * 5 / 1000:.6f} (at $5 for TB)")
```



### A/B test analysis

```python
# Defining Configuration A/B-test
ab_test_config = {
    'table_id': 'your-project-id.your_dataset.ab_tests_sessions',
    'test_code': 'TRAVELUAEAQ',  # test code
    'user_id_field': 'googleID'  # user ID field
}

# Retrieving funnel data broken down by A/B-groups
funnel_results = funnel.funnel_with_ab_test(
    events=events,
    date_range=("2024-07-01", "2024-08-01"),
    ab_test_config=ab_test_config,
    window='24h'
)

# Access to results for control and test groups
control_df = funnel_results['control']
test_df = funnel_results['test']

# Statistical analysis of the significance of the results
significance = funnel.analyze_ab_test_significance(
    control_df=control_df,
    test_df=test_df,
    first_step='step1_users',
    last_step='step4_users',
    confidence_level=0.95
)

print(f"Conversion in the control group: {significance['control_conversion']}%")
print(f"Conversion in test group: {significance['test_conversion']}%")
print(f"Relative improvement: {significance['relative_lift']}%")
print(f"Statistical significance: {significance['is_significant']}")

# Funnel comparison
funnel.compare_funnels(
    dfs=[control_df, test_df],
    labels=["Control", "Test"],
    title="Comparison of funnels A/B-test"
)
```


## Working with different data sources

The library supports working with various data structures:

### Standard event tables:
```python
funnel = BigQueryFunnel(
    project_id="your-project-id",
    dataset_id="your_dataset",
    table_id="your_table",
    data_source="standard"  # default
)
```

### Google Analytics 4 Tables (GA4):
```python
funnel = BigQueryFunnel(
    project_id="your-project-id",
    dataset_id="your_dataset",
    table_id="your_ga4_table",
    data_source="ga4"
)
```

## Documentation

Detailed documentation is available at: [docs/](docs/)

## License

MIT
