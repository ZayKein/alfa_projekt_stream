from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd


# ── 1. REVENUE FORECAST (Facebook Prophet) ───────────────────────────────────

def run_revenue_forecast():
    from prophet import Prophet
    import uuid

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sf_engine = sf_hook.get_sqlalchemy_engine()

    print("INFO: Pulling historical revenue data from Snowflake GOLD...", flush=True)

    query = """
        SELECT
            DATE_TRUNC('month', SALES_MONTH)::DATE AS SALES_MONTH,
            CATEGORY,
            SUM(TOTAL_REVENUE) AS TOTAL_REVENUE
        FROM GOLD.MART_MONTHLY_PRODUCT_SALES
        GROUP BY 1, 2
        ORDER BY 1, 2
    """
    df = pd.read_sql(query, sf_engine)
    df['SALES_MONTH'] = pd.to_datetime(df['SALES_MONTH'])
    df.columns = [c.lower() for c in df.columns]

    print(f"INFO: Loaded {len(df)} rows across {df['category'].nunique()} categories.", flush=True)

    results = []
    generated_at = datetime.utcnow()
    categories = list(df['category'].unique()) + ['ALL']

    for category in categories:
        if category == 'ALL':
            cat_df = df.groupby('sales_month')['total_revenue'].sum().reset_index()
        else:
            cat_df = (
                df[df['category'] == category]
                .groupby('sales_month')['total_revenue']
                .sum()
                .reset_index()
            )

        if len(cat_df) < 12:
            print(f"SKIP: {category} has only {len(cat_df)} months — skipping.", flush=True)
            continue

        prophet_df = cat_df.rename(columns={'sales_month': 'ds', 'total_revenue': 'y'})

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.95,
            seasonality_mode='multiplicative',
        )
        model.fit(prophet_df)

        future = model.make_future_dataframe(periods=6, freq='MS')
        forecast = model.predict(future)
        forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        forecast = forecast.merge(prophet_df[['ds', 'y']], on='ds', how='left')

        for _, row in forecast.iterrows():
            results.append({
                'FORECAST_ID':      str(uuid.uuid4()),
                'FORECAST_DATE':    row['ds'].date(),
                'CATEGORY':         category,
                'REVENUE_ACTUAL':   float(row['y']) if pd.notna(row.get('y')) else None,
                'REVENUE_FORECAST': max(0.0, float(row['yhat'])),
                'REVENUE_LOWER':    max(0.0, float(row['yhat_lower'])),
                'REVENUE_UPPER':    max(0.0, float(row['yhat_upper'])),
                'IS_FUTURE':        bool(pd.isna(row.get('y'))),
                'GENERATED_AT':     generated_at,
            })

        print(f"✅ {category}: {len(forecast)} rows forecasted.", flush=True)

    result_df = pd.DataFrame(results)
    sf_hook.run("DROP TABLE IF EXISTS GOLD.ML_REVENUE_FORECAST")
    result_df.to_sql(
        'ML_REVENUE_FORECAST', sf_engine, schema='GOLD',
        if_exists='append', index=False, method='multi', chunksize=5000,
    )
    print(f"✅ DONE: {len(result_df)} rows → GOLD.ML_REVENUE_FORECAST", flush=True)


# ── 2. ANOMALY DETECTION (Z-score) ───────────────────────────────────────────

def run_anomaly_detection():
    import uuid
    import numpy as np

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sf_engine = sf_hook.get_sqlalchemy_engine()

    print("INFO: Pulling conversion and attach rate data for anomaly detection...", flush=True)

    # Product conversion rates
    product_df = pd.read_sql("""
        SELECT
            TRAFFIC_MONTH        AS PERIOD,
            PRODUCT_ID::VARCHAR  AS ENTITY_ID,
            PRODUCT_NAME         AS ENTITY_NAME,
            CATEGORY,
            CONVERSION_RATE_PCT  AS METRIC_VALUE
        FROM GOLD.MART_TRAFFIC_CONVERSION_BY_PRODUCT
        WHERE CONVERSION_RATE_PCT IS NOT NULL
    """, sf_engine)
    product_df['ENTITY_TYPE'] = 'PRODUCT'
    product_df['METRIC_NAME'] = 'CONVERSION_RATE_PCT'

    # Employee addon attach rates
    employee_df = pd.read_sql("""
        SELECT
            PERFORMANCE_MONTH      AS PERIOD,
            EMPLOYEE_ID::VARCHAR   AS ENTITY_ID,
            EMPLOYEE_NAME          AS ENTITY_NAME,
            NULL                   AS CATEGORY,
            ADDON_ATTACH_RATE_PCT  AS METRIC_VALUE
        FROM GOLD.MART_EMPLOYEE_ADDON_PERFORMANCE
        WHERE ADDON_ATTACH_RATE_PCT IS NOT NULL
    """, sf_engine)
    employee_df['ENTITY_TYPE'] = 'EMPLOYEE'
    employee_df['METRIC_NAME'] = 'ADDON_ATTACH_RATE_PCT'

    combined = pd.concat([product_df, employee_df], ignore_index=True)
    combined['PERIOD'] = pd.to_datetime(combined['PERIOD'])
    combined.columns = [c.upper() for c in combined.columns]

    generated_at = datetime.utcnow()
    results = []

    for (entity_type, entity_id), group in combined.groupby(['ENTITY_TYPE', 'ENTITY_ID']):
        if len(group) < 3:
            continue

        values = group['METRIC_VALUE'].astype(float)
        mean = values.mean()
        std = values.std()

        if std == 0:
            continue

        for _, row in group.iterrows():
            z = (float(row['METRIC_VALUE']) - mean) / std
            results.append({
                'ANOMALY_ID':        str(uuid.uuid4()),
                'ENTITY_TYPE':       row['ENTITY_TYPE'],
                'ENTITY_ID':         row['ENTITY_ID'],
                'ENTITY_NAME':       row['ENTITY_NAME'],
                'METRIC_NAME':       row['METRIC_NAME'],
                'PERIOD':            row['PERIOD'].date(),
                'METRIC_VALUE':      float(row['METRIC_VALUE']),
                'BASELINE_MEAN':     round(mean, 6),
                'BASELINE_STD':      round(std, 6),
                'Z_SCORE':           round(z, 4),
                'IS_ANOMALY':        bool(abs(z) > 2.0),
                'ANOMALY_DIRECTION': 'HIGH' if z > 0 else 'LOW',
                'GENERATED_AT':      generated_at,
            })

    result_df = pd.DataFrame(results)
    anomaly_count = result_df['IS_ANOMALY'].sum()

    sf_hook.run("DROP TABLE IF EXISTS GOLD.ML_ANOMALY_FLAGS")
    result_df.to_sql(
        'ML_ANOMALY_FLAGS', sf_engine, schema='GOLD',
        if_exists='append', index=False, method='multi', chunksize=5000,
    )
    print(f"✅ DONE: {len(result_df)} rows → GOLD.ML_ANOMALY_FLAGS ({anomaly_count} anomalies flagged)", flush=True)


# ── 3. TRAFFIC PATTERN PREDICTION (Ridge Regression) ─────────────────────────

def run_traffic_prediction():
    from sklearn.linear_model import Ridge
    from sklearn.preprocessing import OneHotEncoder
    from sklearn.pipeline import Pipeline
    import numpy as np

    sf_hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    sf_engine = sf_hook.get_sqlalchemy_engine()

    print("INFO: Pulling hourly traffic data for pattern prediction...", flush=True)

    df = pd.read_sql("""
        SELECT
            HOUR_OF_DAY,
            DAYOFWEEKISO(EVENT_HOUR)  AS DAY_OF_WEEK,
            DAYNAME(EVENT_HOUR)       AS DAY_NAME,
            TOTAL_VISITS
        FROM GOLD.MART_HOURLY_TRAFFIC_CONVERSION
        WHERE TOTAL_VISITS IS NOT NULL
    """, sf_engine)

    df.columns = [c.upper() for c in df.columns]
    print(f"INFO: Loaded {len(df)} hourly rows for training.", flush=True)

    X = df[['HOUR_OF_DAY', 'DAY_OF_WEEK']].astype(str)
    y = df['TOTAL_VISITS'].astype(float)

    model = Pipeline([
        ('enc', OneHotEncoder(handle_unknown='ignore', sparse_output=False)),
        ('reg', Ridge(alpha=1.0)),
    ])
    model.fit(X, y)

    # Predict for all 168 hour × day combinations
    hours = list(range(24))
    days = list(range(1, 8))  # ISO: 1=Monday … 7=Sunday
    day_names = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday',
                 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}

    grid = pd.DataFrame(
        [(h, d) for d in days for h in hours],
        columns=['HOUR_OF_DAY', 'DAY_OF_WEEK']
    )
    grid_X = grid.astype(str)
    grid['PREDICTED_VISITS'] = model.predict(grid_X).clip(min=0).round(2)
    grid['DAY_NAME'] = grid['DAY_OF_WEEK'].map(day_names)
    grid['GENERATED_AT'] = datetime.utcnow()

    generated_at = datetime.utcnow()
    sf_hook.run("DROP TABLE IF EXISTS GOLD.ML_TRAFFIC_PREDICTION")
    grid.to_sql(
        'ML_TRAFFIC_PREDICTION', sf_engine, schema='GOLD',
        if_exists='append', index=False, method='multi', chunksize=5000,
    )
    print(f"✅ DONE: {len(grid)} rows → GOLD.ML_TRAFFIC_PREDICTION", flush=True)


# ── DAG DEFINITION ────────────────────────────────────────────────────────────

default_args = {
    'owner': 'alfa_projekt',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '06_ML_Predictions',
    default_args=default_args,
    description='ML layer: Prophet revenue forecast, Z-score anomaly detection, Ridge traffic prediction',
    schedule_interval=None,
    catchup=False,
    tags=['alfa_projekt', 'ml', 'prophet', 'forecast', 'anomaly', 'regression'],
) as dag:

    forecast_revenue = PythonOperator(
        task_id='prophet_revenue_forecast',
        python_callable=run_revenue_forecast,
    )

    detect_anomalies = PythonOperator(
        task_id='zscore_anomaly_detection',
        python_callable=run_anomaly_detection,
    )

    predict_traffic = PythonOperator(
        task_id='ridge_traffic_prediction',
        python_callable=run_traffic_prediction,
    )

    # All three are independent — run in parallel
    [forecast_revenue, detect_anomalies, predict_traffic]
