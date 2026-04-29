from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import pandas as pd


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
    # Run per category plus one aggregate for overall total
    categories = list(df['category'].unique()) + ['ALL']

    for category in categories:
        if category == 'ALL':
            cat_df = (
                df.groupby('sales_month')['total_revenue']
                .sum()
                .reset_index()
            )
        else:
            cat_df = (
                df[df['category'] == category]
                .groupby('sales_month')['total_revenue']
                .sum()
                .reset_index()
            )

        # Prophet needs at least 2 full seasonal cycles for yearly seasonality
        if len(cat_df) < 12:
            print(f"SKIP: {category} has only {len(cat_df)} months — skipping.", flush=True)
            continue

        prophet_df = cat_df.rename(columns={'sales_month': 'ds', 'total_revenue': 'y'})

        model = Prophet(
            yearly_seasonality=True,
            weekly_seasonality=False,
            daily_seasonality=False,
            interval_width=0.95,
            seasonality_mode='multiplicative',  # better for revenue with growth trend
        )
        model.fit(prophet_df)

        # Forecast 6 months beyond last known data point
        future = model.make_future_dataframe(periods=6, freq='MS')
        forecast = model.predict(future)

        forecast = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()
        forecast = forecast.merge(prophet_df[['ds', 'y']], on='ds', how='left')

        for _, row in forecast.iterrows():
            results.append({
                'FORECAST_ID':       str(uuid.uuid4()),
                'FORECAST_DATE':     row['ds'].date(),
                'CATEGORY':          category,
                'REVENUE_ACTUAL':    float(row['y']) if pd.notna(row.get('y')) else None,
                'REVENUE_FORECAST':  max(0.0, float(row['yhat'])),
                'REVENUE_LOWER':     max(0.0, float(row['yhat_lower'])),
                'REVENUE_UPPER':     max(0.0, float(row['yhat_upper'])),
                'IS_FUTURE':         bool(pd.isna(row.get('y'))),
                'GENERATED_AT':      generated_at,
            })

        print(f"✅ {category}: {len(forecast)} rows forecasted.", flush=True)

    result_df = pd.DataFrame(results)

    print(f"INFO: Writing {len(result_df)} rows to GOLD.ML_REVENUE_FORECAST...", flush=True)

    # Full refresh — predictions change each run as new data arrives
    sf_hook.run("DROP TABLE IF EXISTS GOLD.ML_REVENUE_FORECAST")

    result_df.to_sql(
        'ML_REVENUE_FORECAST',
        sf_engine,
        schema='GOLD',
        if_exists='append',
        index=False,
        method='multi',
        chunksize=5000,
    )

    print(f"✅ DONE: {len(result_df)} rows written to GOLD.ML_REVENUE_FORECAST.", flush=True)


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
    description='Prophet revenue forecast per category — writes ML_REVENUE_FORECAST to Snowflake GOLD',
    schedule_interval=None,
    catchup=False,
    tags=['alfa_projekt', 'ml', 'prophet', 'forecast'],
) as dag:

    forecast_revenue = PythonOperator(
        task_id='prophet_revenue_forecast',
        python_callable=run_revenue_forecast,
    )
