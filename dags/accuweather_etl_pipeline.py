# Import DAG and the @task decorator from the Airflow 3 Task SDK
# @task turns a normal Python function into an Airflow task (TaskFlow API)
from airflow.sdk import DAG, task

# HttpHook is used to call REST APIs using an Airflow Connection
from airflow.providers.http.hooks.http import HttpHook

# PostgresHook is used to connect to PostgreSQL using an Airflow Connection
from airflow.providers.postgres.hooks.postgres import PostgresHook

# datetime is used for the DAG start date
from datetime import datetime


# -----------------------------
# Step 1: Define configuration variables
# -----------------------------

# AccuWeather identifies every city by a "location key".
# Find one with: GET /locations/v1/cities/search?q=<city name>
LOCATION_KEY = "198332"   # Manali, Himachal Pradesh, India
CITY_NAME = "Manali"

# Airflow Connection IDs (created from airflow_settings.yaml or Admin -> Connections)
POSTGRES_CONN_ID = "weather_db"          # PostgreSQL database that stores the results
API_CONN_ID = "accuweather_api"          # AccuWeather API (API key is stored as the password)


# -----------------------------
# Step 2: Define the DAG (Workflow)
# -----------------------------
# schedule="@hourly": AccuWeather updates current conditions about once an hour
# catchup=False: Do not run missed historical jobs
# retries: Retry a failed task 2 times (useful when an API is briefly down)
# -----------------------------

with DAG(
    dag_id="accuweather_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args={"retries": 2},
    tags=["weather", "accuweather", "etl"],
) as dag:

    # -----------------------------
    # Task 1: Extract - call the AccuWeather Current Conditions API
    # -----------------------------
    @task
    def extract_current_conditions():
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")

        # Read the API key from the connection's password field
        # so it never appears in the DAG code or in the request URL
        api_key = http_hook.get_connection(API_CONN_ID).password

        # GET https://dataservice.accuweather.com/currentconditions/v1/{locationKey}?details=true
        response = http_hook.run(
            endpoint=f"currentconditions/v1/{LOCATION_KEY}",
            data={"details": "true"},
            headers={"Authorization": f"Bearer {api_key}"},
        )

        # The API returns a list with one observation
        return response.json()[0]

    # -----------------------------
    # Task 2: Transform - keep only the fields we want, flattened
    # -----------------------------
    @task
    def transform_current_conditions(raw: dict):
        return {
            "location_key": LOCATION_KEY,
            "city": CITY_NAME,
            "observed_at": raw["LocalObservationDateTime"],
            "weather_text": raw["WeatherText"],
            "is_daytime": raw["IsDayTime"],
            "has_precipitation": raw["HasPrecipitation"],
            "temperature_c": raw["Temperature"]["Metric"]["Value"],
            "real_feel_c": raw["RealFeelTemperature"]["Metric"]["Value"],
            "humidity_pct": raw["RelativeHumidity"],
            "wind_speed_kmh": raw["Wind"]["Speed"]["Metric"]["Value"],
            "wind_direction": raw["Wind"]["Direction"]["English"],
            "pressure_mb": raw["Pressure"]["Metric"]["Value"],
            "uv_index": raw["UVIndex"],
            "cloud_cover_pct": raw["CloudCover"],
            "visibility_km": raw["Visibility"]["Metric"]["Value"],
        }

    # -----------------------------
    # Task 3: Load - insert the row into PostgreSQL
    # -----------------------------
    @task
    def load_current_conditions(row: dict):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Create the table the first time the DAG runs
        # UNIQUE (location_key, observed_at) stops duplicate rows
        # if the DAG runs again before AccuWeather publishes a new observation
        pg_hook.run("""
            CREATE TABLE IF NOT EXISTS accuweather_current_conditions (
                location_key      TEXT,
                city              TEXT,
                observed_at       TIMESTAMPTZ,
                weather_text      TEXT,
                is_daytime        BOOLEAN,
                has_precipitation BOOLEAN,
                temperature_c     FLOAT,
                real_feel_c       FLOAT,
                humidity_pct      INT,
                wind_speed_kmh    FLOAT,
                wind_direction    TEXT,
                pressure_mb       FLOAT,
                uv_index          INT,
                cloud_cover_pct   INT,
                visibility_km     FLOAT,
                loaded_at         TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (location_key, observed_at)
            );
        """)

        columns = list(row.keys())
        placeholders = ", ".join(["%s"] * len(columns))

        pg_hook.run(
            f"""
            INSERT INTO accuweather_current_conditions ({", ".join(columns)})
            VALUES ({placeholders})
            ON CONFLICT (location_key, observed_at) DO NOTHING;
            """,
            parameters=tuple(row.values()),
        )

    # -----------------------------
    # Step 3: Set the task order (Extract -> Transform -> Load)
    # -----------------------------
    load_current_conditions(transform_current_conditions(extract_current_conditions()))
