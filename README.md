# AccuWeather ETL Pipeline with Apache Airflow (Astronomer)

An ETL pipeline built with **Apache Airflow 3** on the **Astro CLI**. Every hour it reads the current weather for a city from the
[AccuWeather Core Weather API](https://developer.accuweather.com/core-weather/location-key-currentconditions)
and loads it into a **PostgreSQL** database.

```
AccuWeather API  ──►  extract_current_conditions  ──►  transform_current_conditions  ──►  load_current_conditions  ──►  PostgreSQL
 (location key)        (HttpHook + API key)             (flatten JSON, 15 fields)          (PostgresHook, no duplicates)   (weather-db)
```

What you learn:

- Calling a REST API that needs an **API key**, with the key kept safe in an **Airflow Connection** (never in the code).
- The **TaskFlow API** (`@task`), with data passed between tasks through **XCom**.
- Adding your own database to the Astro project with **docker-compose.override.yml**.
- Loading rows without duplicates by using `UNIQUE` and `ON CONFLICT DO NOTHING`.

---

## Project structure

```
accuweather-etl-airflow/
├── dags/
│   └── accuweather_etl_pipeline.py     # the ETL DAG
├── tests/dags/test_dag_example.py      # checks: no import errors, tags set, retries >= 2
├── docker-compose.override.yml         # extra PostgreSQL database "weather-db" for the results
├── airflow_settings.example.yaml       # template for the Airflow connections (copy to airflow_settings.yaml)
├── requirements.txt                    # HTTP and PostgreSQL providers
├── Dockerfile                          # Astro Runtime 3.4 (Airflow 3)
└── README.md
```

---

## Prerequisites

| Tool | Why |
|---|---|
| **Docker Desktop** | Runs Airflow and PostgreSQL in containers |
| **Astro CLI** | Starts Airflow locally (`astro dev start`) |
| **AccuWeather API key** | Free account at [developer.accuweather.com](https://developer.accuweather.com) |
| **Git** | To clone this repository |

Install the Astro CLI:

```bash
# Mac
brew install astro

# Windows (PowerShell as administrator)
winget install -e --id Astronomer.Astro
```

Check it works:

```bash
astro version
```

---

## Step 1: Clone the repository

```bash
git clone https://github.com/satyam88/accuweather-etl-airflow.git
cd accuweather-etl-airflow
```

## Step 2: Get your AccuWeather API key

1. Create a free account at [developer.accuweather.com](https://developer.accuweather.com).
2. Copy your API key from the portal.

The free plan allows a limited number of calls per day, so the DAG runs once an hour.

## Step 3: Find the location key of your city

AccuWeather does not use latitude and longitude for current conditions. Every city has a **location key**.
Search by city name:

```bash
# Mac / Linux
curl -H "Authorization: Bearer <your-api-key>" \
  "https://dataservice.accuweather.com/locations/v1/cities/search?q=Manali"
```

```powershell
# Windows PowerShell
Invoke-RestMethod -Headers @{Authorization="Bearer <your-api-key>"} `
  -Uri "https://dataservice.accuweather.com/locations/v1/cities/search?q=Manali" |
  Select-Object Key, LocalizedName, @{n="State";e={$_.AdministrativeArea.LocalizedName}}
```

Several cities can have the same name, so pick the one in the right state:

```
Key      LocalizedName  State
196142   Manali         Tamil Nadu
198332   Manali         Himachal Pradesh     <-- the hill station
2755149  Manali         Maharashtra
2798947  Manali         Tamil Nadu
196143   Manali         Tamil Nadu
```

| City | Location key |
|---|---|
| Manali, Himachal Pradesh | `198332` (default in this project) |
| Pune, Maharashtra | `204848` |

To use another city, change these two lines in `dags/accuweather_etl_pipeline.py`:

```python
LOCATION_KEY = "198332"   # Manali, Himachal Pradesh, India
CITY_NAME = "Manali"
```

## Step 4: Add your API key to the Airflow settings

Copy the template:

```bash
# Mac / Linux
cp airflow_settings.example.yaml airflow_settings.yaml

# Windows PowerShell
Copy-Item airflow_settings.example.yaml airflow_settings.yaml
```

Open `airflow_settings.yaml` and replace `<your-accuweather-api-key>` with your key:

```yaml
    - conn_id: accuweather_api
      conn_type: http
      conn_host: https://dataservice.accuweather.com
      conn_password: <your-accuweather-api-key>   # sent as a Bearer token
```

- `conn_host` is the base URL of the API; the DAG adds only the path.
- `conn_password` stores the API key. Airflow hides passwords in the UI and in task logs.
- `airflow_settings.yaml` is listed in `.gitignore`, so **your key is never pushed to GitHub**.

The same file also creates the `weather_db` connection to the PostgreSQL database.

## Step 5: Start Airflow

```bash
astro dev start
```

Astro builds the image, starts Airflow and the extra `weather-db` database, and loads the connections:

```
Added Connection: weather_db
Added Connection: accuweather_api
➤ Airflow UI: http://localhost:8080
```

`docker-compose.override.yml` adds the database next to the Airflow containers:

```yaml
services:
  weather-db:
    image: postgres:17
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow123
      POSTGRES_DB: airflow_db
    ports:
      - "5433:5432"
    volumes:
      - weather_postgres_data:/var/lib/postgresql/data
    networks:
      - airflow

volumes:
  weather_postgres_data:
```

- `networks: - airflow` puts the database on Astro's Docker network, so Airflow reaches it by the name `weather-db`.
- `5433:5432`: from your laptop use port **5433**; inside Docker (in the Airflow connection) use **5432**.
- The file name must be exactly `docker-compose.override.yml`. A space at the end of the name makes Astro ignore it.

## Step 6: Run the DAG

1. Open the Airflow UI (the URL printed by `astro dev start`, usually http://localhost:8080).
2. Open **accuweather_etl_pipeline**, switch it **ON** and click **Trigger**.
3. All three tasks turn green (**Success**). After that it runs every hour.

Or test it from the command line without the scheduler:

```bash
astro dev run dags test accuweather_etl_pipeline
```

## Step 7: Verify your data

```bash
docker exec -it $(docker ps -qf name=weather-db) psql -U airflow -d airflow_db \
  -c "select city, location_key, observed_at, weather_text, temperature_c, humidity_pct, wind_speed_kmh from accuweather_current_conditions order by observed_at;"
```

```
  city  | location_key |      observed_at       | weather_text | temperature_c | humidity_pct | wind_speed_kmh
--------+--------------+------------------------+--------------+---------------+--------------+----------------
 Manali | 198332       | 2026-11-21 05:52:00+00 | Cloudy       |          12.6 |           20 |            9.8
 Manali | 198332       | 2026-11-21 06:07:00+00 | Cloudy       |          14.1 |           18 |            9.8
```

You get one row for each new AccuWeather observation. You can also connect any SQL client to
`localhost:5433`, database `airflow_db`, user `airflow`, password `airflow123`.

---

## How the DAG works

| Task | What it does |
|---|---|
| `extract_current_conditions` | Reads the API key from the `accuweather_api` connection and calls `GET /currentconditions/v1/{LOCATION_KEY}?details=true` with the header `Authorization: Bearer <key>`. Returns the JSON (pushed to XCom). |
| `transform_current_conditions` | Flattens the nested JSON (for example `Temperature → Metric → Value`) into 15 columns: weather text, temperature, feels-like temperature, humidity, wind speed and direction, pressure, UV index, cloud cover, visibility and more. |
| `load_current_conditions` | Creates `accuweather_current_conditions` if needed and inserts the row. `UNIQUE (location_key, observed_at)` with `ON CONFLICT DO NOTHING` skips an observation that is already stored. |

| Setting | Value | Meaning |
|---|---|---|
| `schedule` | `@hourly` | AccuWeather updates current conditions about once an hour |
| `catchup` | `False` | Do not run missed past intervals |
| `retries` | `2` | Retry a failed task twice (an API can be briefly down) |
| `data={"details": "true"}` | | Ask for full details (humidity, UV, wind, pressure) |

## Run the tests

```bash
astro dev pytest
```

The tests check that the DAG has no import errors, has tags and sets `retries >= 2`.

## Stop Airflow

```bash
astro dev stop      # stop the containers, keep the data
astro dev kill      # remove the containers and the data
```

---

## Troubleshooting

| Problem | Fix |
|---|---|
| `401 Unauthorized` | The API key is wrong or missing. Check `conn_password` in `airflow_settings.yaml` and run `astro dev restart`. |
| `503` or `429` error | The daily call limit of the free plan is used up. Wait until the next day or run the DAG less often. |
| `failed to resolve host 'weather-db'` | The `weather-db` container is not running. Check that the file is named exactly `docker-compose.override.yml`, then run `astro dev restart`. |
| Connection `accuweather_api` not found | `airflow_settings.yaml` is missing (Step 4) or Airflow was not restarted. |
| `port 5433 is already allocated` | Another PostgreSQL container uses the port. Stop it, or change `5433` in `docker-compose.override.yml`. |
| Wrong city in the data | Another city with the same name was used. Search again (Step 3) and check the state. |

---

## Tech stack

Apache Airflow 3 (Astro Runtime 3.4) · Astro CLI · Docker · PostgreSQL 17 · AccuWeather Core Weather API · Python
