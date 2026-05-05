import time
from datetime import date, datetime, timedelta
from urllib.parse import quote

import psycopg
import requests
from dateutil.relativedelta import relativedelta


API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJoZWxlbmEuYWxjb2xlYUBwcm90b25tYWlsLmNvbSIsImp0aSI6ImExZTMyMTQ1LTdkNDctNGM5OC1iZWIxLTAxYTQwZDU5YjkyOCIsImlzcyI6IkFFTUVUIiwiaWF0IjoxNzc2NzY3NjIxLCJ1c2VySWQiOiJhMWUzMjE0NS03ZDQ3LTRjOTgtYmViMS0wMWE0MGQ1OWI5MjgiLCJyb2xlIjoiIn0.LbqKVMMntsSFuwvwaRsgURon0eR38IZFdvRUTuyFBy4"
CORE_URL = "https://opendata.aemet.es/opendata"

WEATHER_VALUES_SELECTED_STATIONS = (
    "api/valores/climatologicos/diarios/datos/fechaini/{fechaInicio}/"
    "fechafin/{fechaFin}/estacion/{idema}"
)

AEMET_API_SLEEP_SECONDS = 4.0
AEMET_429_BACKOFF_SECONDS = 20
AEMET_MAX_RETRIES = 3
AEMET_DATA_DELAY_DAYS = 4

MIN_GAP_DAYS = 30
GAP_RECHECK_ATTEMPTS = 3
CHUNK_MONTHS = 6
PREHISTORY_EMPTY_WINDOWS_TO_STOP = 3

DB_CONFIG = {
    "host": "192.168.1.200",
    "port": 5432,
    "dbname": "dw",
    "user": "usr_devsa",
    "password": "AWI@postgres#1006",
}


def rate_limited_get(url, headers=None, timeout=30):
    """Wrapper de requests.get con pausas y reintentos ante 429."""
    for attempt in range(1, AEMET_MAX_RETRIES + 1):
        response = requests.get(url, headers=headers, timeout=timeout)
        time.sleep(AEMET_API_SLEEP_SECONDS)

        if response.status_code != 429:
            return response

        if attempt < AEMET_MAX_RETRIES:
            print(
                "AEMET ha devuelto HTTP 429. "
                f"Reintentando en {AEMET_429_BACKOFF_SECONDS} segundos "
                f"(intento {attempt}/{AEMET_MAX_RETRIES})."
            )
            time.sleep(AEMET_429_BACKOFF_SECONDS)

    return response


def fetch_aemet(url, headers):
    """Hace la doble llamada de AEMET y devuelve la lista final de datos."""
    try:
        response = rate_limited_get(url, headers=headers, timeout=30)
        if response.status_code == 429:
            print("AEMET ha devuelto HTTP 429 por exceso de peticiones.")
            return None
        if response.status_code != 200:
            print(f"Respuesta HTTP inesperada en la primera llamada: {response.status_code}")
            return None

        datos_url = response.json().get("datos")
        if not datos_url:
            return None

        response2 = rate_limited_get(datos_url, timeout=30)
        if response2.status_code == 429:
            print("AEMET ha devuelto HTTP 429 por exceso de peticiones.")
            return None
        if response2.status_code != 200:
            print(f"Respuesta HTTP inesperada en la segunda llamada: {response2.status_code}")
            return None

        return response2.json()
    except Exception as exc:
        print(f"Error en fetch: {exc}")
        return None


def parse_date_input(value):
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return datetime.strptime(str(value), "%Y-%m-%d").date()


def format_aemet_datetime(value, end_of_day=False):
    date_value = parse_date_input(value)
    time_suffix = "23:59:59UTC" if end_of_day else "00:00:00UTC"
    return quote(f"{date_value.strftime('%Y-%m-%d')}T{time_suffix}", safe="")


def build_station_weather_url(core_url, start_date, end_date, indicativo):
    endpoint = WEATHER_VALUES_SELECTED_STATIONS.format(
        fechaInicio=format_aemet_datetime(start_date),
        fechaFin=format_aemet_datetime(end_date, end_of_day=True),
        idema=indicativo,
    )
    return f"{core_url.rstrip('/')}/{endpoint.lstrip('/')}"


def clean_decimal(value):
    if value is None:
        return None
    try:
        return float(str(value).replace(",", "."))
    except ValueError:
        return None


def deduplicate_records(records):
    """Deduplica por fecha dentro de una misma estación."""
    unique_by_date = {}
    for record in records:
        record_date = record.get("fecha")
        if record_date and record_date not in unique_by_date:
            unique_by_date[record_date] = record
    return [unique_by_date[key] for key in sorted(unique_by_date)]


def insert_climate_values(records, indicativo, cursor, conn):
    if not records:
        return 0

    inserted = 0
    for record in records:
        try:
            cursor.execute(
                """
                INSERT INTO valores_climatologicos (
                    fecha, indicativo, altitud,
                    tmed, tmax, tmin, horatmax, horatmin,
                    prec, dir, velmedia, racha, horaracha,
                    sol, presmax, presmin, horapresmax, horapresmin,
                    hrmedia, hrmax, hrmin, horahrmax, horahrmin
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                ON CONFLICT (fecha, indicativo) DO UPDATE SET
                """,
                (
                    record.get("fecha"),
                    indicativo,
                    clean_decimal(record.get("altitud")),
                    clean_decimal(record.get("tmed")),
                    clean_decimal(record.get("tmax")),
                    clean_decimal(record.get("tmin")),
                    record.get("horatmax"),
                    record.get("horatmin"),
                    clean_decimal(record.get("prec")),
                    clean_decimal(record.get("dir")),
                    clean_decimal(record.get("velmedia")),
                    clean_decimal(record.get("racha")),
                    record.get("horaracha"),
                    clean_decimal(record.get("sol")),
                    clean_decimal(record.get("presMax")),
                    clean_decimal(record.get("presMin")),
                    record.get("horaPresMax"),
                    record.get("horaPresMin"),
                    clean_decimal(record.get("hrMedia")),
                    clean_decimal(record.get("hrMax")),
                    clean_decimal(record.get("hrMin")),
                    record.get("horaHrMax"),
                    record.get("horaHrMin"),
                ),
            )
            inserted += cursor.rowcount
            conn.commit()
        except Exception as exc:
            conn.rollback()
            print(
                f"Error insertando {indicativo} en fecha {record.get('fecha')}: {exc}"
            )
    return inserted


def get_station_ids(cursor):
    cursor.execute("SELECT indicativo FROM estaciones ORDER BY indicativo")
    return [row[0] for row in cursor.fetchall()]


def get_earliest_loaded_date(cursor, indicativo):
    cursor.execute(
        "SELECT MIN(fecha) FROM valores_climatologicos WHERE indicativo = %s",
        (indicativo,),
    )
    return cursor.fetchone()[0]


def get_large_internal_gaps(cursor, min_gap_days=MIN_GAP_DAYS):
    cursor.execute(
        """
        WITH ordered_dates AS (
            SELECT
                indicativo,
                fecha,
                LAG(fecha) OVER (PARTITION BY indicativo ORDER BY fecha) AS prev_fecha
            FROM valores_climatologicos
        )
        SELECT
            indicativo,
            prev_fecha + 1 AS gap_start,
            fecha - 1 AS gap_end,
            fecha - prev_fecha - 1 AS gap_days
        FROM ordered_dates
        WHERE prev_fecha IS NOT NULL
          AND fecha - prev_fecha - 1 >= %s
        ORDER BY indicativo, gap_start
        """,
        (min_gap_days,),
    )
    return cursor.fetchall()


def split_interval_in_chunks(start_date, end_date, months=CHUNK_MONTHS):
    chunk_start = parse_date_input(start_date)
    chunk_end_limit = parse_date_input(end_date)

    while chunk_start <= chunk_end_limit:
        chunk_end = min(
            chunk_start + relativedelta(months=months) - timedelta(days=1),
            chunk_end_limit,
        )
        yield chunk_start, chunk_end
        chunk_start = chunk_end + timedelta(days=1)


def fetch_interval_multiple_times(
    indicativo,
    start_date,
    end_date,
    core_url,
    api_key,
    attempts=GAP_RECHECK_ATTEMPTS,
):
    headers = {"api_key": api_key}

    for attempt in range(1, attempts + 1):
        url = build_station_weather_url(core_url, start_date, end_date, indicativo)
        data = fetch_aemet(url, headers)

        if data is None:
            print(
                f"  {indicativo} {start_date} a {end_date}: "
                f"intento {attempt}/{attempts} sin respuesta valida."
            )
            continue

        if not isinstance(data, list):
            print(
                f"  {indicativo} {start_date} a {end_date}: "
                f"intento {attempt}/{attempts} devolvio un formato inesperado."
            )
            continue

        if data:
            print(
                f"  {indicativo} {start_date} a {end_date}: "
                f"intento {attempt}/{attempts} encontro {len(data)} registros."
            )
            return deduplicate_records(data)
        else:
            print(
                f"  {indicativo} {start_date} a {end_date}: "
                f"intento {attempt}/{attempts} sin registros."
            )

    return []


def repair_internal_gaps(conn, cursor, core_url, api_key, min_gap_days=MIN_GAP_DAYS):
    gaps = get_large_internal_gaps(cursor, min_gap_days=min_gap_days)
    if not gaps:
        print("No se han encontrado gaps internos de 30 dias o mas.")
        return 0

    total_inserted = 0
    print(f"Se han encontrado {len(gaps)} gaps internos de {min_gap_days}+ dias.")

    for indicativo, gap_start, gap_end, gap_days in gaps:
        print(
            f"Revisando gap de {indicativo}: {gap_start} a {gap_end} "
            f"({gap_days} dias sin datos en base)."
        )

        for chunk_start, chunk_end in split_interval_in_chunks(gap_start, gap_end):
            records = fetch_interval_multiple_times(
                indicativo,
                chunk_start,
                chunk_end,
                core_url,
                api_key,
            )

            if not records:
                continue

            inserted = insert_climate_values(records, indicativo, cursor, conn)
            total_inserted += inserted
            print(
                f"  Tramo {chunk_start} a {chunk_end}: "
                f"{inserted} registros nuevos insertados."
            )

    return total_inserted


def backfill_prehistory_for_station(
    conn,
    cursor,
    indicativo,
    core_url,
    api_key,
    lower_bound_date=None,
):
    earliest_loaded = get_earliest_loaded_date(cursor, indicativo)
    latest_available_date = datetime.utcnow().date() - timedelta(days=AEMET_DATA_DELAY_DAYS)

    current_end = earliest_loaded - timedelta(days=1) if earliest_loaded else latest_available_date
    lower_bound = parse_date_input(lower_bound_date) if lower_bound_date else None
    consecutive_empty_windows = 0
    total_inserted = 0

    print(
        f"Backfill historico para {indicativo} desde "
        f"{current_end} hacia atras."
    )

    while True:
        if lower_bound is not None and current_end < lower_bound:
            break

        current_start = current_end - relativedelta(months=CHUNK_MONTHS) + timedelta(days=1)
        if lower_bound is not None and current_start < lower_bound:
            current_start = lower_bound

        if current_start > current_end:
            break

        records = fetch_interval_multiple_times(
            indicativo,
            current_start,
            current_end,
            core_url,
            api_key,
        )

        if records:
            inserted = insert_climate_values(records, indicativo, cursor, conn)
            total_inserted += inserted
            consecutive_empty_windows = 0
            print(
                f"  Prehistoria {current_start} a {current_end}: "
                f"{inserted} registros nuevos insertados."
            )
        else:
            consecutive_empty_windows += 1
            print(
                f"  Prehistoria {current_start} a {current_end}: "
                f"sin datos tras {GAP_RECHECK_ATTEMPTS} comprobaciones."
            )
            if consecutive_empty_windows >= PREHISTORY_EMPTY_WINDOWS_TO_STOP:
                print(
                    f"  {indicativo}: se detiene el backfill historico tras "
                    f"{PREHISTORY_EMPTY_WINDOWS_TO_STOP} ventanas vacias consecutivas."
                )
                break

        if lower_bound is not None and current_start == lower_bound:
            break

        current_end = current_start - timedelta(days=1)

    return total_inserted


def repair_prehistory(conn, cursor, core_url, api_key, lower_bound_date=None):
    station_ids = get_station_ids(cursor)
    total_inserted = 0

    for index, indicativo in enumerate(station_ids, start=1):
        print(f"[{index}/{len(station_ids)}] Revisando prehistoria de {indicativo}.")
        total_inserted += backfill_prehistory_for_station(
            conn,
            cursor,
            indicativo,
            core_url,
            api_key,
            lower_bound_date=lower_bound_date,
        )

    return total_inserted


def regularize_climate_data(min_gap_days=MIN_GAP_DAYS, lower_bound_date=None):
    conn = psycopg.connect(**DB_CONFIG)

    try:
        cursor = conn.cursor()
        try:
            internal_gap_inserts = repair_internal_gaps(
                conn,
                cursor,
                CORE_URL,
                API_KEY,
                min_gap_days=min_gap_days,
            )

            prehistory_inserts = repair_prehistory(
                conn,
                cursor,
                CORE_URL,
                API_KEY,
                lower_bound_date=lower_bound_date,
            )

            result = {
                "internal_gap_inserts": internal_gap_inserts,
                "prehistory_inserts": prehistory_inserts,
                "total_inserts": internal_gap_inserts + prehistory_inserts,
            }
            print(result)
            return result
        finally:
            cursor.close()
    finally:
        conn.close()


if __name__ == "__main__":
    regularize_climate_data()
