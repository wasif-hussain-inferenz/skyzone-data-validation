import datetime

import pandas as pd

from src.snowflake_client import get_connection


def fetch_active_parks(conn_params):
    conn = get_connection(conn_params)
    try:
        query = """
        SELECT ROLLERNAME
        FROM DIMLOCATION
        WHERE BUSINESSGROUP = 'O&O'
          AND (CLOSEDATE IS NULL OR CLOSEDATE > CURRENT_DATE())
        ORDER BY ROLLERNAME
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    print(df.head(10))
    return df["ROLLERNAME"].dropna().tolist()


def get_latest_snowflake_date(conn_params):
    """Get the latest date available in Snowflake FACTREVENUE table."""

    conn = get_connection(conn_params)
    try:
        query = """
        SELECT MAX(RECORDDATE) as latest_date
        FROM FACTREVENUE
        """
        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    if not df.empty:
        latest = df.iloc[0, 0]
        if latest is not None:
            return pd.to_datetime(latest).strftime("%Y-%m-%d")

    return None


def load_snowflake_data(conn_params, dates, parks_list):
    conn = get_connection(conn_params)
    try:
        date_series = normalize_dates(dates)
        park_string = ",".join([sql_string_literal(park) for park in parks_list])
        date_expressions = ", ".join(
            [f"TO_DATE('{date_value}')" for date_value in sorted(date_series)]
        )

        print("Fetching Snowflake data for dates:", sorted(date_series))
        print("Total parks passed:", len(parks_list))

        query = f"""
        SELECT
            CAST(fr.recorddate AS DATE) AS DATE,
            dl.rollername AS VENUE,
            SUM(fr.netrevenue) AS SNOWFLAKE_REVENUE
        FROM FACTREVENUE fr
        JOIN DIMLOCATION dl ON fr.sk_location = dl.sk_location
        WHERE dl.rollername IN ({park_string})
            AND CAST(fr.recorddate AS DATE) IN ({date_expressions})
        GROUP BY CAST(fr.recorddate AS DATE), dl.rollername
        ORDER BY CAST(fr.recorddate AS DATE), dl.rollername
        """

        df = pd.read_sql(query, conn)
    finally:
        conn.close()

    print("Rows fetched from Snowflake:", len(df))
    print("Columns:", df.columns.tolist())

    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
        df["VENUE"] = normalize_venue_series(df["VENUE"])
    else:
        print("WARNING: No data returned from Snowflake")

    return df


def normalize_dates(dates):
    if isinstance(dates, (str, datetime.date, pd.Timestamp)):
        dates = [dates]

    date_series = pd.to_datetime(pd.Series(dates), errors="coerce").dt.date
    date_values = date_series.dropna().unique()
    if len(date_values) == 0:
        raise ValueError("No valid Roller dates were provided to load_snowflake_data")

    return date_values


def normalize_venue_series(series):
    return (
        series.astype(str)
        .str.upper()
        .str.strip()
        .str.replace(r"\s+", " ", regex=True)
    )


def sql_string_literal(value):
    return "'" + str(value).replace("'", "''") + "'"
