import pandas as pd
from src.snowflake_client import get_connection


def fetch_active_parks(conn_params):

    conn = get_connection(conn_params)

    query = """
    SELECT ROLLERNAME
    FROM DIMLOCATION
    WHERE BUSINESSGROUP = 'O&O'
      AND (CLOSEDATE IS NULL OR CLOSEDATE > CURRENT_DATE())
    ORDER BY ROLLERNAME
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print(df.head(10))

    return df["ROLLERNAME"].dropna().tolist()

def get_latest_snowflake_date(conn_params):
    """Get the latest date available in Snowflake FACTREVENUE table"""
    conn = get_connection(conn_params)
    
    query = """
    SELECT MAX(RECORDDATE) as latest_date
    FROM FACTREVENUE
    """
    
    df = pd.read_sql(query, conn)
    conn.close()
    
    if not df.empty:
        # Get the first column value regardless of name
        latest = df.iloc[0, 0]
        if latest is not None:
            return pd.to_datetime(latest).strftime('%Y-%m-%d')
    return None

def load_snowflake_data(conn_params, check_date, parks_list):

    conn = get_connection(conn_params)

    park_string = ",".join([f"'{p}'" for p in parks_list])

    print("Fetching Snowflake data for date:", check_date)
    print("Total parks passed:", len(parks_list))

    query = f"""
    SELECT 
        CAST(fr.recorddate AS DATE) AS DATE,
        dl.rollername AS VENUE,
        SUM(fr.netrevenue) AS SNOWFLAKE_REVENUE
    FROM FACTREVENUE fr
    JOIN DIMLOCATION dl ON fr.sk_location = dl.sk_location
    WHERE dl.rollername IN ({park_string})
        AND CAST(fr.recorddate AS DATE) = TO_DATE('{check_date}')
    GROUP BY CAST(fr.recorddate AS DATE), dl.rollername
    ORDER BY dl.rollername
    """

    df = pd.read_sql(query, conn)
    conn.close()

    print("Rows fetched from Snowflake:", len(df))
    print("Columns:", df.columns.tolist())

    # 🔥🔥 NORMALIZATION (VERY IMPORTANT)
    if not df.empty:
        df["DATE"] = pd.to_datetime(df["DATE"]).dt.date
        df["VENUE"] = (
    df["VENUE"]
    .astype(str)
    .str.upper()
    .str.strip()
    .str.replace(r"\s+", " ", regex=True)
)
    else:
        print("⚠️ WARNING: No data returned from Snowflake")

    return df


