import pandas as pd

def compare_revenue(roller_df, snowflake_df, threshold=0.005):
    # Ensure dates are datetime
    roller_df["DATE"] = pd.to_datetime(roller_df["DATE"])
    snowflake_df["DATE"] = pd.to_datetime(snowflake_df["DATE"])

    # Clean Roller revenue
    roller_df["ROLLER_REVENUE"] = (
        roller_df["ROLLER_REVENUE"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    # Convert Snowflake revenue
    snowflake_df["SNOWFLAKE_REVENUE"] = pd.to_numeric(
        snowflake_df["SNOWFLAKE_REVENUE"], errors="coerce"
    )

    # Common venues
    common = set(roller_df["VENUE"]) & set(snowflake_df["VENUE"])
    print("Roller venues:", len(roller_df["VENUE"].unique()))
    print("Snowflake venues:", len(snowflake_df["VENUE"].unique()))
    print("Matching venues:", len(common))

    # Merge data
    merged = pd.merge(
        roller_df,
        snowflake_df,
        on=["DATE", "VENUE"],
        how="outer"
    ).fillna(0)

    # Compute absolute variance and round to avoid floating point issues
    merged["VARIANCE"] = (abs(merged["SNOWFLAKE_REVENUE"] - merged["ROLLER_REVENUE"])).round(3)

    # Flag matches vs mismatches
    merged["MATCH"] = merged["VARIANCE"].apply(
        lambda value: "Match" if value <= threshold else "Mismatch"
    )

    return merged