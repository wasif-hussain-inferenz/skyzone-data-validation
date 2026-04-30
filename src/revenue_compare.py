import pandas as pd


def compare_revenue(roller_df, snowflake_df):

    roller_df["DATE"] = pd.to_datetime(roller_df["DATE"])
    snowflake_df["DATE"] = pd.to_datetime(snowflake_df["DATE"])

    roller_df["ROLLER_REVENUE"] = (
        roller_df["ROLLER_REVENUE"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False)
        .astype(float)
    )

    snowflake_df["SNOWFLAKE_REVENUE"] = pd.to_numeric(
        snowflake_df["SNOWFLAKE_REVENUE"], errors="coerce"
    )

    common = set(roller_df["VENUE"]) & set(snowflake_df["VENUE"])

    print("Roller venues:", len(roller_df["VENUE"].unique()))
    print("Snowflake venues:", len(snowflake_df["VENUE"].unique()))
    print("Matching venues:", len(common))

    merged = pd.merge(
        roller_df,
        snowflake_df,
        on=["DATE", "VENUE"],
        how="outer"
    )

    merged = merged.fillna(0)

    merged["VARIANCE"] = merged["SNOWFLAKE_REVENUE"] - merged["ROLLER_REVENUE"]

    merged["MATCH"] = merged["VARIANCE"].round(2).apply(
        lambda value: "Match" if value == 0 else "Mismatch"
    )

    return merged
