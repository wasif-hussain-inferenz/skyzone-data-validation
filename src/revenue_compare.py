import pandas as pd


def compare_revenue(roller_df, snowflake_df, threshold=0.005):
    roller_df = roller_df.copy()
    snowflake_df = snowflake_df.copy()

    roller_df["DATE"] = pd.to_datetime(roller_df["DATE"])
    snowflake_df["DATE"] = pd.to_datetime(snowflake_df["DATE"])

    roller_df["ROLLER_REVENUE"] = clean_money_series(roller_df["ROLLER_REVENUE"])
    snowflake_df["SNOWFLAKE_REVENUE"] = clean_money_series(
        snowflake_df["SNOWFLAKE_REVENUE"]
    )

    common = set(roller_df["VENUE"]) & set(snowflake_df["VENUE"])
    print("Roller venues:", len(roller_df["VENUE"].unique()))
    print("Snowflake venues:", len(snowflake_df["VENUE"].unique()))
    print("Matching venues:", len(common))

    merged = pd.merge(
        roller_df,
        snowflake_df,
        on=["DATE", "VENUE"],
        how="outer",
    ).fillna(0)

    merged["VARIANCE"] = (
        abs(merged["SNOWFLAKE_REVENUE"] - merged["ROLLER_REVENUE"])
    ).round(3)

    merged["MATCH"] = merged["VARIANCE"].apply(
        lambda value: "Match" if value <= threshold else "Mismatch"
    )

    return merged


def clean_money_series(series):
    return pd.to_numeric(
        series.astype(str)
        .str.replace("$", "", regex=False)
        .str.replace(",", "", regex=False),
        errors="coerce",
    ).fillna(0)
