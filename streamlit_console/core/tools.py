import pandas as pd
import streamlit as st


def apply_date_filter(df: pd.DataFrame, date_col: str | None):
    if not date_col or date_col not in df.columns:
        return df, None

    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True).dt.tz_localize(None)
    df = df[df[date_col].notna()]

    if df.empty:
        return df, None

    min_date = df[date_col].min().date()
    max_date = df[date_col].max().date()

    period = st.date_input(
        "Choose period",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

    if len(period) == 2:
        start_date, end_date = period
        df = df[
            (df[date_col] >= pd.to_datetime(start_date)) &
            (df[date_col] <= pd.to_datetime(end_date))
        ]

    return df, period