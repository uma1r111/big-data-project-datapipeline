import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
import warnings
import os

warnings.filterwarnings("ignore")

RAW_CSV = "data/raw_data.csv"
FE_CSV = "data/engineered_data.csv"


def load_raw_data(raw_csv: str = RAW_CSV) -> pd.DataFrame:
    """Load and sort raw energy data."""
    raw_df = pd.read_csv(raw_csv)
    raw_df["datetime"] = pd.to_datetime(raw_df["datetime"])
    raw_df.sort_values("datetime", inplace=True)
    return raw_df


def load_previous_data(fe_csv: str = FE_CSV) -> pd.DataFrame:
    """Load previously feature-engineered data, if it exists."""
    if os.path.exists(fe_csv):
        prev_df = pd.read_csv(fe_csv)
        prev_df["datetime"] = pd.to_datetime(prev_df["datetime"])
        prev_df.sort_values("datetime", inplace=True)
        return prev_df
    return pd.DataFrame()


def create_temporal_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add temporal time-based features."""
    df["hour"] = df["datetime"].dt.hour
    df["day_of_week"] = df["datetime"].dt.dayofweek
    df["month"] = df["datetime"].dt.month
    df["day_of_month"] = df["datetime"].dt.day
    df["week_of_year"] = df["datetime"].dt.isocalendar().week
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["is_peak_hour"] = df["hour"].between(17, 21).astype(int)
    df["is_night"] = (df["hour"].between(23, 23) | df["hour"].between(0, 6)).astype(int)

    # Cyclical encoding
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["month_sin"] = np.sin(2 * np.pi * df["month"] / 12)
    df["month_cos"] = np.cos(2 * np.pi * df["month"] / 12)
    return df


def create_interaction_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add interaction and derived environmental features."""
    df["renewable_pct"] = df["uk_gen_wind_%"] + df["uk_gen_solar_%"]
    df["fossil_pct"] = df["uk_gen_gas_%"]
    df["heating_demand"] = (18 - df["temperature_C"]).clip(lower=0)
    df["cooling_demand"] = (df["temperature_C"] - 22).clip(lower=0)
    df["wind_solar_combined"] = df["uk_gen_wind_%"] * df["solar_radiation_Wm2"]
    df["carbon_per_price"] = df["carbon_intensity_actual"] / (
        df["retail_price_£_per_kWh"] + 1e-6
    )
    return df


def apply_log_transforms(df: pd.DataFrame) -> pd.DataFrame:
    """Apply log1p to pollutant and intensity features."""
    log_features = [
        "so2",
        "pm2_5",
        "co",
        "no2",
        "pm10",
        "solar_radiation_Wm2",
        "uk_gen_solar_%",
        "aqi_us",
    ]
    for col in log_features:
        if col in df.columns:
            df[f"log_{col}"] = np.log1p(df[col].fillna(0))
    return df


def scale_features(df: pd.DataFrame) -> pd.DataFrame:
    """Scale numeric features using StandardScaler."""
    scale_features = [
        "temperature_C",
        "wind_speed_mps",
        "humidity_%",
        "carbon_intensity_actual",
        "uk_gen_wind_%",
        "uk_gen_gas_%",
    ]
    scale_features = [c for c in scale_features if c in df.columns]
    scaler = StandardScaler()
    scaled = scaler.fit_transform(df[scale_features].fillna(0))
    scaled_df = pd.DataFrame(scaled, columns=[f"scaled_{c}" for c in scale_features])
    return pd.concat(
        [df.reset_index(drop=True), scaled_df.reset_index(drop=True)], axis=1
    )


def run_feature_engineering():
    print("🔹 Loading raw data...")
    raw_df = load_raw_data()
    print(f"Raw data shape: {raw_df.shape}")

    prev_df = load_previous_data()
    if not prev_df.empty:
        new_df = raw_df[~raw_df["datetime"].isin(prev_df["datetime"])].copy()
        print(f"New rows to process: {len(new_df)}")
    else:
        new_df = raw_df.copy()
        print("Processing full dataset...")

    if new_df.empty:
        print("✅ No new data to process.")
        return

    # Sequentially apply transformations
    new_df = create_temporal_features(new_df)
    new_df = create_interaction_features(new_df)
    new_df = apply_log_transforms(new_df)
    new_df = scale_features(new_df)

    final_df = (
        pd.concat([prev_df, new_df])
        .drop_duplicates(subset="datetime")
        .sort_values("datetime")
    )

    os.makedirs(os.path.dirname(FE_CSV), exist_ok=True)
    final_df.to_csv(FE_CSV, index=False)
    print(f"✅ Saved processed data → {FE_CSV}")
    return final_df


if __name__ == "__main__":
    run_feature_engineering()