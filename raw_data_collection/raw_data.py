import os
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv
import time
import json
from pathlib import Path

load_dotenv()


# ---------- DATE SETUP ----------
def get_yesterday_dates():
    """Return yesterday's start and end date (UTC)."""
    today = datetime.now(timezone.utc).date()
    yesterday = today - timedelta(days=1)
    return str(yesterday), str(yesterday)


# ---------- OPEN-METEO WEATHER DATA ----------
def fetch_weather_data(lat=51.5072, lon=-0.1276):
    start_date, end_date = get_yesterday_dates()
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m,"
        f"cloudcover,shortwave_radiation"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    df_weather = pd.DataFrame(
        {
            "datetime": data["hourly"]["time"],
            "temperature_C": data["hourly"]["temperature_2m"],
            "humidity_%": data["hourly"]["relative_humidity_2m"],
            "wind_speed_mps": data["hourly"]["wind_speed_10m"],
            "cloud_cover_%": data["hourly"]["cloudcover"],
            "solar_radiation_Wm2": data["hourly"]["shortwave_radiation"],
        }
    )
    df_weather["datetime"] = pd.to_datetime(df_weather["datetime"], utc=True)
    return df_weather


# ---------- OPEN-METEO AIR QUALITY DATA ----------
def fetch_air_quality(lat=51.5072, lon=-0.1276):
    start_date, end_date = get_yesterday_dates()
    url = (
        f"https://air-quality-api.open-meteo.com/v1/air-quality?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start_date}&end_date={end_date}"
        f"&hourly=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,"
        f"sulphur_dioxide,ozone,us_aqi"
    )

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    df_aqi = pd.DataFrame(
        {
            "datetime": pd.to_datetime(data["hourly"]["time"], utc=True),
            "pm10": data["hourly"]["pm10"],
            "pm2_5": data["hourly"]["pm2_5"],
            "co": data["hourly"]["carbon_monoxide"],
            "no2": data["hourly"]["nitrogen_dioxide"],
            "so2": data["hourly"]["sulphur_dioxide"],
            "o3": data["hourly"]["ozone"],
            "aqi_us": data["hourly"]["us_aqi"],
        }
    )
    return df_aqi


# ---------- UK CARBON INTENSITY (FETCH LAST 2 DAYS) ----------
def fetch_carbon_intensity():
    """Fetch UK carbon intensity data for yesterday, with API failure safety."""
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    today = datetime.now(timezone.utc).date()

    all_records = []

    urls = [
        f"https://api.carbonintensity.org.uk/intensity/date/{yesterday}",
        f"https://api.carbonintensity.org.uk/intensity/date/{today}",
    ]

    for url in urls:
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json().get("data", [])
            all_records.extend(data)
        except Exception as e:
            print(f"Error fetching data from {url}: {e}")

    # ✅ Return empty DataFrame safely if no data
    if not all_records:
        return pd.DataFrame(
            columns=[
                "datetime",
                "carbon_intensity_actual",
                "carbon_intensity_forecast",
                "carbon_index",
            ]
        )

    # Convert valid records with safe handling for missing/None intensity
    records = []
    for r in all_records:
        if not r.get("from"):
            continue
        intensity = r.get("intensity") or {}
        records.append(
            {
                "datetime": r.get("from"),
                "carbon_intensity_actual": intensity.get("actual"),
                "carbon_intensity_forecast": intensity.get("forecast"),
                "carbon_index": intensity.get("index"),
            }
        )

    df_carbon = pd.DataFrame(records)
    df_carbon["datetime"] = pd.to_datetime(
        df_carbon["datetime"], utc=True, errors="coerce"
    )
    df_carbon = df_carbon[df_carbon["datetime"].dt.date == yesterday]

    return df_carbon.reset_index(drop=True)


# ---------- UK GENERATION MIX ----------
def fetch_carbon_generation_mix():
    url = "https://api.carbonintensity.org.uk/generation"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    timestamp = pd.to_datetime(data["data"]["from"], utc=True)
    gen_mix = data["data"]["generationmix"]

    mix_dict = {"datetime": timestamp}
    for item in gen_mix:
        fuel = item["fuel"].lower().replace(" ", "_")
        # Only include specific fuel types
        if fuel in ["biomass", "imports", "gas", "nuclear", "solar", "wind"]:
            mix_dict[f"uk_gen_{fuel}_%"] = item["perc"]

    return pd.DataFrame([mix_dict])


# ---------- OCTOPUS ENERGY PRICES (FETCH LAST 3 DAYS) ----------
def fetch_octopus_prices():
    products_url = "https://api.octopus.energy/v1/products/"
    response = requests.get(products_url)
    response.raise_for_status()

    products_data = response.json()
    agile_products = [
        p for p in products_data.get("results", []) if "AGILE" in p["code"]
    ]
    if not agile_products:
        raise ValueError("No Agile tariffs found")

    latest_agile = agile_products[0]
    product_code = latest_agile["code"]

    tariff_code = None
    for link in latest_agile.get("links", []):
        if "electricity-tariffs" in link.get("href", ""):
            tariff_code = link["href"].split("/")[-2]
            break
    if not tariff_code:
        tariff_code = f"E-1R-{product_code}-A"

    # Fetch last 3 days to ensure we get all yesterday's prices
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=3)

    period_from = start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    period_to = end_date.strftime("%Y-%m-%dT%H:%M:%SZ")

    rates_url = (
        f"https://api.octopus.energy/v1/products/{product_code}/"
        f"electricity-tariffs/{tariff_code}/standard-unit-rates/"
        f"?period_from={period_from}&period_to={period_to}"
    )

    response = requests.get(rates_url)
    response.raise_for_status()
    data = response.json().get("results", [])

    df_prices = pd.DataFrame(data)
    df_prices["datetime"] = pd.to_datetime(df_prices["valid_from"], utc=True)
    df_prices["retail_price_£_per_kWh"] = df_prices["value_inc_vat"] / 100

    # Filter to only yesterday's data
    yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
    df_prices = df_prices[df_prices["datetime"].dt.date == yesterday]

    return df_prices[["datetime", "retail_price_£_per_kWh"]]


# ---------- MERGE ALL SOURCES ----------
def merge_all_sources(weather_df, aqi_df, carbon_df, carbon_gen_df, prices_df):
    # Start with hourly weather data
    merged = weather_df.copy()

    # Merge hourly data (exact matches)
    merged = merged.merge(aqi_df, on="datetime", how="outer")
    merged = merged.merge(carbon_df, on="datetime", how="outer")

    # Sort before merge_asof
    merged = merged.sort_values("datetime").reset_index(drop=True)
    prices_df = prices_df.sort_values("datetime").reset_index(drop=True)

    # (matches nearest half-hourly price to each hour) 9:30 -> 10:00, 10:30 --> 11:00 etc.
    merged = pd.merge_asof(
        merged,
        prices_df,
        on="datetime",
        direction="nearest",
        tolerance=pd.Timedelta("30min"),  # Allow 30-minute difference
    )

    # Add generation mix (forward fill for all rows)
    for col in carbon_gen_df.columns:
        if col != "datetime":
            merged[col] = carbon_gen_df[col].iloc[0]

    merged = merged.sort_values("datetime").reset_index(drop=True)
    return merged


# ---------- APPEND TO EXISTING FILE ----------
def append_to_historical(new_data, save_dir="data", file_name="raw_data.csv"):
    save_path = os.path.join(save_dir, file_name)
    os.makedirs(save_dir, exist_ok=True)

    if os.path.exists(save_path):
        existing_df = pd.read_csv(save_path)
        existing_df["datetime"] = pd.to_datetime(existing_df["datetime"], utc=True)

        # Filter new data to only yesterday
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        new_data = new_data[new_data["datetime"].dt.date == yesterday]

        # Combine datasets
        combined = pd.concat([existing_df, new_data], ignore_index=True)

        # keeping the most recent data for each datetime
        combined = combined.sort_values(["datetime", "datetime"]).drop_duplicates(
            subset=["datetime"], keep="last"
        )

        combined = combined.sort_values("datetime").reset_index(drop=True)

        combined.to_csv(save_path, index=False)
        print(f"Appended {len(new_data)} records to {save_path}")
        return combined
    else:
        new_data.to_csv(save_path, index=False)
        print(f"Created new file: {save_path}")
        return new_data


# ---------- DAILY COLLECTION ----------
def collect_and_append_yesterday(save_dir="data", file_name="raw_data.csv"):
    try:
        yesterday = datetime.now(timezone.utc).date() - timedelta(days=1)
        print(f"\n{'=' * 55}")
        print(f"Collecting Data for {yesterday}")
        print(f"{'=' * 55}\n")

        print("Fetching weather data...")
        weather_df = fetch_weather_data()
        print(f"   ✓ {len(weather_df)} weather records")

        print("Fetching air quality data...")
        aqi_df = fetch_air_quality()
        print(f"   ✓ {len(aqi_df)} air quality records")

        print("Fetching carbon intensity (last 2 days)...")
        carbon_df = fetch_carbon_intensity()
        print(f"   ✓ {len(carbon_df)} carbon intensity records")

        print("Fetching generation mix...")
        carbon_gen_df = fetch_carbon_generation_mix()
        print("   ✓ Generation mix fetched")

        print("Fetching electricity prices (last 3 days)...")
        prices_df = fetch_octopus_prices()
        print(f"   ✓ {len(prices_df)} price records")

        print("\n🔄 Merging datasets...")
        merged_df = merge_all_sources(
            weather_df, aqi_df, carbon_df, carbon_gen_df, prices_df
        )

        # Keep only full-hour data and ensure datetime format
        merged_df["datetime"] = pd.to_datetime(merged_df["datetime"], utc=True)

        # Filter to only yesterday's data
        yesterday_date = datetime.now(timezone.utc).date() - timedelta(days=1)
        merged_df = merged_df[merged_df["datetime"].dt.date == yesterday_date]

        # Keep only full hours (minute == 0)
        merged_df = merged_df[merged_df["datetime"].dt.minute == 0]
        merged_df = merged_df.sort_values("datetime").reset_index(drop=True)

        print(f"   ✓ {len(merged_df)} hourly records for {yesterday_date}")

        # Check data completeness
        print("\nData Completeness Check:")
        missing_carbon = merged_df["carbon_intensity_actual"].isnull().sum()
        missing_prices = merged_df["retail_price_£_per_kWh"].isnull().sum()
        missing_weather = merged_df["temperature_C"].isnull().sum()

        print(f"   Missing weather: {missing_weather}/{len(merged_df)}")
        print(f"   Missing carbon intensity: {missing_carbon}/{len(merged_df)}")
        print(f"   Missing prices: {missing_prices}/{len(merged_df)}")

        if missing_prices > 0:
            print("\nPrice data missing for hours:")
            missing_price_hours = merged_df[
                merged_df["retail_price_£_per_kWh"].isnull()
            ]["datetime"]
            for dt in missing_price_hours:
                print(f"      {dt}")

        if missing_carbon > 2 or missing_prices > 5:
            print("\nWARNING: High missing data count!")
            print("   This is expected if APIs haven't updated yet.")
            print("   Consider running this script later in the day (after 2 PM UTC).")

        final_df = append_to_historical(merged_df, save_dir, file_name)

        print(f"\nCollection Complete for {yesterday}")
        print(f"Total records in file: {len(final_df)}")
        print(f"Range: {final_df['datetime'].min()} → {final_df['datetime'].max()}")
        print(f"{'=' * 55}\n")

        return final_df

    except Exception as e:
        print(f"Error during daily data collection: {e}")
        raise


if __name__ == "__main__":
    try:
        # Run main collection
        final_df = collect_and_append_yesterday()

        print("Saving monitoring metrics...")

        # Total APIs you call
        total_apis = 5
        success_count = 0
        total_response_time = 0.0

        # Record individual API calls and durations
        api_calls = {
            "weather": "https://archive-api.open-meteo.com/v1/archive",
            "air_quality": "https://air-quality-api.open-meteo.com/v1/air-quality",
            "carbon_intensity": "https://api.carbonintensity.org.uk/intensity/date",
            "generation_mix": "https://api.carbonintensity.org.uk/generation",
            "octopus_prices": "https://api.octopus.energy/v1/products/",
        }

        # test each endpoint
        import requests

        for name, url in api_calls.items():
            try:
                start = time.time()
                resp = requests.head(url, timeout=5)
                resp.raise_for_status()
                duration = round(time.time() - start, 3)
                success_count += 1
                total_response_time += duration
            except Exception:
                duration = None

        # Compute metrics
        api_success_rate = round(success_count / total_apis, 2)
        avg_response_time = (
            round(total_response_time / success_count, 3) if success_count else None
        )

        # Prepare monitoring folder
        Path("monitoring").mkdir(parents=True, exist_ok=True)
        metrics_path = Path("monitoring/metrics.json")

        # Load existing metrics if available
        if metrics_path.exists():
            try:
                with open(metrics_path, "r") as f:
                    metrics = json.load(f)
            except Exception:
                print("⚠️ Failed to load existing metrics.json — starting fresh.")
                metrics = {}
        else:
            metrics = {}

        # Update with new data
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        metrics.update(
            {
                "total_apis": total_apis,
                "last_run": timestamp,
                "api_success_rate": api_success_rate,
                "avg_api_response_time": avg_response_time,
            }
        )

        # Save metrics
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2)

        print(f"Monitoring metrics saved to {metrics_path}")

    except Exception as e:
        print(f"Error in main process: {e}")