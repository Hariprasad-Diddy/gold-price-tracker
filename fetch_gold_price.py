import requests
import re
import os
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

# Target URL
silver_url = "https://www.grtjewels.com/gifts/gold-coin.html"

def fetch_page(url: str) -> requests.Response:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": "https://www.grtjewels.com/",
        "Connection": "keep-alive",
    }

    session = requests.Session()
    response = session.get(url, headers=headers, timeout=20)

    if response.status_code in {403, 503}:
        try:
            import cloudscraper  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "Blocked with 403/503. Install cloudscraper to bypass: "
                "pip install cloudscraper"
            ) from exc
        scraper = cloudscraper.create_scraper()
        response = scraper.get(url, headers=headers, timeout=20)

    response.raise_for_status()
    return response

response = fetch_page(silver_url)

# Prepare data
now = datetime.now(ZoneInfo("Asia/Kolkata"))
date_str = now.strftime("%Y-%m-%d")
time_str = now.strftime("%H:%M:%S")
records = []

# Extract SILVER price
silver_pattern = r'\\"type\\":\\"SILVER\\".*?\\"amount\\":(\d+)'
silver_match = re.search(silver_pattern, response.text)
if silver_match:
    records.append({"date": date_str, "time": time_str, "article": "SILVER 1G", "price": silver_match.group(1)})

# Extract GOLD prices
gold_pattern = r'\\"type\\":\\"GOLD\\".*?\\"purity\\":\\"(\d+ KT)\\".*?\\"amount\\":(\d+)'
gold_matches = re.findall(gold_pattern, response.text)
unique_gold = dict(gold_matches)

for purity, amount in unique_gold.items():
    records.append({"date": date_str, "time": time_str, "article": f"GOLD {purity} 1G", "price": amount})

if records:
    df = pd.DataFrame(records)
    csv_path = "response.csv"
    
    if os.path.exists(csv_path) and os.path.getsize(csv_path) > 0:
        existing = pd.read_csv(csv_path)
        combined = pd.concat([existing, df], ignore_index=True)
    else:
        combined = df

    combined = combined.sort_values(["date", "time"], ascending=[False, False])
    combined.to_csv(csv_path, index=False)
    print(f"DEBUG: File successfully written to {os.path.abspath(csv_path)}")
else:
    print(response.text)
    print(response.text.find("GOLD 22 KT/1g"))
    print("DEBUG: No records were found. Check your Regex patterns!")
    exit(1)

changes_path: str = "price_changes.csv"

if records:
    df = pd.read_csv(csv_path)
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["timestamp"] = pd.to_datetime(df["date"] + " " + df["time"])
    df = df.sort_values(["article", "timestamp"])

    out_rows = []

    for article, group in df.groupby("article", sort=True):
        group = group.sort_values("timestamp")
        group["date_only"] = group["timestamp"].dt.date

        min_date = group["date_only"].min()
        max_date = group["date_only"].max()
        if pd.isna(min_date) or pd.isna(max_date):
            continue

        last_price = None

        for day in pd.date_range(min_date, max_date, freq="D").date:
            day_rows = group[group["date_only"] == day].sort_values("timestamp")

            if last_price is None:
                if day_rows.empty:
                    continue
                first = day_rows.iloc[0]
                last_price = first["price"]
                out_rows.append({
                    "date": day.isoformat(),
                    "time": first["timestamp"].strftime("%H:%M:%S"),
                    "article": article,
                    "price": last_price,
                })
                remaining = day_rows.iloc[1:]
            else:
                out_rows.append({
                    "date": day.isoformat(),
                    "time": "00:00:00",
                    "article": article,
                    "price": last_price,
                })
                remaining = day_rows

            for _, row in remaining.iterrows():
                if row["price"] != last_price:
                    last_price = row["price"]
                    out_rows.append({
                        "date": day.isoformat(),
                        "time": row["timestamp"].strftime("%H:%M:%S"),
                        "article": article,
                        "price": last_price,
                    })

    daily_changes = pd.DataFrame(out_rows)

    if os.path.exists(changes_path) and os.path.getsize(changes_path) > 0:
        existing_changes = pd.read_csv(changes_path)
        combined_changes = pd.concat([existing_changes, daily_changes], ignore_index=True)
        combined_changes = combined_changes.drop_duplicates(
            subset=["date", "time", "article", "price"]
        )
    else:
        combined_changes = daily_changes

    combined_changes = combined_changes.sort_values(
        ["date", "time", "article"], ascending=[False, False, True]
    )
    combined_changes.to_csv(changes_path, index=False)
