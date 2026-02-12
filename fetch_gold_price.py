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

changes_path : str = "price_changes.csv"
if records:
    df = pd.read_csv(csv_path)
    df_sorted_all = df.sort_values(["article","date","time"])
    df_sorted_all["prev_price"] = df_sorted_all.groupby("article")["price"].shift()
    price_changes = df_sorted_all[
        df_sorted_all["prev_price"].notna()
        & (df_sorted_all["price"] != df_sorted_all["prev_price"])
    ][["date","time","article","prev_price","price"]].rename(
        columns={"prev_price":"old_price","price":"new_price"}
    )

    if os.path.exists(changes_path) and os.path.getsize(changes_path) > 0:
        existing_changes = pd.read_csv(changes_path)
        combined_changes = pd.concat([existing_changes,price_changes],ignore_index=True)
        combined_changes = combined_changes.drop_duplicates(
            subset=["date","time","article","old_price","new_price"]
        )
    else:
        combined_changes = price_changes
    combined_changes = combined_changes.sort_values(
        ["date","time","article"], ascending=[False,False,True]
    )
    combined_changes.to_csv(changes_path,index=False)
    

