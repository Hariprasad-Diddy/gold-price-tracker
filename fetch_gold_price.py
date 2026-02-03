# import requests
# import json
# import re
# import os
# import pandas as pd
# from datetime import datetime 

# url = "https://www.grtjewels.com/all-jewellery.html"
# silver = "https://www.grtjewels.com/gifts/gold-coin.html"
# response = requests.get(silver)

# # Prepare data for CSV
# now = datetime.now()
# date_str = now.strftime("%Y-%m-%d")
# time_str = now.strftime("%H:%M:%S")
# records = []

# # Extract SILVER price
# silver_pattern = r'\\"type\\":\\"SILVER\\".*?\\"amount\\":(\d+)'
# silver_match = re.search(silver_pattern, response.text)

# if silver_match:
#     silver_amount = silver_match.group(1)
#     print(f"Silver price per gram: {silver_amount}")
#     records.append({"date": date_str, "time": time_str, "article": "SILVER 1G", "price": silver_amount})
# else:
#     print("Could not find silver amount")

# # Extract GOLD prices with different purity levels
# gold_pattern = r'\\"type\\":\\"GOLD\\".*?\\"purity\\":\\"(\d+ KT)\\".*?\\"amount\\":(\d+)'
# gold_matches = re.findall(gold_pattern, response.text)

# # Remove duplicates by converting to dict (keeps first occurrence of each purity)
# unique_gold = dict(gold_matches)

# if unique_gold:
#     print("\nGold prices per gram:")
#     for purity, amount in unique_gold.items():
#         print(f"  Gold {purity}: {amount}")
#         records.append({"date": date_str, "time": time_str, "article": f"GOLD {purity} 1G", "price": amount})
# if not unique_gold:
#     print("Could not find gold amounts")
# # Save to CSV in the same folder as this script
# if records:
#     df = pd.DataFrame(records)
#     script_dir = os.path.dirname(os.path.abspath(__file__))
#     csv_path = os.path.join(script_dir, "response.csv")
#     # Add header if file doesn't exist or is empty
#     add_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
#     df.to_csv(csv_path, mode='a', header=add_header, index=False)
#     print(f"\nResults saved to {csv_path}")

# # Save to CSV
# #if records:
#  #   df = pd.DataFrame(records)
#   #  csv_path = "/Users/hariprasad.diddy/Documents/workings/airflow_test/response.csv"
#    # df.to_csv(csv_path, mode='a', header=not os.path.exists(csv_path), index=False)
    
import requests
import re
import os
import pandas as pd
from datetime import datetime 

# Target URL
silver_url = "https://www.grtjewels.com/gifts/gold-coin.html"
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.google.com',
}

response = requests.get(silver_url, headers=headers, timeout=15)
if response.status_code != 200:
    print(f"FAILED: Status {response.status_code}. Site might be blocking GitHub IPs.")
    exit(1)

# Prepare data
now = datetime.now()
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
    
    # Use absolute path relative to the workspace for GitHub
    add_header = not os.path.exists(csv_path) or os.path.getsize(csv_path) == 0
    df.to_csv(csv_path, mode='a', header=add_header, index=False)
    print(f"DEBUG: File successfully written to {os.path.abspath(csv_path)}")
else:
    print(response.text)
    print("DEBUG: No records were found. Check your Regex patterns!")
    exit(1)


