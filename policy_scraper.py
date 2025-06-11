import requests
from bs4 import BeautifulSoup
import os
from urllib.parse import urljoin
import re
from datetime import datetime

# Base configuration
base_url = "https://static.cigna.com"
target_url = "https://static.cigna.com/assets/chcp/resourceLibrary/coveragePolicies/medical_a-z.html"
root_save_dir = os.path.join(os.getcwd(), "policies")
os.makedirs(root_save_dir, exist_ok=True)

# Fetch and parse HTML
response = requests.get(target_url)
response.raise_for_status()
soup = BeautifulSoup(response.content, "html.parser")

# Find all rows in the table
rows = soup.find_all("tr")

# Step 1: Extract all MM/DD/YYYY dates
date_pattern = r"\b\d{2}/\d{2}/\d{4}\b"
date_to_links = {}

for row in rows:
    row_text = row.get_text()
    dates = re.findall(date_pattern, row_text)
    if dates:
        link_tag = row.find("a", href=True)
        if link_tag and link_tag['href'].endswith(".pdf"):
            for date in dates:
                # Convert MM/DD/YYYY → YYYY-MM-DD
                try:
                    formatted_date = datetime.strptime(date, "%m/%d/%Y").strftime("%Y-%m-%d")
                except ValueError:
                    continue  # skip invalid dates
                if formatted_date not in date_to_links:
                    date_to_links[formatted_date] = []
                date_to_links[formatted_date].append(link_tag['href'])

# Step 2: For each date, download the corresponding PDFs into the YYYY-MM-DD folder
for formatted_date, links in date_to_links.items():
    save_dir = os.path.join(root_save_dir, formatted_date)
    os.makedirs(save_dir, exist_ok=True)

    for href in links:
        pdf_url = urljoin(base_url, href)
        filename = os.path.basename(href)
        save_path = os.path.join(save_dir, filename)

        if os.path.exists(save_path):
            continue

        print(f"Downloading {filename} for date {formatted_date}...")
        try:
            pdf_response = requests.get(pdf_url)
            pdf_response.raise_for_status()
            with open(save_path, "wb") as f:
                f.write(pdf_response.content)
        except Exception as e:
            print(f"❌ Failed to download {filename}: {e}")

print(f"✅ Finished downloading PDFs for {len(date_to_links)} dates.")
