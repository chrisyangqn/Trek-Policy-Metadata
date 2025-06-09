# Policy Metadata Extractor

This script extracts metadata from a **local PDF** using OpenAI’s Assistant API and uploads it to a **Snowflake** table.

## ✅ What It Does

- Uploads a local PDF (e.g., abortion policy)
- Uses LLM to extract metadata as JSON
- Inserts parsed data into `ENG_RESEARCH.DATA.POLICY_METADATA`

## 📌 Example

Tested on:

- `ad_a006_administrativepolicy_abortion.pdf`

## 🛠 In Progress

Working on adding:

- A scraping pipeline to auto-fetch new policies

## 🚀 Usage

1. Add your credentials to `.env`
2. Run:
   ```bash
   python policy_metadata_loader.py
   ```
