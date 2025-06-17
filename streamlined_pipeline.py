import os
import re
import time
import requests
import json

from bs4 import BeautifulSoup
from urllib.parse import urljoin
from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector
from datetime import datetime

# ------------------ Environment ------------------
load_dotenv("password.env")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
assistant_id = os.getenv("OPENAI_ASSISTANT_ID")

# ------------------ Snowflake Insert ------------------
def insert_into_snowflake(metadata):
    import json
    from datetime import datetime

    # 所需字段及默认值（避免 KeyError）
    default_values = {
        "policy_id": "UNKNOWN",
        "specialty": [],
        "client_interest_flag": [],
        "policy_type": "Unknown",
        "payer": "Unknown",
        "topic_keywords": [],
        "update_type": "Unknown",
        "effective_date": datetime.utcnow().date().isoformat(),
        "jurisdiction": [],
        "urgency_level": 0
    }

    for key, default in default_values.items():
        if key not in metadata:
            metadata[key] = default

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO ENG_RESEARCH.DATA.POLICY_METADATA (
            POLICY_ID, SPECIALTY, CLIENT_INTEREST_FLAG, POLICY_TYPE,
            PAYER, TOPIC_KEYWORDS, UPDATE_TYPE, EFFECTIVE_DATE,
            JURISDICTION, URGENCY_LEVEL, PDF_URL
        )
        SELECT
            %s,
            PARSE_JSON(%s),
            PARSE_JSON(%s),
            %s,
            %s,
            PARSE_JSON(%s),
            %s,
            %s,
            PARSE_JSON(%s),
            %s,
            %s
    """

    try:
        cursor.execute(insert_query, (
            metadata["policy_id"],
            json.dumps(metadata["specialty"]),
            json.dumps(metadata["client_interest_flag"]),
            metadata["policy_type"],
            metadata["payer"],
            json.dumps(metadata["topic_keywords"]),
            metadata["update_type"],
            metadata["effective_date"],
            json.dumps(metadata["jurisdiction"]),
            metadata["urgency_level"],
            metadata["pdf_url"]  # ✅ 新增字段
        ))
        conn.commit()
        print(f"✅ Inserted policy {metadata['policy_id']} into Snowflake.")

    except snowflake.connector.errors.IntegrityError as e:
        if "unique constraint" in str(e).lower():
            print(f"⚠️ Skipped: Policy {metadata['policy_id']} already exists.")
        else:
            print("❌ Snowflake insert error:", e)
    finally:
        cursor.close()
        conn.close()


# ------------------ OpenAI Assistant Call ------------------
def call_assistant_with_bytes(pdf_bytes):
    file = client.files.create(file=("policy.pdf", pdf_bytes, "application/pdf"), purpose="assistants")
    thread = client.beta.threads.create()
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Please extract structured metadata from this policy as JSON.",
        attachments=[{
            "file_id": file.id,
            "tools": [{"type": "code_interpreter"}]
        }]
    )

    # Wait for response
    run = client.beta.threads.runs.create_and_poll(thread_id=thread.id, assistant_id=assistant_id)
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    reply = messages.data[0].content[0].text.value
    return reply

# ------------------ Main Scraper Pipeline ------------------
def run_pipeline():
    base_url = "https://static.cigna.com"
    target_url = urljoin(base_url, "/assets/chcp/resourceLibrary/coveragePolicies/medical_a-z.html")

    response = requests.get(target_url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, "html.parser")
    rows = soup.find_all("tr")

    pdf_links = []
    date_pattern = r"\b\d{2}/\d{2}/\d{4}\b"

    for row in rows:
        row_text = row.get_text()
        dates = re.findall(date_pattern, row_text)
        if dates:
            link_tag = row.find("a", href=True)
            if link_tag and link_tag['href'].endswith(".pdf"):
                full_url = urljoin(base_url, link_tag['href'])
                pdf_links.append(full_url)

    print(f"🔍 Found {len(pdf_links)} PDF links.")

    for pdf_url in pdf_links:
        try:
            print(f"\n📥 Fetching: {pdf_url}")
            pdf_response = requests.get(pdf_url)
            pdf_response.raise_for_status()
            pdf_bytes = pdf_response.content

            raw_reply = call_assistant_with_bytes(pdf_bytes)
            print("🧠 Raw metadata:\n", raw_reply)

            metadata = json.loads(raw_reply)
            metadata["pdf_url"] = pdf_url
            insert_into_snowflake(metadata)

            time.sleep(1)  # Avoid hitting rate limits

        except Exception as e:
            print(f"❌ Failed on {pdf_url}: {e}")

if __name__ == "__main__":
    run_pipeline()
