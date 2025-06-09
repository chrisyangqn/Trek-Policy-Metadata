import os
import time
import json
from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector

# Load environment variables
load_dotenv("password.env")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
assistant_id = os.getenv("OPENAI_ASSISTANT_ID")

def call_assistant_with_pdf(pdf_path):
    """Upload a PDF and call the assistant to extract structured metadata."""

    with open(pdf_path, "rb") as f:
        uploaded_file = client.files.create(file=f, purpose="assistants")

    thread = client.beta.threads.create()

    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Please extract structured metadata from this policy as JSON.",
        attachments=[{
            "file_id": uploaded_file.id,
            "tools": [{"type": "code_interpreter"}]
        }]
    )

    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id
    )

    while True:
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run_status.status == "completed":
            break
        elif run_status.status == "failed":
            raise Exception("Assistant run failed")
        time.sleep(2)

    messages = client.beta.threads.messages.list(thread_id=thread.id)
    return messages.data[0].content[0].text.value

def extract_json(text_response):
    """Extract and parse JSON from assistant text response."""
    match = text_response[text_response.index("{"):text_response.rindex("}") + 1]
    return json.loads(match)

def insert_into_snowflake(metadata):
    """Insert metadata into Snowflake as parsed JSON arrays."""
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
            JURISDICTION, URGENCY_LEVEL
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
            %s
    """

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
        metadata["urgency_level"]
    ))

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    pdf_path = "ad_a006_administrativepolicy_abortion.pdf"
    response = call_assistant_with_pdf(pdf_path)
    metadata = extract_json(response)
    insert_into_snowflake(metadata)
    print("✅ Metadata successfully inserted into Snowflake.")