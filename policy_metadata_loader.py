import os
import time
import json
import glob
from dotenv import load_dotenv
from openai import OpenAI
import snowflake.connector

# ------------------ Environment & Initialization ------------------

load_dotenv("password.env")
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
assistant_id = os.getenv("OPENAI_ASSISTANT_ID")

# ------------------ Call OpenAI Assistant ------------------

def call_assistant_with_pdf(pdf_path):
    # 1. Upload the PDF file
    with open(pdf_path, "rb") as f:
        uploaded_file = client.files.create(file=f, purpose="assistants")

    # 2. Create a new thread
    thread = client.beta.threads.create()

    # 3. Add a user message to the thread
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="Please extract structured metadata from this policy as JSON.",
        attachments=[{
            "file_id": uploaded_file.id,
            "tools": [{"type": "code_interpreter"}]
        }]
    )

    # 4. Run the Assistant
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=assistant_id
    )

    # 5. Wait for completion
    while True:
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run_status.status == "completed":
            break
        elif run_status.status == "failed":
            raise Exception("❌ Assistant run failed.")
        time.sleep(0.5)

    # 6. Retrieve the Assistant's response
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    reply = messages.data[0].content[0].text.value
    return reply

# ------------------ JSON Extraction ------------------

def extract_json(text):
    try:
        match = text[text.index("{"):text.rindex("}")+1]
        return json.loads(match)
    except Exception as e:
        print("⚠️ Failed to parse JSON:", e)
        return None

# ------------------ Insert Into Snowflake ------------------

def insert_into_snowflake(metadata):
    import json
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
            metadata["urgency_level"]
        ))
        conn.commit()
        print(f"✅ Inserted policy {metadata['policy_id']} into Snowflake.")

    except snowflake.connector.errors.IntegrityError as e:
        if "unique constraint" in str(e).lower():
            print(f"⚠️ Skipped: Policy {metadata['policy_id']} already exists.")
        else:
            raise

    finally:
        cursor.close()
        conn.close()

# ------------------ Main Pipeline ------------------

from datetime import datetime

if __name__ == "__main__":
    folder_root = "policies"  # Root folder
    start_date = datetime.strptime("2023-04-15", "%Y-%m-%d")
    end_date = datetime.strptime("2025-05-15", "%Y-%m-%d")

    # List all subdirectories like policies/YYYY-MM-DD/
    subfolders = [
        os.path.join(folder_root, name) for name in os.listdir(folder_root)
        if os.path.isdir(os.path.join(folder_root, name))
    ]

    # Filter subfolders by date range
    valid_folders = []
    for folder in subfolders:
        folder_name = os.path.basename(folder)
        try:
            folder_date = datetime.strptime(folder_name, "%Y-%m-%d")
            if start_date <= folder_date <= end_date:
                valid_folders.append(folder)
        except ValueError:
            continue  # Skip folders that don't match date format

    # Process PDF files in valid folders
    for folder in sorted(valid_folders):
        pdf_files = glob.glob(os.path.join(folder, "*.pdf"))
        for pdf_path in pdf_files:
            print(f"\n📄 Processing: {pdf_path}")
            try:
                raw_reply = call_assistant_with_pdf(pdf_path)
                print("🧠 Assistant response:\n", raw_reply)

                metadata = extract_json(raw_reply)
                if metadata:
                    insert_into_snowflake(metadata)
                else:
                    print("❌ JSON parsing failed. Skipped insertion.")
            except Exception as e:
                print(f"❌ Error while processing {pdf_path}: {e}")