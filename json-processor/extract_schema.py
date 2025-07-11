import os, json, re
from pathlib import Path
from dotenv import load_dotenv
import google.generativeai as genai

load_dotenv()
client = genai.Client(api_key=os.getenv("GOOGLE_API_KEY"))

MODEL = "gemini-2.0-flash"
PROMPT_TMPL = """You are a data architect…
INPUT_JSON = {json_blob}
Task: Return CREATE TABLE …
Output: pure SQL, no commentary."""

schemas_dir = Path("schemas")
schemas_dir.mkdir(exist_ok=True)

for fp in Path("json_files").rglob("*.json"):
    print(fp)
    prompt = PROMPT_TMPL.format(json_blob=fp.read_text())
    raw = client.models.generate_content(model=MODEL, contents=prompt).text
    ddl = re.sub(r'^```.*\n', '', raw)
    ddl = re.sub(r'\n```.*$', '', ddl).strip()

    domain = fp.parent.name
    (schemas_dir / domain).mkdir(exist_ok=True)
    (schemas_dir / domain / f"{fp.stem}.sql").write_text(ddl)

