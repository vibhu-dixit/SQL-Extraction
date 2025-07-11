#!/usr/bin/env python3
"""
EAV (Entity-Attribute-Value) Global Union Implementation
Creates a flexible global union table and populates it with actual data from JSON files
"""

import sqlite3
import json
import re
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CONFIG
JSON_FILES_DIR = Path("/content/json-processor/json_files")
DOMAIN_DB_DIR = Path("domain_dbs")
DOMAIN_DB_DIR.mkdir(exist_ok=True)

def sanitize_table_name(name):
    # SQLite table names can't have spaces or special chars
    return re.sub(r'\W|^(?=\d)', '_', name)

def flatten_dict(data):
    """Flattens a nested dictionary (1-level deep for now), skipping timestamp/date keys."""
    items = []
    for k, v in data.items():
        if k in ('timestamp', 'date'):
            continue
        if isinstance(v, dict):
            for sub_k, sub_v in v.items():
                if sub_k in ('timestamp', 'date'):
                    continue
                items.append((f"{k}_{sub_k}", sub_v))
        else:
            items.append((k, v))
    return dict(items)

def is_timestamp(s):
    # Adjust this regex as needed for your timestamp format
    return bool(re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", s))

def process_domain(domain_dir):
    db_path = DOMAIN_DB_DIR / f"{domain_dir.name}.db"
    conn = sqlite3.connect(db_path)
    print(f"Creating DB: {db_path}")

    for json_file in domain_dir.glob("*.json"):
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        entity_name = sanitize_table_name(json_file.stem)
        # Create table for this entity
        create_sql = f'''
            CREATE TABLE IF NOT EXISTS "{entity_name}" (
            timestamp TEXT,
                attribute_name TEXT,
                attribute_value TEXT
            );
        '''
        conn.execute(create_sql)

        # Case 1: Top-level keys are timestamps
        if isinstance(data, dict) and all(is_timestamp(k) for k in data.keys()):
            for ts, attrs in data.items():
                if isinstance(attrs, dict):
                    flat_attrs = flatten_dict(attrs)
                    for attr, value in flat_attrs.items():
                        val = json.dumps(value) if isinstance(value, (dict, list)) else value
                        conn.execute(
                            f'INSERT INTO "{entity_name}" (timestamp, attribute_name, attribute_value) VALUES (?, ?, ?)',
                            (ts, attr, val)
                        )
        else:
            # Case 2: timestamp/date is a field or missing
            timestamp = None
            if isinstance(data, dict):
                if 'timestamp' in data:
                    timestamp = data['timestamp']
                elif 'date' in data:
                    timestamp = data['date']
                flat_data = flatten_dict(data)
            else:
                continue  # skip non-dict JSONs

            for attr, value in flat_data.items():
                val = json.dumps(value) if isinstance(value, (dict, list)) else value
                conn.execute(
                    f'INSERT INTO "{entity_name}" (timestamp, attribute_name, attribute_value) VALUES (?, ?, ?)',
                    (timestamp, attr, val)
                )

    conn.commit()
    conn.close()

def main():
    for domain_dir in JSON_FILES_DIR.iterdir():
        if domain_dir.is_dir():
            process_domain(domain_dir)

if __name__ == "__main__":
    main()