#!/usr/bin/env python3
"""
Improved Relational Table Structure Implementation
Creates clean, properly structured SQLite tables with proper data flattening
"""

import sqlite3
import json
import re
from pathlib import Path
from collections import defaultdict, OrderedDict
import logging
from typing import Dict, Any, List, Set

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CONFIG
JSON_FILES_DIR = Path("json_files")
DOMAIN_DB_DIR = Path("domain_dbs")
DOMAIN_DB_DIR.mkdir(exist_ok=True)

def sanitize_column_name(name):
    """Sanitize column names for SQLite compatibility"""
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^\w]', '_', str(name))
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    return sanitized.lower()

def sanitize_table_name(name):
    """Sanitize table names for SQLite compatibility - remove parentheses and special chars"""
    # Remove parentheses and replace with underscore
    cleaned = re.sub(r'[()]', '_', name)
    # Remove other special characters
    cleaned = re.sub(r'[^\w]', '_', cleaned)
    # Remove multiple underscores
    cleaned = re.sub(r'_+', '_', cleaned)
    # Remove leading/trailing underscores
    cleaned = cleaned.strip('_')
    return cleaned

def extract_medal_info(medal_data):
    """
    Extract medal information from JSON array
    Returns structured medal data
    """
    if not medal_data:
        return []
    
    medals = []
    
    # Handle both string and list formats
    if isinstance(medal_data, str):
        try:
            medal_list = json.loads(medal_data)
        except:
            medal_list = [medal_data]
    else:
        medal_list = medal_data
    
    for medal_item in medal_list:
        if isinstance(medal_item, str):
            # Parse medal string like "Gold Medal: 2011 Guadalajara Individual eventing"
            if 'Medal:' in medal_item:
                parts = medal_item.split(':', 1)
                if len(parts) == 2:
                    medal_type = parts[0].strip()
                    details = parts[1].strip()
                    
                    # Extract year from details
                    year_match = re.search(r'(\d{4})', details)
                    year = year_match.group(1) if year_match else None
                    
                    # Extract event type (Individual/Team)
                    event_type = 'Individual'
                    if 'Team' in details:
                        event_type = 'Team'
                    
                    medals.append({
                        'type': medal_type,
                        'year': year,
                        'event_type': event_type,
                        'details': details
                    })
    
    return medals

def flatten_dict_improved(data, parent_key='', sep='_'):
    """
    Improved flattening that handles medal data properly
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        
        if isinstance(v, dict):
            items.extend(flatten_dict_improved(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Special handling for medal data
            if k.lower() in ['medaltemplates', 'medals', 'medal_templates']:
                medals = extract_medal_info(v)
                # Add medal count fields
                items.append((f"{new_key}_gold_count", sum(1 for m in medals if 'Gold' in m.get('type', ''))))
                items.append((f"{new_key}_silver_count", sum(1 for m in medals if 'Silver' in m.get('type', ''))))
                items.append((f"{new_key}_bronze_count", sum(1 for m in medals if 'Bronze' in m.get('type', ''))))
                items.append((f"{new_key}_total_count", len(medals)))
                # Add medal types as JSON for detailed queries
                items.append((f"{new_key}_types", json.dumps([m.get('type') for m in medals])))
                items.append((f"{new_key}_years", json.dumps([m.get('year') for m in medals if m.get('year')])))
                items.append((f"{new_key}_event_types", json.dumps([m.get('event_type') for m in medals])))
            else:
                # Regular list handling
                items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    
    return dict(items)

def is_timestamp(s):
    """Check if string matches timestamp format"""
    return bool(re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", str(s)))

def extract_records_from_json_improved(data):
    """
    Extract individual records from JSON data with improved medal handling
    """
    records = []

    if isinstance(data, dict):
        # Case 1: Top-level keys are timestamps
        if all(is_timestamp(k) for k in data.keys()):
            for timestamp, attrs in data.items():
                if isinstance(attrs, dict):
                    record = flatten_dict_improved(attrs)
                    record['timestamp'] = timestamp
                    records.append(record)
        else:
            # Case 2: Single record
            record = flatten_dict_improved(data)
            records.append(record)

    elif isinstance(data, list):
        # Case 3: List of records
        for item in data:
            if isinstance(item, dict):
                record = flatten_dict_improved(item)
                records.append(record)

    return records

def get_all_columns_from_records(records):
    """
    Get all unique column names from all records
    """
    all_columns = set()
    for record in records:
        all_columns.update(record.keys())
    return sorted(list(all_columns))

def create_table_with_columns(conn, table_name, columns):
    """
    Create table with specified columns
    """
    # Drop table if exists to avoid conflicts
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    # Sanitize column names and create mapping
    column_mapping = {}
    sanitized_columns = []

    for col in columns:
        sanitized = sanitize_column_name(col)
        column_mapping[col] = sanitized
        sanitized_columns.append(sanitized)

    # Create table SQL
    columns_sql = ", ".join([f'"{col}" TEXT' for col in sanitized_columns])
    create_sql = f'''
        CREATE TABLE "{table_name}" (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            {columns_sql}
        )
    '''

    conn.execute(create_sql)
    return column_mapping, sanitized_columns

def insert_records_properly(conn, table_name, records, column_mapping, sanitized_columns):
    """
    Insert records with proper column mapping
    """
    if not records:
        return

    # Create insert statement
    columns_sql = ", ".join([f'"{col}"' for col in sanitized_columns])
    placeholders = ", ".join(["?" for _ in sanitized_columns])
    insert_sql = f'INSERT INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'

    # Insert each record
    for i, record in enumerate(records):
        values = []

        # For each column in order, get the corresponding value from the record
        for original_col in sorted(column_mapping.keys()):
            value = record.get(original_col)

            # Handle different value types
            if value is None:
                values.append(None)
            elif isinstance(value, (dict, list)):
                values.append(json.dumps(value))
            else:
                values.append(str(value))

        try:
            conn.execute(insert_sql, values)
            logger.debug(f"Inserted record {i+1}: {dict(zip(sanitized_columns, values))}")
        except sqlite3.Error as e:
            logger.error(f"Error inserting record {i+1}: {e}")
            logger.error(f"Values: {values}")
            raise

def process_json_file_improved(conn, json_file_path):
    """
    Process a single JSON file and create corresponding table with improved handling
    """
    logger.info(f"Processing: {json_file_path}")

    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error reading {json_file_path}: {e}")
        return

    # Extract records with improved medal handling
    records = extract_records_from_json_improved(data)

    if not records:
        logger.warning(f"No records found in {json_file_path}")
        return

    # Get all columns
    all_columns = get_all_columns_from_records(records)

    # Create clean table name - sanitize the filename
    table_name = sanitize_table_name(json_file_path.stem)

    # Create table
    column_mapping, sanitized_columns = create_table_with_columns(conn, table_name, all_columns)

    # Insert records
    insert_records_properly(conn, table_name, records, column_mapping, sanitized_columns)

    logger.info(f"Created table '{table_name}' with {len(records)} records and {len(all_columns)} columns")

    # Debug: Show first few records
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 3')
    sample_records = cursor.fetchall()
    logger.debug(f"Sample records from {table_name}: {sample_records}")

def process_domain_improved(domain_dir):
    """
    Process all JSON files in a domain directory with improved handling
    """
    db_path = DOMAIN_DB_DIR / f"{domain_dir.name}.db"

    # Remove existing database to start fresh
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    logger.info(f"Creating fresh DB: {db_path}")

    try:
        json_files = list(domain_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files in {domain_dir.name}")

        for json_file in json_files:
            process_json_file_improved(conn, json_file)

        conn.commit()
        logger.info(f"Successfully processed domain: {domain_dir.name}")

    except Exception as e:
        logger.error(f"Error processing domain {domain_dir.name}: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()

def verify_data_integrity_improved():
    """
    Verify that data was inserted correctly with improved structure
    """
    logger.info("Verifying data integrity...")

    for db_file in DOMAIN_DB_DIR.glob("*.db"):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print(f"\nDatabase: {db_file.name}")

        for (table_name,) in tables:
            # Get table schema
            cursor.execute(f"PRAGMA table_info('{table_name}');")
            columns = cursor.fetchall()

            # Get record count
            cursor.execute(f"SELECT COUNT(*) FROM '{table_name}';")
            record_count = cursor.fetchone()[0]

            print(f"  Table: {table_name}")
            print(f"    Records: {record_count}")
            print(f"    Columns: {len(columns)}")

            # Show sample data
            if record_count > 0:
                cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 2')
                sample_data = cursor.fetchall()
                print(f"    Sample data: {sample_data[0] if sample_data else 'None'}")

        conn.close()

def main():
    """
    Main function to process all domains with improved handling
    """
    logger.info("Starting improved JSON to Relational DB conversion...")

    if not JSON_FILES_DIR.exists():
        logger.error(f"JSON files directory does not exist: {JSON_FILES_DIR}")
        return

    # Process each domain directory
    domain_dirs = [d for d in JSON_FILES_DIR.iterdir() if d.is_dir()]
    logger.info(f"Found {len(domain_dirs)} domain directories")

    for domain_dir in domain_dirs:
        logger.info(f"\n{'='*50}")
        logger.info(f"Processing domain: {domain_dir.name}")
        logger.info(f"{'='*50}")
        try:
            process_domain_improved(domain_dir)
        except Exception as e:
            logger.error(f"Failed to process {domain_dir.name}: {e}")
            continue

    # Verify data integrity
    verify_data_integrity_improved()

    logger.info("\nProcessing complete!")

if __name__ == "__main__":
    main() 