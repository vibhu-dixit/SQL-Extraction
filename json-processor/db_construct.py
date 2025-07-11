#!/usr/bin/env python3
"""
Fixed Relational Table Structure Implementation
Creates traditional relational tables where each attribute becomes a column
with proper data insertion and column mapping
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
JSON_FILES_DIR = Path("/content/json-processor/json_files")
DOMAIN_DB_DIR = Path("domain_dbs")
DOMAIN_DB_DIR.mkdir(exist_ok=True)

def sanitize_column_name(name):
    """Sanitize column names for SQLite compatibility - keep original names mostly intact"""
    # Only replace problematic characters, keep original structure
    sanitized = re.sub(r'[^\w]', '_', str(name))
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    return sanitized

def sanitize_table_name(name):
    """Sanitize table names for SQLite compatibility"""
    return re.sub(r'\W|^(?=\d)', '_', name)

def flatten_dict(data, parent_key='', sep='_'):
    """
    Flatten nested dictionary into a single level
    """
    items = []
    for k, v in data.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            # Convert lists to JSON strings
            items.append((new_key, json.dumps(v)))
        else:
            items.append((new_key, v))
    return dict(items)

def is_timestamp(s):
    """Check if string matches timestamp format"""
    return bool(re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", str(s)))

def extract_records_from_json(data):
    """
    Extract individual records from JSON data
    Returns list of dictionaries, each representing one record
    """
    records = []

    if isinstance(data, dict):
        # Case 1: Top-level keys are timestamps
        if all(is_timestamp(k) for k in data.keys()):
            for timestamp, attrs in data.items():
                if isinstance(attrs, dict):
                    record = flatten_dict(attrs)
                    record['timestamp'] = timestamp
                    records.append(record)
        else:
            # Case 2: Single record
            record = flatten_dict(data)
            records.append(record)

    elif isinstance(data, list):
        # Case 3: List of records
        for item in data:
            if isinstance(item, dict):
                record = flatten_dict(item)
                records.append(record)

    return records

def get_all_columns_from_domain(domain_dir):
    """
    Get all unique column names from ALL JSON files in a domain
    This ensures consistent table schema across all records
    """
    all_columns = set()
    
    json_files = list(domain_dir.glob("*.json"))
    logger.info(f"Scanning {len(json_files)} JSON files for column names in {domain_dir.name}")
    
    for json_file in json_files:
        try:
            with open(json_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # Extract records from this file
            records = extract_records_from_json(data)
            
            # Add all column names from this file
            for record in records:
                all_columns.update(record.keys())
                
        except Exception as e:
            logger.error(f"Error reading {json_file}: {e}")
            continue
    
    return sorted(list(all_columns))

def create_table_with_columns(conn, table_name, columns):
    """
    Create table with specified columns
    """
    # Drop table if exists to avoid conflicts
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')

    # Sanitize column names and create mapping - handle duplicates
    column_mapping = {}
    sanitized_columns = []
    existing_names = set()

    for col in columns:
        sanitized = sanitize_column_name(col)
        # Handle duplicates by appending counter
        counter = 1
        original_sanitized = sanitized
        while sanitized in existing_names:
            sanitized = f"{original_sanitized}_{counter}"
            counter += 1
        
        existing_names.add(sanitized)
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
    Insert records with NULL for missing columns
    """
    if not records:
        return

    # Create insert statement with ALL columns
    columns_sql = ", ".join([f'"{col}"' for col in sanitized_columns])
    placeholders = ", ".join(["?" for _ in sanitized_columns])
    insert_sql = f'INSERT INTO "{table_name}" ({columns_sql}) VALUES ({placeholders})'

    # Reverse mapping: sanitized_col -> original_col
    reverse_mapping = {v: k for k, v in column_mapping.items()}

    # Insert each record
    for i, record in enumerate(records):
        values = []
        # For each column in table order, get the corresponding value from the record
        for sanitized_col in sanitized_columns:
            original_col = reverse_mapping[sanitized_col]
            value = record.get(original_col)
            # Handle different value types
            if value is None:
                values.append(None)  # NULL for missing columns
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

def process_json_file(conn, json_file_path, all_domain_columns):
    """
    Process a single JSON file and create corresponding table
    """
    logger.info(f"Processing: {json_file_path}")

    try:
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error reading {json_file_path}: {e}")
        return

    # Extract records
    records = extract_records_from_json(data)

    if not records:
        logger.warning(f"No records found in {json_file_path}")
        return

    # Create table name - use exact JSON filename without processing
    table_name = json_file_path.stem

    # Create table with ALL domain columns (not just this file's columns)
    column_mapping, sanitized_columns = create_table_with_columns(conn, table_name, all_domain_columns)

    # Insert records
    insert_records_properly(conn, table_name, records, column_mapping, sanitized_columns)

    logger.info(f"Created table '{table_name}' with {len(records)} records and {len(all_domain_columns)} columns")

    # Debug: Show first few records
    cursor = conn.cursor()
    cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 3')
    sample_records = cursor.fetchall()
    logger.debug(f"Sample records from {table_name}: {sample_records}")

def process_domain(domain_dir):
    """
    Process all JSON files in a domain directory
    """
    db_path = DOMAIN_DB_DIR / f"{domain_dir.name}.db"

    # Remove existing database to start fresh
    if db_path.exists():
        db_path.unlink()

    conn = sqlite3.connect(db_path)
    logger.info(f"Creating fresh DB: {db_path}")

    try:
        # First, get ALL possible columns from ALL files in this domain
        all_domain_columns = get_all_columns_from_domain(domain_dir)
        logger.info(f"Found {len(all_domain_columns)} unique columns across all files in {domain_dir.name}")
        
        if not all_domain_columns:
            logger.error(f"No columns found in domain {domain_dir.name}")
            return
        
        json_files = list(domain_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files in {domain_dir.name}")

        successful_files = 0
        for json_file in json_files:
            try:
                process_json_file(conn, json_file, all_domain_columns)
                successful_files += 1
            except Exception as e:
                logger.error(f"Failed to process {json_file.name}: {e}")
                continue

        conn.commit()
        logger.info(f"Successfully processed domain: {domain_dir.name} ({successful_files}/{len(json_files)} files)")

    except Exception as e:
        logger.error(f"Error processing domain {domain_dir.name}: {e}")
        conn.rollback()
        raise

    finally:
        conn.close()

def verify_data_integrity():
    """
    Verify that data was inserted correctly
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
    Main function to process all domains
    """
    logger.info("Starting JSON to Relational DB conversion...")

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
            process_domain(domain_dir)
        except Exception as e:
            logger.error(f"Failed to process {domain_dir.name}: {e}")
            continue

    # Verify data integrity
    verify_data_integrity()

    logger.info("\nProcessing complete!")

if __name__ == "__main__":
    main()