#!/usr/bin/env python3
"""
Universal Relational Table Structure Implementation
Creates clean, properly structured SQLite tables for ALL domains
Handles different data structures across cricket, country, economy, equesterian, etc.
Preserves original JSON file names exactly as they are
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

# SQLite limits
MAX_COLUMNS = 100  # Much more conservative limit
MAX_DEPTH = 2  # Very shallow flattening
MAX_LIST_ITEMS = 3  # Only process first 3 items in lists

def sanitize_column_name(name):
    """Sanitize column names for SQLite compatibility"""
    # Replace spaces and special chars with underscores
    sanitized = re.sub(r'[^\w]', '_', str(name))
    # Ensure it doesn't start with a number
    if sanitized and sanitized[0].isdigit():
        sanitized = f"col_{sanitized}"
    return sanitized.lower()

def sanitize_table_name(name):
    """Sanitize table names for SQLite compatibility - preserve original names but handle special chars"""
    # Keep original name but escape special characters for SQLite
    # Remove .json extension if present
    if name.endswith('.json'):
        name = name[:-5]
    return name

def extract_medal_info(medal_data):
    """Extract structured medal information from various formats"""
    if isinstance(medal_data, str):
        # Handle string format like "Gold Medal: 2012 London Team jumping"
        medal_info = {
            'medal_type': None,
            'year': None,
            'location': None,
            'event_type': None,
            'competition': None
        }
        
        # Extract medal type
        if 'gold' in medal_data.lower():
            medal_info['medal_type'] = 'gold'
        elif 'silver' in medal_data.lower():
            medal_info['medal_type'] = 'silver'
        elif 'bronze' in medal_data.lower():
            medal_info['medal_type'] = 'bronze'
        
        # Extract year (4-digit number)
        year_match = re.search(r'\b(19|20)\d{2}\b', medal_data)
        if year_match:
            medal_info['year'] = year_match.group()
        
        # Extract location (capitalized words)
        location_match = re.search(r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\b', medal_data)
        if location_match:
            medal_info['location'] = location_match.group()
        
        # Extract event type
        if 'team' in medal_data.lower():
            medal_info['event_type'] = 'team'
        elif 'individual' in medal_data.lower():
            medal_info['event_type'] = 'individual'
        
        # Extract competition type
        if 'olympic' in medal_data.lower():
            medal_info['competition'] = 'olympic'
        elif 'world' in medal_data.lower():
            medal_info['competition'] = 'world'
        elif 'european' in medal_data.lower():
            medal_info['competition'] = 'european'
        elif 'pan american' in medal_data.lower():
            medal_info['competition'] = 'pan_american'
        
        return medal_info
    elif isinstance(medal_data, dict):
        return medal_data
    else:
        return {'medal_type': str(medal_data), 'year': None, 'location': None, 'event_type': None, 'competition': None}

def flatten_dict_aggressive(data, parent_key='', sep='_', domain_type='unknown', current_depth=0):
    """Aggressive flattening that limits columns severely"""
    items = []
    
    if current_depth >= MAX_DEPTH:
        # If we've gone too deep, just convert to string
        return [(parent_key, json.dumps(data) if isinstance(data, (dict, list)) else str(data))]
    
    if isinstance(data, dict):
        # Only process top-level keys and a few important nested ones
        for k, v in data.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            # Only process important keys or shallow nesting
            if current_depth == 0 or k in ['name', 'title', 'description', 'type', 'value', 'medals', 'medal', 'sport', 'discipline']:
                if isinstance(v, dict):
                    items.extend(flatten_dict_aggressive(v, new_key, sep, domain_type, current_depth + 1))
                elif isinstance(v, list):
                    # Only process first few items
                    for i, item in enumerate(v[:MAX_LIST_ITEMS]):
                        if isinstance(item, dict):
                            items.extend(flatten_dict_aggressive(item, f"{new_key}_{i}", sep, domain_type, current_depth + 1))
                        else:
                            items.append((f"{new_key}_{i}", item))
                else:
                    items.append((new_key, v))
            else:
                # For other keys, just convert to string
                items.append((new_key, json.dumps(v) if isinstance(v, (dict, list)) else str(v)))
    
    elif isinstance(data, list):
        # Only process first few items
        for i, item in enumerate(data[:MAX_LIST_ITEMS]):
            if isinstance(item, dict):
                items.extend(flatten_dict_aggressive(item, f"{parent_key}_{i}", sep, domain_type, current_depth + 1))
            else:
                items.append((f"{parent_key}_{i}", item))
    
    else:
        items.append((parent_key, data))
    
    return items

def detect_domain_type(domain_name):
    """Detect the type of domain based on directory name"""
    domain_lower = domain_name.lower()
    
    if 'cricket' in domain_lower:
        return 'cricket'
    elif 'country' in domain_lower:
        return 'country'
    elif 'economy' in domain_lower:
        return 'economy'
    elif 'equesterian' in domain_lower or 'equestrian' in domain_lower:
        return 'equestrian'
    elif 'cyclist' in domain_lower:
        return 'cyclist'
    elif 'golfer' in domain_lower:
        return 'golfer'
    elif 'field_hockey' in domain_lower:
        return 'field_hockey'
    elif 'gov_agencies' in domain_lower:
        return 'gov_agencies'
    elif 'table_tennis' in domain_lower:
        return 'table_tennis'
    else:
        return 'unknown'

def extract_records_from_json_universal(data, domain_type='unknown'):
    """Extract records from JSON data with aggressive flattening"""
    if isinstance(data, dict):
        # Single record
        flattened = dict(flatten_dict_aggressive(data, domain_type=domain_type))
        return [flattened]
    elif isinstance(data, list):
        # Multiple records
        records = []
        for item in data[:10]:  # Limit to first 10 records
            if isinstance(item, dict):
                flattened = dict(flatten_dict_aggressive(item, domain_type=domain_type))
                records.append(flattened)
        return records
    else:
        return [{'data': str(data)}]

def get_all_columns_from_records(records):
    """Get all unique column names from records"""
    columns = set()
    for record in records:
        columns.update(record.keys())
    return sorted(list(columns))

def create_table_with_columns(conn, table_name, columns):
    """Create table with proper column types"""
    # Limit number of columns to avoid SQLite issues
    if len(columns) > MAX_COLUMNS:
        logger.warning(f"Table {table_name} has {len(columns)} columns, limiting to {MAX_COLUMNS}")
        columns = columns[:MAX_COLUMNS]
    
    # Create column definitions
    column_defs = []
    for col in columns:
        col_name = sanitize_column_name(col)
        # Default to TEXT for flexibility
        col_type = 'TEXT'
        column_defs.append(f'"{col_name}" {col_type}')
    
    # Create table
    create_sql = f'''
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {', '.join(column_defs)}
    )
    '''
    
    try:
        conn.execute(create_sql)
        conn.commit()
        logger.info(f"Created table: {table_name}")
    except sqlite3.Error as e:
        logger.error(f"Error creating table {table_name}: {e}")
        raise

def insert_records_properly(conn, table_name, records, column_mapping, sanitized_columns):
    """Insert records with proper handling of data types"""
    if not records:
        return
    
    # Limit columns to avoid issues
    if len(sanitized_columns) > MAX_COLUMNS:
        sanitized_columns = sanitized_columns[:MAX_COLUMNS]
        # Update column mapping accordingly
        column_mapping = {k: v for k, v in column_mapping.items() if v in sanitized_columns}
    
    # Prepare insert statement
    placeholders = ', '.join(['?' for _ in sanitized_columns])
    columns_str = ', '.join([f'"{col}"' for col in sanitized_columns])
    insert_sql = f'INSERT INTO "{table_name}" ({columns_str}) VALUES ({placeholders})'
    
    # Prepare data for insertion
    insert_data = []
    for record in records:
        row_data = []
        for col in sanitized_columns:
            value = record.get(column_mapping.get(col, col), None)
            
            # Handle different data types
            if isinstance(value, (list, dict)):
                value = json.dumps(value)
            elif value is None:
                value = None
            else:
                value = str(value)
            
            row_data.append(value)
        insert_data.append(row_data)
    
    # Insert data
    try:
        conn.executemany(insert_sql, insert_data)
        conn.commit()
        logger.info(f"Inserted {len(records)} records into {table_name}")
    except sqlite3.Error as e:
        logger.error(f"Error inserting into {table_name}: {e}")
        raise

def process_json_file_universal(conn, json_file_path, domain_type):
    """Process a single JSON file with universal handling"""
    try:
        with open(json_file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Extract table name from file name (preserve original name)
        table_name = sanitize_table_name(json_file_path.stem)
        
        # Extract records
        records = extract_records_from_json_universal(data, domain_type)
        
        if not records:
            logger.warning(f"No records extracted from {json_file_path}")
            return
        
        # Get all columns and remove duplicates
        all_columns = get_all_columns_from_records(records)
        
        # Remove duplicate columns (keep first occurrence)
        unique_columns = []
        seen_columns = set()
        for col in all_columns:
            sanitized_col = sanitize_column_name(col)
            if sanitized_col not in seen_columns:
                unique_columns.append(col)
                seen_columns.add(sanitized_col)
        
        # Create column mapping (original -> sanitized)
        column_mapping = {col: sanitize_column_name(col) for col in unique_columns}
        sanitized_columns = [column_mapping[col] for col in unique_columns]
        
        # Create table
        create_table_with_columns(conn, table_name, sanitized_columns)
        
        # Insert records
        insert_records_properly(conn, table_name, records, column_mapping, sanitized_columns)
        
        logger.info(f"Successfully processed {json_file_path} -> {table_name}")
        
    except Exception as e:
        logger.error(f"Error processing {json_file_path}: {e}")
        raise

def process_domain_universal(domain_dir):
    """Process all JSON files in a domain directory"""
    domain_name = domain_dir.name
    domain_type = detect_domain_type(domain_name)
    
    logger.info(f"Processing domain: {domain_name} (type: {domain_type})")
    
    # Create database file
    db_path = DOMAIN_DB_DIR / f"{domain_name}.db"
    
    # Remove existing database if it exists
    if db_path.exists():
        db_path.unlink()
    
    # Connect to database
    conn = sqlite3.connect(str(db_path))
    
    try:
        # Process all JSON files
        json_files = list(domain_dir.glob("*.json"))
        logger.info(f"Found {len(json_files)} JSON files in {domain_name}")
        
        for json_file in json_files:
            process_json_file_universal(conn, json_file, domain_type)
        
        # Verify tables were created
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        logger.info(f"Created {len(tables)} tables in {domain_name}.db")
        
        # Show table info
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table[0]})")
            columns = cursor.fetchall()
            logger.info(f"  Table '{table[0]}': {len(columns)} columns")
        
    finally:
        conn.close()
    
    logger.info(f"Completed processing {domain_name}")

def verify_data_integrity_universal():
    """Verify that all databases were created and contain data"""
    logger.info("Verifying data integrity...")
    
    for db_file in DOMAIN_DB_DIR.glob("*.db"):
        conn = sqlite3.connect(str(db_file))
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = cursor.fetchall()
        
        logger.info(f"\nDatabase: {db_file.name}")
        logger.info(f"Tables: {len(tables)}")
        
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM \"{table_name}\"")
            count = cursor.fetchone()[0]
            logger.info(f"  {table_name}: {count} records")
        
        conn.close()

def main():
    """Main function to process all domains"""
    logger.info("Starting universal database construction...")
    
    if not JSON_FILES_DIR.exists():
        logger.error(f"JSON_FILES_DIR does not exist: {JSON_FILES_DIR}")
        return
    
    # Process each domain directory
    for domain_dir in JSON_FILES_DIR.iterdir():
        if domain_dir.is_dir():
            try:
                process_domain_universal(domain_dir)
            except Exception as e:
                logger.error(f"Error processing domain {domain_dir.name}: {e}")
    
    # Verify all databases
    verify_data_integrity_universal()
    
    logger.info("Universal database construction completed!")

if __name__ == "__main__":
    main() 