import sqlite3
from .schema_analyzer import analyze_json

def create_database(json_data, db_name):
    """Create database and tables based on JSON structure."""
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Analyze JSON
    field_types, nested_fields, pattern_fields = analyze_json(json_data)

    # Entity table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Entity (
            entity_id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            entity_type TEXT NOT NULL
        )
    ''')

    # Attributes table
    attr_columns = [
        "attr_id INTEGER PRIMARY KEY AUTOINCREMENT",
        "entity_id INTEGER",
        "record_id TEXT NOT NULL"
    ]
    attr_fields = []
    excluded_fields = ['name', 'common_name', 'ridername', 'fullname'] + list(pattern_fields['leaders'])
    for field, sql_type in field_types.items():
        if field not in excluded_fields and field not in nested_fields:
            sanitized_field = field.replace(' ', '_').replace('-', '_').replace('/', '_')
            attr_columns.append(f"{sanitized_field} {sql_type}")
            attr_fields.append(field)
    
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS Attributes (
            {', '.join(attr_columns)},
            FOREIGN KEY (entity_id) REFERENCES Entity(entity_id)
        )
    ''')

    # Leaders table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS Leaders (
            leader_id INTEGER PRIMARY KEY AUTOINCREMENT,
            entity_id INTEGER,
            leader_name TEXT NOT NULL,
            leader_role TEXT NOT NULL,
            start_date TEXT NOT NULL,
            end_date TEXT,
            FOREIGN KEY (entity_id) REFERENCES Entity(entity_id)
        )
    ''')

    # Nested tables
    nested_tables = {}
    for field, nests in nested_fields.items():
        if isinstance(nests[0], list):
            table_name = field.capitalize().replace(' ', '_').replace('-', '_').replace('/', '_')
            nested_field_types, _, _ = analyze_json(nests[0], prefix=f"{field}_")
            columns = [
                f"{field.lower().replace(' ', '_').replace('-', '_')}_id INTEGER PRIMARY KEY AUTOINCREMENT",
                "entity_id INTEGER",
                "record_id TEXT"
            ] + [f"{k.replace(' ', '_').replace('-', '_').replace('/', '_')} {v}" for k, v in nested_field_types.items()]
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(columns)},
                    FOREIGN KEY (entity_id) REFERENCES Entity(entity_id)
                )
            ''')
            nested_tables[field] = list(nested_field_types.keys())
    
    conn.commit()
    return conn, cursor, attr_fields, nested_tables, pattern_fields