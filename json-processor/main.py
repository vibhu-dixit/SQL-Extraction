import json
from .database_manager import create_database
from .data_processor import process_and_store_data

def load_json(file_path):
    """Load JSON data from file."""
    with open(file_path, 'r') as f:
        return json.load(f)

def main(json_files):
    """Process multiple JSON files and store in database."""
    db_name = 'universal_data.db'
    for file_path in json_files:
        try:
            json_data = load_json(file_path)
            conn, cursor, attr_fields, nested_tables, pattern_fields = create_database(json_data, db_name)
            process_and_store_data(json_data, conn, cursor, attr_fields, nested_tables, pattern_fields, file_path)
            conn.close()
            print(f"Processed {file_path} and stored in {db_name}")
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
            if 'conn' in locals():
                conn.close()

if __name__ == "__main__":
    json_files = [
        'json_files/australia.json',
        'json_files/Cadel_Evans.json',
    ]  # Update with actual paths
    main(json_files)