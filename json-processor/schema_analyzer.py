import pandas as pd
import os
import re
from datetime import datetime

def process_and_store_data(json_data, conn, cursor, attr_fields, nested_tables, pattern_fields, file_name):
    """Normalize JSON data and insert into database."""
    entity_data = []
    attr_data = []
    leaders_data = []
    nested_data = defaultdict(list)

    entity_id = None
    entity_name = None
    entity_type = None

    # Determine entity name and type
    first_entry = json_data if isinstance(json_data, dict) and not all(re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', k) for k in json_data.keys()) else list(json_data.values())[0] if isinstance(json_data, dict) else json_data[0] if isinstance(json_data, list) else {}
    name_fields = ['name', 'common_name', 'ridername', 'fullname']
    for field in name_fields:
        if field in first_entry:
            entity_name = first_entry[field]
            break
    if not entity_name:
        entity_name = os.path.basename(file_name).replace('.json', '')
    
    # Infer entity type
    if 'common_name' in first_entry:
        entity_type = 'Country'
    elif 'ridername' in first_entry or 'discipline' in first_entry:
        entity_type = 'Person'
    elif 'name' in first_entry and any(f in first_entry for f in ['test_rank', 'odi_rank', 'num_odis']):
        entity_type = 'Team'
    else:
        entity_type = 'Unknown'

    # Check if entity exists
    cursor.execute("SELECT entity_id FROM Entity WHERE name = ? AND entity_type = ?", (entity_name, entity_type))
    result = cursor.fetchone()
    if result:
        entity_id = result[0]
    else:
        cursor.execute("INSERT INTO Entity (name, entity_type) VALUES (?, ?)", (entity_name, entity_type))
        entity_id = cursor.lastrowid
        conn.commit()

    entity_data.append({'entity_id': entity_id, 'name': entity_name, 'entity_type': entity_type})

    # Process records
    records = []
    if isinstance(json_data, dict) and all(re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', k) for k in json_data.keys()):
        records = [(k, v) for k, v in json_data.items()]
        records.sort(key=lambda x: datetime.strptime(x[0], '%Y-%m-%dT%H:%M:%SZ'))
    elif isinstance(json_data, dict):
        records = [('0', json_data)]
    elif isinstance(json_data, list):
        records = [(str(i), item) for i, item in enumerate(json_data)]

    for i, (record_id, entry) in enumerate(records):
        # Attributes
        attr_entry = {'entity_id': entity_id, 'record_id': record_id}
        for field in attr_fields:
            attr_entry[field.replace(' ', '_').replace('-', '_').replace('/', '_')] = entry.get(field)
        attr_data.append(attr_entry)

        # Leaders
        end_date = records[i + 1][0] if i + 1 < len(records) else None
        leader_pairs = {}
        for field in pattern_fields['leaders']:
            if field.startswith('leader_title'):
                num = field.replace('leader_title', '')
                leader_pairs[num] = {'title': entry.get(field)}
            elif field.startswith('leader_name'):
                num = field.replace('leader_name', '')
                if num in leader_pairs:
                    leader_pairs[num]['name'] = entry.get(field)
            elif field in ['od_captain', 't20i_captain']:
                format_type = 'ODI' if field == 'od_captain' else 'T20I'
                if entry.get(field):
                    leaders_data.append({
                        'entity_id': entity_id,
                        'leader_name': entry[field],
                        'leader_role': f"{format_type} Captain",
                        'start_date': record_id,
                        'end_date': end_date
                    })
        for num, pair in leader_pairs.items():
            if pair.get('name') and pair.get('title'):
                leaders_data.append({
                    'entity_id': entity_id,
                    'leader_name': pair['name'],
                    'leader_role': pair['title'],
                    'start_date': record_id,
                    'end_date': end_date
                })

        # Nested data
        for field, fields in nested_tables.items():
            if field in entry and isinstance(entry[field], list):
                table_name = field.capitalize().replace(' ', '_').replace('-', '_').replace('/', '_')
                for item in entry[field]:
                    nested_entry = {'entity_id': entity_id, 'record_id': record_id}
                    if isinstance(item, dict):
                        for k in fields:
                            nested_entry[k.replace(' ', '_').replace('-', '_').replace('/', '_')] = item.get(k)
                    else:
                        nested_entry['value'] = item
                    nested_data[table_name].append(nested_entry)

    # Insert data
    entity_df = pd.DataFrame(entity_data)
    attr_df = pd.DataFrame(attr_data)
    leaders_df = pd.DataFrame(leaders_data)

    if not entity_df.empty:
        entity_df.to_sql('Entity', conn, if_exists='append', index=False)
    if not attr_df.empty and attr_df.shape[1] > 2:
        attr_df.to_sql('Attributes', conn, if_exists='append', index=False)
    if not leaders_df.empty:
        leaders_df.to_sql('Leaders', conn, if_exists='append', index=False)
    for table_name, data in nested_data.items():
        if data:
            pd.DataFrame(data).to_sql(table_name, conn, if_exists='append', index=False)

    conn.commit()