import re
from collections import defaultdict
from .type_inference import infer_sql_type

def analyze_json(json_data, prefix='', field_types=None, nested_fields=None, pattern_fields=None):
    """Analyze JSON structure to collect fields, types, and patterns."""
    if field_types is None:
        field_types = defaultdict(set)
    if nested_fields is None:
        nested_fields = defaultdict(list)
    if pattern_fields is None:
        pattern_fields = defaultdict(set)

    def process_entry(entry, prefix=''):
        if isinstance(entry, dict):
            for key, value in entry.items():
                full_key = f"{prefix}{key}" if prefix else key
                if isinstance(value, dict):
                    process_entry(value, f"{full_key}_")
                    nested_fields[full_key].append(value)
                elif isinstance(value, list):
                    nested_fields[full_key].append(value)
                    for item in value:
                        process_entry(item, f"{full_key}_")
                else:
                    field_types[full_key].add(infer_sql_type(value))
                # Detect patterned fields (e.g., leader_titleX, leader_nameX)
                if re.match(r'.*(_title|_name|_captain)\d+$', key) or key in ['od_captain', 't20i_captain']:
                    pattern_fields['leaders'].add(full_key)

    if isinstance(json_data, dict):
        if all(re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z', k) for k in json_data.keys()):
            # Timestamped structure
            for timestamp, entry in json_data.items():
                process_entry(entry)
        else:
            process_entry(json_data)
    elif isinstance(json_data, list):
        for item in json_data:
            process_entry(item)
    else:
        field_types[str(json_data)] = infer_sql_type(json_data)

    # Resolve field types
    resolved_types = {}
    for field, types in field_types.items():
        if len(types) == 1:
            resolved_types[field] = types.pop()
        else:
            if "INTEGER" in types and "REAL" in types:
                resolved_types[field] = "REAL"
            elif "INTEGER" in types or "REAL" in types:
                resolved_types[field] = "REAL"
            else:
                resolved_types[field] = "TEXT"
    
    return resolved_types, nested_fields, pattern_fields