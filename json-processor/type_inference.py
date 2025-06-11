def infer_sql_type(value):
    """Infer SQL data type from a Python value."""
    if value is None:
        return "TEXT"
    if isinstance(value, (int, bool)):
        return "INTEGER"
    if isinstance(value, float):
        return "REAL"
    if isinstance(value, str):
        try:
            float(value.replace(',', '').replace('$', '').replace('–', '0').strip())
            return "REAL"
        except ValueError:
            return "TEXT"
    if isinstance(value, (list, dict)):
        return None  # Nested structures handled separately
    return "TEXT"