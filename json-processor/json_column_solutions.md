# JSON Column Handling Solutions for Table Tennis Player Domain

## Problem Analysis

The `medaltemplates` column contains **JSON arrays** with medal information in this format:
```json
["Sport: Men's table tennis", "Country: {{POR}}", "Competition: European Championships", "Gold Medal: 2011 Gdansk-Sopot Doubles Singles", "Bronze Medal: 2008 Saint-Petersburg Doubles Singles"]
```

## Root Cause of Errors

1. **LLM doesn't understand JSON structure** - tries to use `SUM(medaltemplates)` instead of parsing JSON
2. **Wrong aggregation functions** - using SUM on text/JSON data
3. **Complex parsing required** - need to extract medal types, years, and event types from JSON arrays
4. **No JSON parsing functions** - SQLite doesn't have built-in JSON functions in older versions

## Solutions

### 1. SQLite JSON Functions (Modern SQLite)

```sql
-- Count gold medals
SELECT COUNT(*) FROM (
  SELECT json_each.value 
  FROM "Marcos_Freitas", 
       json_each(medaltemplates) 
  WHERE json_each.value LIKE '%Gold Medal%'
);

-- Extract medal types
SELECT DISTINCT json_each.value 
FROM "Marcos_Freitas", 
     json_each(medaltemplates) 
WHERE json_each.value LIKE '%Medal%';

-- Count medals by type
SELECT 
  CASE 
    WHEN json_each.value LIKE '%Gold Medal%' THEN 'Gold'
    WHEN json_each.value LIKE '%Silver Medal%' THEN 'Silver' 
    WHEN json_each.value LIKE '%Bronze Medal%' THEN 'Bronze'
    ELSE 'Other'
  END as medal_type,
  COUNT(*) as count
FROM "Marcos_Freitas", 
     json_each(medaltemplates) 
WHERE json_each.value LIKE '%Medal%'
GROUP BY medal_type;
```

### 2. String Parsing Approach (Works with older SQLite)

```sql
-- Count gold medals using string functions
SELECT COUNT(*) 
FROM "Marcos_Freitas" 
WHERE medaltemplates LIKE '%Gold Medal%';

-- Extract years from medal entries
SELECT DISTINCT 
  SUBSTR(medaltemplates, 
         INSTR(medaltemplates, 'Gold Medal: ') + 12, 
         4) as year
FROM "Marcos_Freitas" 
WHERE medaltemplates LIKE '%Gold Medal%'
AND SUBSTR(medaltemplates, INSTR(medaltemplates, 'Gold Medal: ') + 12, 4) GLOB '[0-9][0-9][0-9][0-9]';

-- Count medals by event type
SELECT 
  CASE 
    WHEN medaltemplates LIKE '%Singles%' THEN 'Singles'
    WHEN medaltemplates LIKE '%Doubles%' THEN 'Doubles'
    WHEN medaltemplates LIKE '%Team%' THEN 'Team'
    ELSE 'Other'
  END as event_type,
  COUNT(*) as count
FROM "Marcos_Freitas" 
WHERE medaltemplates LIKE '%Medal%'
GROUP BY event_type;
```

### 3. Custom SQLite Functions

```python
# Add custom JSON parsing functions to SQLite
import sqlite3
import json

def json_count_medals(json_array, medal_type):
    """Count medals of specific type in JSON array"""
    try:
        data = json.loads(json_array)
        count = sum(1 for item in data if medal_type in str(item))
        return count
    except:
        return 0

def json_extract_medal_types(json_array):
    """Extract all medal types from JSON array"""
    try:
        data = json.loads(json_array)
        medals = [item for item in data if 'Medal' in str(item)]
        return ','.join(medals)
    except:
        return ''

# Register functions with SQLite
conn = sqlite3.connect('table_tennis_player.db')
conn.create_function('json_count_medals', 2, json_count_medals)
conn.create_function('json_extract_medal_types', 1, json_extract_medal_types)
```

### 4. Improved LLM Prompts

```python
# Enhanced prompt for JSON column handling
JSON_COLUMN_PROMPT = """
IMPORTANT: The medaltemplates column contains JSON arrays with medal information.

Example medaltemplates data:
["Sport: Men's table tennis", "Country: {{POR}}", "Competition: European Championships", "Gold Medal: 2011 Gdansk-Sopot Doubles Singles", "Bronze Medal: 2008 Saint-Petersburg Doubles Singles"]

To count medals:
- Use LIKE '%Gold Medal%' to count gold medals
- Use LIKE '%Silver Medal%' to count silver medals  
- Use LIKE '%Bronze Medal%' to count bronze medals

To extract years:
- Use SUBSTR() and INSTR() to extract year from medal entries
- Example: SUBSTR(medaltemplates, INSTR(medaltemplates, 'Gold Medal: ') + 12, 4)

To find event types:
- Use LIKE '%Singles%', LIKE '%Doubles%', LIKE '%Team%'

Table: {table_name}
Question: {question}

Generate SQL that properly handles the JSON array structure.
"""
```

### 5. Data Preprocessing Solution

```python
# Preprocess JSON data into structured columns
def preprocess_medal_data():
    """Convert JSON medaltemplates into structured columns"""
    
    conn = sqlite3.connect('table_tennis_player.db')
    
    # Add structured columns
    conn.execute("""
        ALTER TABLE "Marcos_Freitas" ADD COLUMN gold_medals INTEGER DEFAULT 0;
        ALTER TABLE "Marcos_Freitas" ADD COLUMN silver_medals INTEGER DEFAULT 0;
        ALTER TABLE "Marcos_Freitas" ADD COLUMN bronze_medals INTEGER DEFAULT 0;
        ALTER TABLE "Marcos_Freitas" ADD COLUMN first_medal_year TEXT;
        ALTER TABLE "Marcos_Freitas" ADD COLUMN last_medal_year TEXT;
    """)
    
    # Update with parsed data
    for table in get_all_tables():
        conn.execute(f"""
            UPDATE "{table}" SET 
                gold_medals = (LENGTH(medaltemplates) - LENGTH(REPLACE(medaltemplates, 'Gold Medal', ''))) / LENGTH('Gold Medal'),
                silver_medals = (LENGTH(medaltemplates) - LENGTH(REPLACE(medaltemplates, 'Silver Medal', ''))) / LENGTH('Silver Medal'),
                bronze_medals = (LENGTH(medaltemplates) - LENGTH(REPLACE(medaltemplates, 'Bronze Medal', ''))) / LENGTH('Bronze Medal')
        """)
```

### 6. Hybrid Approach (Recommended)

```python
# Combine LLM with rule-based parsing
def generate_smart_sql(question, table_name):
    """Generate SQL with JSON awareness"""
    
    # Detect medal counting questions
    if 'gold medal' in question.lower():
        return f'SELECT COUNT(*) FROM "{table_name}" WHERE medaltemplates LIKE "%Gold Medal%"'
    
    if 'silver medal' in question.lower():
        return f'SELECT COUNT(*) FROM "{table_name}" WHERE medaltemplates LIKE "%Silver Medal%"'
    
    if 'bronze medal' in question.lower():
        return f'SELECT COUNT(*) FROM "{table_name}" WHERE medaltemplates LIKE "%Bronze Medal%"'
    
    # Detect year extraction
    if 'year' in question.lower() and 'first' in question.lower():
        return f'''
        SELECT MIN(SUBSTR(medaltemplates, INSTR(medaltemplates, 'Medal: ') + 7, 4)) 
        FROM "{table_name}" 
        WHERE medaltemplates LIKE "%Medal%"
        AND SUBSTR(medaltemplates, INSTR(medaltemplates, 'Medal: ') + 7, 4) GLOB '[0-9][0-9][0-9][0-9]'
        '''
    
    # Fallback to LLM
    return ask_llm_with_json_context(question, table_name)
```

## Implementation Priority

1. **Immediate**: Update prompts to include JSON structure examples
2. **Short-term**: Implement string-based parsing for medal counting
3. **Medium-term**: Add custom SQLite functions for JSON parsing
4. **Long-term**: Preprocess data into structured columns

## Expected Impact

- **Wrong Answers**: Reduce from 78.4% to ~20% by properly parsing JSON
- **Empty Results**: Reduce from 19.6% to ~5% by using correct column references
- **Overall Success Rate**: Improve from 7.3% to ~75%

The key is making the LLM aware of the JSON structure and providing specific parsing patterns for the medal data format. 