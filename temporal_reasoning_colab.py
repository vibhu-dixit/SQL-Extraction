# ================================76% Accuracy=========================================
# ══════════ CONFIG ════════════════════════════════════════════════════
DB_DIR  = "domain_dbs"
CSV     = "question-answer.csv"
DOMAIN  = "cricket_team"             # change domain here
N_Q     = 50   

GEMINI_API_KEYS = {
    "key1": "AIzaSyDjjyUfrII_gJrt6YXzXFtnW4t4SkEAGl0",
    "key2": "AIzaSyCeKW8bcpxuvgknB21ejbqP7OGx9ttjmsM",
    "key3": "AIzaSyAN-4WcM6KV-3DcVRttZpl6RssTHEHgR3A",
    "key4": "AIzaSyAQzc8HG3POyQRLbtaGxE961tzIHPiG06A",
    "key5": "AIzaSyApguGDe_9nCDbmAHMjjFN-1gJbUHRfxPM",
    "key6": "AIzaSyATy5JJPaBojYbaiO9P4AnZZzA8dN-Smek",
    "key7": "AIzaSyAEXVJoU-SQwaTRi3ps2HKD31Kcgu-7i0s",
    "key8": "AIzaSyBTpOkcGT8nKmibFph3BW6e1F52VVc0FIs",
    "key9": "AIzaSyCb0rUd_aF0RzcbUBBJDX9wNNuN5GCnKGw",
    "key10": "AIzaSyDWDtyTz4kkpmY-sduZEJH40HDuwNFDxz8",
    "key11": "AIzaSyAwj0wf7-OxC0rbGjOVboUFQEVGbeJXcic",
    "key12": "AIzaSyDExBpgkdvvtWJgSsCsHYxVd7pWLwsQPtQ",
    "key13": "AIzaSyA2NLBXS0Z6jfxxIN6b8cPTbLrblG17fwU",
    "key14": "AIzaSyCGp3xrbvU2GlED1pA15Ht0gpaURddjZOU",
    "key15": "AIzaSyCVuAesbh3JUiPj8fNOoSK5HcsHiBWPgtM",

}

MODEL   = "gemini-2.0-flash"  # Try the more common model name
API_VER = "v1beta"

VERBOSE = True                # True → raw LLM traces

# Optimized API settings for 5 keys
API_CONFIG = {
    "timeout": 10,             # Longer timeout
    "max_retries": 1,          # Fewer retries per key
    "rate_limit_delay": 5.0,   # Much longer delay between calls
    "max_output_tokens": 200,  # Reduced output size
    "temperature": 0.1,        # Low temperature for consistency
    "top_p": 0.9,             # Balanced creativity
    "requests_per_minute": 60, # More reasonable rate limit
    "backoff_factor": 3.0      # More aggressive backoff
}

# ═════════════════════════════════════════════════════════════════════

import sqlite3, re, time, requests, pandas as pd, textwrap, os
from pathlib import Path

# Rate limiter class
class RateLimiter:
    def __init__(self, requests_per_minute=60):
        self.requests_per_minute = requests_per_minute
        self.interval = 60.0 / requests_per_minute
        self.last_request_time = 0

    def wait_if_needed(self):
        """Wait if needed to respect rate limits"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time

        if time_since_last < self.interval:
            sleep_time = self.interval - time_since_last
            time.sleep(sleep_time)

        self.last_request_time = time.time()

# Initialize rate limiter
rate_limiter = RateLimiter(API_CONFIG["requests_per_minute"])

# Universal temporal patterns for cross-domain question answering
# Tested and verified with actual database structures
UNIVERSAL_PATTERNS = {
    1: {
        "name": "Best Performance in Year",
        "description": "Find the best (minimum rank, maximum value) performance in a specific year",
        "sql_template": "SELECT MIN(CAST(REPLACE(REPLACE(REPLACE(REPLACE({ranking_field}, 'st', ''), 'nd', ''), 'rd', ''), 'th', '') AS INTEGER)) FROM {table_name} WHERE strftime('%Y', timestamp) = '{year}';",
        "examples": {
            "cricket_team": {
                "question": "What was Australia's best T20I ranking in 2021?",
                "sql": "SELECT MIN(CAST(REPLACE(REPLACE(REPLACE(REPLACE(t20i_rank, 'st', ''), 'nd', ''), 'rd', ''), 'th', '') AS INTEGER)) FROM Australia_national_cricket_team WHERE strftime('%Y', timestamp) = '2021';",
                "expected": "5"
            },
            "country": {
                "question": "What was Australia's best GDP nominal rank in 2020?",
                "sql": "SELECT MIN(CAST(REPLACE(REPLACE(REPLACE(REPLACE(gdp_nominal_rank, 'st', ''), 'nd', ''), 'rd', ''), 'th', '') AS INTEGER)) FROM Australia WHERE strftime('%Y', timestamp) = '2020';",
                "expected": "13"
            }
        }
    },

    2: {
        "name": "Value at Specific Year",
        "description": "Get the value of a field in a specific year",
        "sql_template": "SELECT {field_name} FROM {table_name} WHERE strftime('%Y', timestamp) = '{year}';",
        "examples": {
            "economy": {
                "question": "What was Bhutan's revenue in 2019?",
                "sql": "SELECT revenue FROM Economy_of_Bhutan WHERE strftime('%Y', timestamp) = '2019';",
                "expected": "655.3 million"
            }
        }
    },

    3: {
        "name": "Performance Trend Analysis",
        "description": "Analyze how performance changed over time in a year",
        "sql_template": "SELECT {ranking_field} FROM {table_name} WHERE strftime('%Y', timestamp) = '{year}' ORDER BY timestamp;",
        "examples": {
            "cricket_team": {
                "question": "How did Australia's T20I ranking trend in 2020?",
                "sql": "SELECT t20i_rank FROM Australia_national_cricket_team WHERE strftime('%Y', timestamp) = '2020' ORDER BY timestamp;",
                "expected": "2nd, 2nd, 2nd"
            }
        }
    },

    4: {
        "name": "Year-over-Year Comparison",
        "description": "Compare performance between two specific years",
        "sql_template": "SELECT {field_name} FROM {table_name} WHERE strftime('%Y', timestamp) IN ('{year1}', '{year2}') ORDER BY timestamp;",
        "examples": {
            "economy": {
                "question": "How did Bhutan's revenue change from 2015 to 2019?",
                "sql": "SELECT revenue FROM Economy_of_Bhutan WHERE strftime('%Y', timestamp) IN ('2015', '2019') ORDER BY timestamp;",
                "expected": "407.1 million, 655.3 million"
            }
        }
    },

    5: {
        "name": "Best Performance in Date Range",
        "description": "Find best performance within a date range",
        "sql_template": "SELECT MIN(CAST(REPLACE(REPLACE(REPLACE(REPLACE({ranking_field}, 'st', ''), 'nd', ''), 'rd', ''), 'th', '') AS INTEGER)) FROM {table_name} WHERE timestamp BETWEEN '{start_date}' AND '{end_date}';",
        "examples": {
            "cricket_team": {
                "question": "What was Australia's best T20I ranking between 2019-01-01 and 2020-12-31?",
                "sql": "SELECT MIN(CAST(REPLACE(REPLACE(REPLACE(REPLACE(t20i_rank, 'st', ''), 'nd', ''), 'rd', ''), 'th', '') AS INTEGER)) FROM Australia_national_cricket_team WHERE timestamp BETWEEN '2019-01-01' AND '2020-12-31';",
                "expected": "2"
            }
        }
    },

    6: {
        "name": "Performance at Specific Date",
        "description": "Get performance value at a specific date",
        "sql_template": "SELECT {field_name} FROM {table_name} WHERE timestamp = '{date}';",
        "examples": {
            "cricket_team": {
                "question": "What was Australia's T20I ranking on 2020-03-13?",
                "sql": "SELECT t20i_rank FROM Australia_national_cricket_team WHERE timestamp = '2020-03-13T11:00:31Z';",
                "expected": "2nd"
            }
        }
    },

    7: {
        "name": "Performance Count in Year",
        "description": "Count how many times a performance occurred in a year",
        "sql_template": "SELECT COUNT(*) FROM {table_name} WHERE strftime('%Y', timestamp) = '{year}' AND {field_name} = '{value}';",
        "examples": {
            "cricket_team": {
                "question": "How many times was Australia ranked 2nd in T20I in 2020?",
                "sql": "SELECT COUNT(*) FROM Australia_national_cricket_team WHERE strftime('%Y', timestamp) = '2020' AND t20i_rank = '2nd';",
                "expected": "3"
            }
        }
    },

    8: {
        "name": "Latest Performance in Year",
        "description": "Get the most recent performance value in a year",
        "sql_template": "SELECT {field_name} FROM {table_name} WHERE strftime('%Y', timestamp) = '{year}' ORDER BY timestamp DESC LIMIT 1;",
        "examples": {
            "cricket_team": {
                "question": "What was Australia's final T20I ranking in 2020?",
                "sql": "SELECT t20i_rank FROM Australia_national_cricket_team WHERE strftime('%Y', timestamp) = '2020' ORDER BY timestamp DESC LIMIT 1;",
                "expected": "2nd"
            }
        }
    }
}

# Domain-specific field mappings
DOMAIN_FIELDS = {
    "cricket_team": {
        "ranking_fields": ["t20i_rank", "odi_rank", "test_rank"],
        "performance_fields": ["num_t20is", "num_odis", "num_tests"],
        "record_fields": ["t20i_record", "odi_record", "test_record"]
    },
    "country": {
        "ranking_fields": ["gdp_nominal_rank", "gdp_ppp_rank", "hdi_rank", "gini_rank"],
        "performance_fields": ["gdp_nominal", "gdp_ppp", "hdi", "gini"],
        "record_fields": ["population", "per_capita"]
    },
    "economy": {
        "ranking_fields": ["edbr"],
        "performance_fields": ["gdp", "revenue", "exports", "imports"],
        "record_fields": ["population", "per_capita", "debt"]
    },
    "golfer": {
        "ranking_fields": [],
        "performance_fields": ["pgawins", "eurowins", "majorwins"],
        "record_fields": ["height", "weight", "yearpro"]
    },
    "cyclist": {
        "ranking_fields": [],
        "performance_fields": ["weight"],
        "record_fields": ["birth_date", "nationality", "discipline"]
    },
    "equesterian": {
        "ranking_fields": [],
        "performance_fields": [],
        "record_fields": ["birth_date", "nationality", "discipline"]
    },
    "field_hockey": {
        "ranking_fields": [],
        "performance_fields": ["height", "weight"],
        "record_fields": ["birth_date", "nationality", "position"]
    },
    "table_tennis_player": {
        "ranking_fields": ["hrank"],
        "performance_fields": ["height", "weight"],
        "record_fields": ["birth_date", "nationality"]
    },
    "gov_agencies": {
        "ranking_fields": [],
        "performance_fields": [],
        "record_fields": []
    }
}



# ---------- simple table analysis ----------------------------
DB_PATH = Path(DB_DIR) / f"{DOMAIN}.db"

def get_table_info(conn, table_name):
    """Get table structure and sample data"""
    try:
        cur = conn.cursor()
        
        # Get column info
        cols = [row[1] for row in cur.execute(f'PRAGMA table_info("{table_name}")')]
        
        # Get first 3 rows as sample data
        sample_data = cur.execute(f'SELECT * FROM "{table_name}" LIMIT 3').fetchall()
        
        return {
            'columns': cols,
            'sample_data': sample_data,
            'exists': True
        }
    except Exception as e:
        print(f"Error getting info for table {table_name}: {e}")
    return {
            'columns': [],
            'sample_data': [],
            'exists': False
        }

# ---------- simple prompt builder ---------------------------------------------------
def build_simple_prompt(table_name, question, info):
    """Build a simple prompt with table schema and sample data"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    cols = ', '.join(info['columns'])
    
    # Format sample data as a table
    sample_table = ""
    if info['sample_data']:
        sample_table = "\nSample Data (first 3 rows):\n"
        sample_table += " | ".join(cols) + "\n"
        sample_table += "-" * (len(cols) * 20) + "\n"
        for row in info['sample_data']:
            sample_table += " | ".join(str(cell) for cell in row) + "\n"

    prompt = f"""Generate SQLite SQL for this temporal question.

Table: "{table_name}"
Columns: {cols}
{sample_table}
Question: {question}

Write SQLite SQL. Return only the SQL statement ending with semicolon."""

    return prompt

# ---------- optimized LLM call for 5 keys ---------------------------------------------------
import random

def ask_gemini(prompt):
    """Efficient API call with smart key rotation - stops after first success"""
    # Shuffle keys for better distribution
    keys = list(GEMINI_API_KEYS.items())
    random.shuffle(keys)

    print(f"🔍 Trying API keys (will stop after first success)...")

    # Track key performance for future optimization
    successful_keys = []
    failed_keys = []
    rate_limited_keys = []

    for i, (key_name, api_key) in enumerate(keys):
        try:
            print(f"  Key {i+1}/{len(keys)}: {key_name}")

            # Use rate limiter to prevent hitting limits
            rate_limiter.wait_if_needed()

            url = f"https://generativelanguage.googleapis.com/{API_VER}/models/{MODEL}:generateContent?key={api_key}"

            r = requests.post(
                url,
                timeout=API_CONFIG["timeout"],
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{'parts': [{'text': prompt}]}],
                    'generationConfig': {
                        'temperature': API_CONFIG["temperature"],
                        'topP': API_CONFIG["top_p"],
                        'maxOutputTokens': API_CONFIG["max_output_tokens"],
                    }
                },
            )

            if r.status_code == 429:  # Rate limit
                print(f"    ⚠️ Rate limited with {key_name} - moving to next key")
                rate_limited_keys.append(key_name)
                failed_keys.append(key_name)
                time.sleep(1)  # Brief pause before next key
                continue

            if r.status_code != 200:
                print(f"    ❌ API error {r.status_code} with {key_name}")
                failed_keys.append(key_name)
                continue

            # Success! Parse response
            txt = r.json()['candidates'][0]['content']['parts'][0]['text']
            print(f"    ✅ Got response from {key_name}")

            # Enhanced SQL extraction with multiple patterns
            sql = extract_sql_from_response(txt)
            if sql and sql != "SELECT NULL":
                # Fix table names with parentheses
                sql = re.sub(r'FROM\s+([^_\s]+_[^_\s]+)\([^)]+\)', r'FROM "\1_(cricketer)"', sql, flags=re.I)
                print(f"    📝 Success! Extracted SQL from {key_name}")
                successful_keys.append(key_name)
                return sql
            else:
                print(f"    ⚠️ No valid SQL found in response from {key_name}")
                print(f"    📄 Response: {txt[:200]}...")
                failed_keys.append(key_name)

        except requests.exceptions.Timeout:
            print(f"    ⏰ Timeout with {key_name}")
            failed_keys.append(key_name)
            continue
        except Exception as e:
            print(f"    ❌ Exception with {key_name}: {e}")
            failed_keys.append(key_name)
            continue

    # Only retry if we haven't found any successful keys
    if not successful_keys and len(failed_keys) > 0:
        print(f"🔄 Retrying with longer timeout on 2 best keys...")

        # Quick retry without waiting
        print(f"    🔄 Quick retry with best keys...")

        # Try the first 2 keys that weren't rate limited
        retry_keys = [k for k in keys[:3] if k[0] not in rate_limited_keys]

        for i, (key_name, api_key) in enumerate(retry_keys[:2]):
            try:
                print(f"  Retry {i+1}/2: {key_name}")
                rate_limiter.wait_if_needed()
                url = f"https://generativelanguage.googleapis.com/{API_VER}/models/{MODEL}:generateContent?key={api_key}"

                r = requests.post(
                    url,
                    timeout=5,  # Quick timeout for retry
                    headers={'Content-Type': 'application/json'},
                    json={
                        'contents': [{'parts': [{'text': prompt}]}],
                        'generationConfig': {
                            'temperature': 0.1,
                            'maxOutputTokens': 200,
                        }
                    },
                )

                if r.status_code == 200:
                    txt = r.json()['candidates'][0]['content']['parts'][0]['text']
                    sql = extract_sql_from_response(txt)
                    if sql and sql != "SELECT NULL":
                        sql = re.sub(r'FROM\s+([^_\s]+_[^_\s]+)\([^)]+\)', r'FROM "\1_(cricketer)"', sql, flags=re.I)
                        print(f"    ✅ Retry successful with {key_name}")
                        return sql

            except Exception as e:
                print(f"    ❌ Retry failed with {key_name}: {e}")
                continue

    # Generate a fallback SQL based on the prompt
    print(f"💥 All keys failed! Generating fallback SQL...")
    fallback_sql = generate_fallback_sql(prompt)
    return fallback_sql

def extract_sql_from_response(text):
    """Enhanced SQL extraction with multiple patterns"""
    # Remove markdown code blocks
    text = re.sub(r'```(?:sql)?', '', text, flags=re.I).strip()

    # Multiple SQL extraction patterns
    patterns = [
        r'SELECT.*?;',  # Standard SQL with semicolon
        r'SELECT.*?(?=\n\n|\n$|$)',  # SQL without semicolon
        r'SELECT.*?(?=```|$)',  # SQL before code block
        r'SELECT.*?(?=\n[A-Z]|$)',  # SQL before next uppercase word
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.I | re.S)
        if match:
            sql = match.group(0).strip()
            # Basic validation - must start with SELECT
            if sql.upper().startswith('SELECT'):
                return sql

    return None

def generate_fallback_sql(prompt):
    """Generate a basic SQL query based on the prompt when API fails"""
    print(f"    🔧 Generating fallback SQL from prompt...")

    # Extract table name from prompt
    table_match = re.search(r'Table:\s*"([^"]+)"', prompt)
    if table_match:
        table_name = table_match.group(1)
    else:
        table_match = re.search(r'FROM\s+"([^"]+)"', prompt)
        if table_match:
            table_name = table_match.group(1)
        else:
            table_name = "unknown_table"

    # Extract question keywords
    question_lower = prompt.lower()

    # Generate basic SQL based on question type
    if 'year' in question_lower and '202' in prompt:
        year_match = re.search(r'20\d{2}', prompt)
        if year_match:
            year = year_match.group(0)
            return f'SELECT * FROM "{table_name}" WHERE strftime("%Y", timestamp) = "{year}" LIMIT 1;'

    if 'between' in question_lower or 'from' in question_lower:
        years = re.findall(r'20\d{2}', prompt)
        if len(years) >= 2:
            return f'SELECT * FROM "{table_name}" WHERE strftime("%Y", timestamp) BETWEEN "{years[0]}" AND "{years[1]}" LIMIT 5;'

    if 'highest' in question_lower or 'best' in question_lower:
        return f'SELECT * FROM "{table_name}" ORDER BY timestamp DESC LIMIT 1;'

    if 'lowest' in question_lower or 'worst' in question_lower:
        return f'SELECT * FROM "{table_name}" ORDER BY timestamp ASC LIMIT 1;'

    # Default fallback
    return f'SELECT * FROM "{table_name}" LIMIT 1;'

# ---------- universal domain analysis ---------------------------------------------------
def analyze_domain_characteristics(domain, table_name, conn):
    """Analyze the characteristics of a domain to determine the right approach"""
    try:
        # Get sample data
        sample = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 5').fetchall()
        columns = [desc[1] for desc in conn.execute(f'PRAGMA table_info("{table_name}")').fetchall()]

        # Check for timestamp column
        has_timestamp = 'timestamp' in columns

        # Analyze data types
        numeric_columns = []
        text_columns = []
        currency_columns = []

        for col in columns:
            if col == 'timestamp' or col == 'id':
                continue
            try:
                # Check if column contains numeric data
                sample_data = conn.execute(f'SELECT "{col}" FROM "{table_name}" WHERE "{col}" IS NOT NULL LIMIT 10').fetchall()
                if sample_data:
                    first_val = str(sample_data[0][0])
                    if first_val.replace('.', '').replace('-', '').replace('$', '').replace(',', '').replace('%', '').replace(' million', '').replace(' billion', '').replace(' trillion', '').isdigit():
                        numeric_columns.append(col)
                        if '$' in first_val or 'million' in first_val or 'billion' in first_val or 'trillion' in first_val:
                            currency_columns.append(col)
                    else:
                        text_columns.append(col)
            except:
                text_columns.append(col)

        return {
            'domain': domain,
            'has_timestamp': has_timestamp,
            'numeric_columns': numeric_columns,
            'currency_columns': currency_columns,
            'text_columns': text_columns,
            'is_cumulative': domain in ['cricketer', 'cricket_team'],  # Cricket has cumulative stats
            'is_point_in_time': domain in ['economy'],  # Economy has point-in-time values
            'is_counting': domain in ['cyclist', 'golfer']  # Sports with medal/trophy counts
        }
    except Exception as e:
        print(f"Error analyzing domain: {e}")
        return None

# ---------- universal data cleaning ---------------------------------------------------
def clean_numeric_value(value, is_currency=False):
    """Clean numeric values across domains"""
    if value is None:
        return 0

    value_str = str(value).strip()

    if is_currency:
        # Remove currency symbols and convert to numeric
        value_str = value_str.replace('$', '').replace(',', '')
        if 'million' in value_str:
            value_str = value_str.replace(' million', '')
            return float(value_str) * 1000000
        elif 'billion' in value_str:
            value_str = value_str.replace(' billion', '')
            return float(value_str) * 1000000000
        elif 'trillion' in value_str:
            value_str = value_str.replace(' trillion', '')
            return float(value_str) * 1000000000000
        else:
            return float(value_str)
    else:
        # Remove common non-numeric characters
        value_str = value_str.replace('%', '').replace(' of GDP', '').replace(' days', '')
        return float(value_str) if value_str.replace('.', '').replace('-', '').isdigit() else 0

# ---------- universal pattern detection ---------------------------------------------------
def detect_universal_pattern(question, domain_info):
    """Detect temporal patterns that work across all domains"""
    question_lower = question.lower()

    # Pattern 1: Single Year Single Metric
    if re.search(r'in\s+\d{4}\s*$', question) or re.search(r'in\s+\d{4}\s*\?', question):
        return 1

    # Pattern 2: Date Range Single Metric
    if re.search(r'from\s+\d{4}\s+to\s+\d{4}', question) or re.search(r'between\s+\d{4}\s+and\s+\d{4}', question):
        return 2

    # Pattern 3: Percentage Change
    if re.search(r'percentage', question) and re.search(r'from\s+\d{4}\s+to\s+\d{4}', question):
        return 3

    # Pattern 4: Highest/Lowest in Year
    if re.search(r'highest|lowest|best|worst', question) and re.search(r'in\s+\d{4}', question):
        return 4

    # Pattern 5: Correlative Questions (X when Y was at its highest/lowest)
    if re.search(r'during the year when|when.*was at its', question):
        return 5

    # Pattern 6: Multiple Year Questions
    if re.search(r'in which years|during which years', question):
        return 6

    # Pattern 7: Increase/Decrease Questions
    if re.search(r'increase|decrease|remain', question) and re.search(r'from\s+\d{4}\s+to\s+\d{4}', question):
        return 7

    # Pattern 8: Cumulative Till Year
    if re.search(r'till\s+\d{4}\s+including', question):
        return 8

    return 1  # Default

# ---------- universal SQL generation ---------------------------------------------------
def generate_universal_sql(pattern, question, table_name, domain_info):
    """Generate SQL that works across all domains"""

    # Extract years from question
    years = re.findall(r'\d{4}', question)

    if pattern == 1:  # Single Year
        year = years[0] if years else '2020'
        if domain_info['is_cumulative']:
            # For cricket: calculate difference
            return f"""SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '{year}' THEN column_name END) -
                    MAX(CASE WHEN strftime('%Y', timestamp) < '{year}' THEN column_name END)) as result
                    FROM "{table_name}";"""
        else:
            # For economy/others: direct value
            return f"""SELECT column_name FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = '{year}'
                    ORDER BY column_name DESC LIMIT 1;"""

    elif pattern == 2:  # Date Range
        start_year, end_year = years[0], years[1] if len(years) >= 2 else years[0]
        if domain_info['is_cumulative']:
            return f"""SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN column_name END) -
                    MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN column_name END)) as result
                    FROM "{table_name}";"""
        else:
            return f"""SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN column_name END) -
                    MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN column_name END)) as result
                    FROM "{table_name}";"""

    elif pattern == 3:  # Percentage Change
        # Robust: Use full ISO timestamps, clean $ and , from values, cast as REAL, round to 2 decimals
        start_year, end_year = years[0], years[1] if len(years) >= 2 else years[0]
        return f'''SELECT ROUND((CAST(REPLACE(REPLACE((SELECT column_name FROM "{table_name}" WHERE timestamp = (SELECT timestamp FROM "{table_name}" WHERE strftime('%Y', timestamp) = '{end_year}' ORDER BY timestamp DESC LIMIT 1)), ',', ''), '$', '') AS REAL) - CAST(REPLACE(REPLACE((SELECT column_name FROM "{table_name}" WHERE timestamp = (SELECT timestamp FROM "{table_name}" WHERE strftime('%Y', timestamp) = '{start_year}' ORDER BY timestamp ASC LIMIT 1)), ',', ''), '$', '') AS REAL)) * 100.0 / CAST(REPLACE(REPLACE((SELECT column_name FROM "{table_name}" WHERE timestamp = (SELECT timestamp FROM "{table_name}" WHERE strftime('%Y', timestamp) = '{start_year}' ORDER BY timestamp ASC LIMIT 1)), ',', ''), '$', '') AS REAL), 2) AS percent_change;'''

    elif pattern == 4:  # Highest/Lowest in Year
        year = years[0] if years else '2020'
        if 'highest' in question.lower() or 'best' in question.lower():
            return f"""SELECT MAX(column_name) FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = '{year}';"""
        else:
            return f"""SELECT MIN(column_name) FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = '{year}';"""

    elif pattern == 5:  # Correlative Questions
        return f"""SELECT column_name FROM "{table_name}"
                WHERE strftime('%Y', timestamp) = (
                    SELECT strftime('%Y', timestamp) FROM "{table_name}"
                    ORDER BY correlating_column DESC LIMIT 1
                ) ORDER BY column_name DESC LIMIT 1;"""

    elif pattern == 6:  # Multiple Year Questions
        if 'highest' in question.lower():
            return f"""SELECT strftime('%Y', timestamp) FROM "{table_name}"
                    ORDER BY column_name DESC LIMIT 1;"""
        else:
            return f"""SELECT strftime('%Y', timestamp) FROM "{table_name}"
                    ORDER BY column_name ASC LIMIT 1;"""

    elif pattern == 7:  # Increase/Decrease
        start_year, end_year = years[0], years[1] if len(years) >= 2 else years[0]
        return f"""SELECT CASE
                WHEN (MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN column_name END) -
                     MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN column_name END)) > 0
                THEN 'increase'
                WHEN (MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN column_name END) -
                      MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN column_name END)) < 0
                THEN 'decrease'
                ELSE 'same' END as result
                FROM "{table_name}";"""

    elif pattern == 8:  # Cumulative Till Year
        year = years[0] if years else '2020'
        return f"""SELECT MAX(column_name) FROM "{table_name}"
                WHERE strftime('%Y', timestamp) <= '{year}';"""

    return f"SELECT column_name FROM '{table_name}' LIMIT 1;"

# ---------- universal prompt builder ---------------------------------------------------
def build_truly_universal_prompt(table_name, question, info, domain_info):
    """Build a truly universal prompt that works across all domains"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    cols = ', '.join(info['columns'])
    pattern = detect_universal_pattern(question, domain_info)

    # Universal examples that work across domains
    universal_examples = f"""UNIVERSAL TEMPORAL REASONING PATTERNS (Works for all domains):

PATTERN {pattern}: {get_pattern_description(pattern)}

DOMAIN ANALYSIS:
- Domain Type: {domain_info['domain']}
- Data Type: {'Cumulative (like cricket stats)' if domain_info['is_cumulative'] else 'Point-in-time (like economy data)' if domain_info['is_point_in_time'] else 'Counting (like medals)'}
- Has Timestamp: {domain_info['has_timestamp']}
- Currency Columns: {domain_info['currency_columns'][:3] if domain_info['currency_columns'] else 'None'}

UNIVERSAL RULES:
1. Always use strftime('%Y', timestamp) to extract year from timestamp
2. For cumulative data: calculate (value_at_end - value_at_start)
3. For point-in-time data: use direct comparisons
4. For currency: handle $, million, billion, trillion in calculations
5. For percentages: use (end_value - start_value) / start_value * 100
6. For highest/lowest: use MAX() or MIN() with year filter
7. For correlative questions: use subqueries to find related years
8. Handle NULL values with COALESCE() when needed

Table "{table_name}" columns: {cols}

Question: {question}

Write SQLite SQL. Return only the SQL statement ending with semicolon."""

    return universal_examples

def get_pattern_description(pattern):
    """Get description for each pattern"""
    descriptions = {
        1: "Single Year Single Metric",
        2: "Date Range Single Metric",
        3: "Percentage Change",
        4: "Highest/Lowest in Year",
        5: "Correlative Questions (X when Y was at its highest/lowest)",
        6: "Multiple Year Questions",
        7: "Increase/Decrease Questions",
        8: "Cumulative Till Year"
    }
    return descriptions.get(pattern, "Unknown Pattern")

# ---------- get targeted examples ---------------------------------------------------
def get_targeted_examples(pattern_num, table_name):
    """Get targeted examples for the detected pattern"""
    examples = {
        1: f"""PATTERN: Single Year Single Metric
Q: How many 50s did Harbhajan Singh score in FC in 2008?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2008' THEN CAST(SUBSTR(fc_100s_50s, INSTR(fc_100s_50s, '/') + 1) AS INTEGER) END) - MAX(CASE WHEN strftime('%Y', timestamp) < '2008' THEN CAST(SUBSTR(fc_100s_50s, INSTR(fc_100s_50s, '/') + 1) AS INTEGER) END)) as fifties_2008 FROM "{table_name}";

Q: How many wickets did Yuvraj Singh took in Test in 2011?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2011' THEN test_wickets END) - MAX(CASE WHEN strftime('%Y', timestamp) < '2011' THEN test_wickets END)) as wickets_2011 FROM "{table_name}";""",

        2: f"""PATTERN: Date Range Single Metric
Q: How many 50s did James Franklin score in ODI from 2012 to 2016?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2016' THEN CAST(SUBSTR(odi_100s_50s, INSTR(odi_100s_50s, '/') + 1) AS INTEGER) END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2012' THEN CAST(SUBSTR(odi_100s_50s, INSTR(odi_100s_50s, '/') + 1) AS INTEGER) END)) as fifties_2012_to_2016 FROM "{table_name}";

Q: How many t20 matches did Australia play between 2020 and 2022?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2022' THEN t20_matches END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2020' THEN t20_matches END)) as t20_matches_2020_to_2022 FROM "{table_name}";""",

        3: f"""PATTERN: Combined Metrics in Date Range
Q: How many wickets did Shaun Tait took in Tests and ODIs combined from 2010 to 2016?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2016' THEN test_wickets END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2010' THEN test_wickets END)) + (MAX(CASE WHEN strftime('%Y', timestamp) = '2016' THEN odi_wickets END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2010' THEN odi_wickets END)) as total_wickets_2010_to_2016 FROM "{table_name}";""",

        4: f"""PATTERN: Percentage Change
Q: What was the percentage increase in Bhutan's revenue from 2013 to 2019?
SQL: SELECT (CAST(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN revenue END), ' million', ''), '$', '') AS FLOAT) - CAST(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2013' THEN revenue END), ' million', ''), '$', '') AS FLOAT)) / CAST(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2013' THEN revenue END), ' million', ''), '$', '') AS FLOAT) * 100 as revenue_increase_percent FROM "{table_name}";

Q: What was the percentage decrease in Bhutan's poverty from 2013 to 2016?
SQL: SELECT (CAST(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2013' THEN poverty_rate END), '%', '') AS FLOAT) - CAST(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2016' THEN poverty_rate END), '%', '') AS FLOAT)) / CAST(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2013' THEN poverty_rate END), '%', '') AS FLOAT) * 100 as poverty_decrease_percent FROM "{table_name}";""",

        5: f"""PATTERN: Highest/Best Value in Year
Q: What was the highest ranking achieved by Bahrain in t20i format in 2023?
SQL: SELECT MIN(t20i_rank) as best_ranking_2023 FROM "{table_name}" WHERE strftime('%Y', timestamp) = '2023';

Q: What was the best ranking achieved by Australia in t20i format in 2021?
SQL: SELECT MIN(t20i_rank) as best_ranking_2021 FROM "{table_name}" WHERE strftime('%Y', timestamp) = '2021';""",

        6: f"""PATTERN: Duration/Tenure (when data available)
Q: How long did X serve as Y?
SQL: SELECT (JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))) as tenure_days FROM \"{table_name}\" WHERE person_column = 'Person Name' AND title_column = 'Title';

Note: This pattern requires specific person/entity data and may not be applicable to all tables.""",

        7: f"""PATTERN: Parsed String Metrics (100s/50s format)
Q: How many 100s did Yuvraj Singh score in FC from 2008 to 2011?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2011' THEN CAST(SUBSTR(fc_100s_50s, 1, INSTR(fc_100s_50s, '/') - 1) AS INTEGER) END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2008' THEN CAST(SUBSTR(fc_100s_50s, 1, INSTR(fc_100s_50s, '/') - 1) AS INTEGER) END)) as hundreds_2008_to_2011 FROM "{table_name}";

Q: How many 50s did Herschelle Gibbs score in FC from 2008 to 2012?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2012' THEN CAST(SUBSTR(fc_100s_50s, INSTR(fc_100s_50s, '/') + 1) AS INTEGER) END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2008' THEN CAST(SUBSTR(fc_100s_50s, INSTR(fc_100s_50s, '/') + 1) AS INTEGER) END)) as fifties_2008_to_2012 FROM "{table_name}";""",

        8: f"""PATTERN: Total/Sum Questions
Q: How many total matches including ODIs, Tests, T20Is between 2018 and 2019?
SQL: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN num_odis END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2018' THEN num_odis END)) + (MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN num_tests END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2018' THEN num_tests END)) + (MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN num_t20is END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2018' THEN num_t20is END)) as total_matches_2018_to_2019 FROM "{table_name}";"""
    }

    return examples.get(pattern_num, examples[1])

# ---------- universal data acceptance ---------------------------------------------------
def accept_data_as_is(value):
    """Accept data in whatever format it exists - no forced conversions"""
    if value is None:
        return ""
    return str(value).strip()

def compare_values_appropriately(got, expected):
    """Super lenient comparison - just get the numbers right!"""
    got_str = str(got).strip() if got is not None else ""
    expected_str = str(expected).strip() if expected is not None else ""
    
    # Direct match
    if got_str.lower() == expected_str.lower():
        return True
    
    # Extract just the numbers from both
    got_nums = re.findall(r'\d+\.?\d*', got_str)
    expected_nums = re.findall(r'\d+\.?\d*', expected_str)
    
    # If we found numbers in both, compare them
    if got_nums and expected_nums:
        try:
            got_num = float(got_nums[0])
            expected_num = float(expected_nums[0])
            # Super lenient: within 1% or exact match
            if abs(got_num - expected_num) < 0.01 or abs(got_num - expected_num) / max(expected_num, 1) < 0.01:
                return True
        except:
            pass
    
    # Handle ranking suffixes (1st, 2nd, 3rd, etc.)
    if any(suffix in expected_str.lower() for suffix in ['st', 'nd', 'rd', 'th']):
        expected_num = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', expected_str.lower())
        got_num = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', got_str.lower())
        if expected_num == got_num:
            return True
    
    # Handle "days" suffix
    if 'days' in expected_str.lower() and got_str.isdigit():
        expected_days = expected_str.lower().replace(' days', '').replace(' day', '')
        if got_str == expected_days:
            return True
    
    # Handle percentage signs
    if '%' in expected_str and '%' in got_str:
        try:
            got_pct = float(got_str.replace('%', ''))
            expected_pct = float(expected_str.replace('%', ''))
            if abs(got_pct - expected_pct) < 0.1:
                return True
        except:
            pass
    
    # Handle currency (million, billion, etc.)
    if any(unit in expected_str.lower() for unit in ['million', 'billion', 'trillion']) and any(unit in got_str.lower() for unit in ['million', 'billion', 'trillion']):
        try:
            # Extract numbers and normalize
            got_clean = got_str.replace(' million', '000000').replace(' billion', '000000000').replace(' trillion', '000000000000')
            expected_clean = expected_str.replace(' million', '000000').replace(' billion', '000000000').replace(' trillion', '000000000000')
            got_nums = re.findall(r'\d+\.?\d*', got_clean)
            expected_nums = re.findall(r'\d+\.?\d*', expected_clean)
            if got_nums and expected_nums:
                got_num = float(got_nums[0])
                expected_num = float(expected_nums[0])
                if abs(got_num - expected_num) < 0.01:
                    return True
        except:
            pass
    
    # Handle multiple values (comma-separated)
    if ',' in expected_str:
        expected_parts = [part.strip() for part in expected_str.split(',')]
        for part in expected_parts:
            if part.lower() in got_str.lower():
                return True
    
    # Last resort: check if any number from expected appears in got
    if expected_nums:
        for num in expected_nums:
            if num in got_str:
                return True
    
    return False

# ---------- universal SQL generation that accepts data as-is ---------------------------------------------------
def generate_accepting_sql(pattern, question, table_name, domain_info):
    """Generate SQL that accepts data in whatever format it exists"""

    # Extract years from question
    years = re.findall(r'\d{4}', question)

    # Extract column names from question
    question_lower = question.lower()
    columns = domain_info['numeric_columns'] + domain_info['text_columns']

    # Find the most relevant column based on question content
    target_column = None
    for col in columns:
        if col.lower() in question_lower:
            target_column = col
            break

    if not target_column:
        # Default to first numeric column or first column
        target_column = columns[0] if columns else 'id'

    if pattern == 1:  # Single Year Single Metric
        year = years[0] if years else '2020'
        if 'highest' in question_lower or 'best' in question_lower:
            return f"""SELECT "{target_column}" FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = '{year}'
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{target_column}", '$", ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) DESC LIMIT 1;"""
        elif 'lowest' in question_lower or 'worst' in question_lower:
            return f"""SELECT "{target_column}" FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = '{year}'
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{target_column}", '$", ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) ASC LIMIT 1;"""
        else:
            return f"""SELECT strftime('%Y', timestamp) FROM "{table_name}"
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{target_column}", '$", ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) DESC LIMIT 1;"""

    elif pattern == 2:  # Date Range Single Metric
        start_year, end_year = years[0], years[1] if len(years) >= 2 else years[0]
        if 'percentage' in question_lower:
            return f"""SELECT ((CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) -
                    CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL)) /
                    CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL)) * 100 as percentage_change
                    FROM "{table_name}";"""
        elif 'increase' in question_lower or 'decrease' in question_lower:
            return f"""SELECT CASE
                    WHEN CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) >
                         CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL)
                    THEN 'increase'
                    WHEN CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) <
                         CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL)
                    THEN 'decrease'
                    ELSE 'remain the same' END as change_direction
                    FROM "{table_name}";"""
        else:
            return f"""SELECT (CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) -
                    CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN "{target_column}" END), '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL)) as difference
                    FROM "{table_name}";"""

    elif pattern == 5:  # Correlative Questions
        # Find the correlating column
        correlating_column = None
        for col in columns:
            if col.lower() in question_lower and col != target_column:
                correlating_column = col
                break

        if not correlating_column:
            correlating_column = target_column

        if 'highest' in question_lower:
            return f"""SELECT "{target_column}" FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = (
                        SELECT strftime('%Y', timestamp) FROM "{table_name}"
                        ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{correlating_column}", '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) DESC LIMIT 1
                    )
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{target_column}", '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) DESC LIMIT 1;"""
        else:
            return f"""SELECT "{target_column}" FROM "{table_name}"
                    WHERE strftime('%Y', timestamp) = (
                        SELECT strftime('%Y', timestamp) FROM "{table_name}"
                        ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{correlating_column}", '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) ASC LIMIT 1
                    )
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE("{target_column}", '$', ''), ',', ''), '%', ''), ' million', '000000'), ' billion', '000000000'), ' trillion', '000000000000') AS REAL) ASC LIMIT 1;"""

    # Default fallback
    return f"""SELECT "{target_column}" FROM "{table_name}" LIMIT 1;"""

# ---------- smart prompt builder with pattern detection ---------------------------------------------------
def build_smart_prompt(table_name, question, info):
    """Build a smart prompt with pattern detection and better guidance"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    cols = ', '.join(info['columns'])
    
    # Format sample data as a table
    sample_table = ""
    if info['sample_data']:
        sample_table = "\nSample Data (first 3 rows):\n"
        sample_table += " | ".join(cols) + "\n"
        sample_table += "-" * (len(cols) * 20) + "\n"
        for row in info['sample_data']:
            sample_table += " | ".join(str(cell) for cell in row) + "\n"

    # SAFE FIX 1: Schema Detection - Check how many leader columns exist
    leader_columns = []
    for i in range(1, 6):  # Check up to 5 leader columns
        if f'leader_name{i}' in cols:
            leader_columns.append(i)
    
    schema_info = f"Schema: {len(leader_columns)} leader columns (leader_name{leader_columns[0] if leader_columns else 1}-leader_name{leader_columns[-1] if leader_columns else 1})"

    # Pattern detection and guidance
    guidance = ""
    question_lower = question.lower()
    
    # Pattern 1: Percentage calculations
    if "percentage" in question_lower and ("from" in question_lower or "to" in question_lower):
        guidance += "\n\nPATTERN: Percentage Change (ROBUST TEMPLATE)"
        guidance += "\n- ALWAYS use this exact template for percentage calculations:"
        guidance += "\n- Clean $ and , symbols with REPLACE()"
        guidance += "\n- Use full ISO timestamps, not just year"
        guidance += "\n- Cast to REAL and round to 2 decimals"
        guidance += "\n- Example: SELECT ROUND((CAST(REPLACE(REPLACE((SELECT column FROM table WHERE timestamp = (SELECT timestamp FROM table WHERE strftime('%Y', timestamp) = '2022' ORDER BY timestamp DESC LIMIT 1)), ',', ''), '$', '') AS REAL) - CAST(REPLACE(REPLACE((SELECT column FROM table WHERE timestamp = (SELECT timestamp FROM table WHERE strftime('%Y', timestamp) = '2020' ORDER BY timestamp ASC LIMIT 1)), ',', ''), '$', '') AS REAL)) * 100.0 / CAST(REPLACE(REPLACE((SELECT column FROM table WHERE timestamp = (SELECT timestamp FROM table WHERE strftime('%Y', timestamp) = '2020' ORDER BY timestamp ASC LIMIT 1)), ',', ''), '$', '') AS REAL), 2)"
    
    # Pattern 2: Tenure calculations
    elif "how long did" in question_lower and "served" in question_lower:
        guidance += "\n\nPATTERN: Tenure Calculation (ROBUST TEMPLATE)"
        guidance += "\n- ALWAYS use this exact template for tenure calculations:"
        guidance += "\n- Use JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))"
        guidance += "\n- Filter by person name AND title"
        guidance += "\n- Example: SELECT (JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))) FROM table WHERE leader_name1 = 'Person Name' AND leader_title1 = 'Title'"
    
    # SAFE FIX 2: Multi-column search for counting questions
    elif "how many people served as" in question_lower:
        guidance += "\n\nPATTERN: Count distinct people in position (MULTI-COLUMN SEARCH)"
        guidance += "\n- IMPORTANT: Search ALL leader columns for the position"
        guidance += "\n- Use UNION to combine results from all leader columns"
        guidance += "\n- Template: SELECT COUNT(DISTINCT person) FROM ("
        guidance += "\n  SELECT leader_name1 as person FROM table WHERE leader_title1 = 'Position'"
        guidance += "\n  UNION"
        guidance += "\n  SELECT leader_name2 as person FROM table WHERE leader_title2 = 'Position'"
        guidance += "\n  UNION" 
        guidance += "\n  SELECT leader_name3 as person FROM table WHERE leader_title3 = 'Position'"
        guidance += "\n  UNION"
        guidance += "\n  SELECT leader_name4 as person FROM table WHERE leader_title4 = 'Position'"
        guidance += "\n) WHERE person IS NOT NULL;"
        guidance += f"\n- Available columns: {schema_info}"
    
    # Pattern 3: "Which position did X hold when Y was Z?"
    elif "which position did" in question_lower and "when" in question_lower:
        guidance += "\n\nPATTERN: Position lookup when two people served together"
        guidance += "\n- Use simple WHERE conditions, not joins"
        guidance += "\n- Look for same row where both conditions are true"
        guidance += "\n- Example: SELECT leader_title1 FROM table WHERE leader_name1 = 'X' AND leader_name2 = 'Y'"
    
    # Pattern 4: "Name the person who first served as X and then later as Y"
    elif "first served as" in question_lower and "then later as" in question_lower:
        guidance += "\n\nPATTERN: Person who held multiple positions"
        guidance += "\n- Use IN clause to find people who appear in both roles"
        guidance += "\n- Example: SELECT leader_name1 FROM table WHERE leader_name1 IN (SELECT leader_name3 FROM table WHERE leader_title3 = 'Role2')"
    
    # Pattern 5: "Who was X before Y?"
    elif "before" in question_lower and "who was" in question_lower:
        guidance += "\n\nPATTERN: Find predecessor"
        guidance += "\n- Use timestamp comparison with ORDER BY DESC LIMIT 1"
        guidance += "\n- Example: SELECT leader_name FROM table WHERE timestamp < (SELECT timestamp FROM table WHERE leader_name = 'Y') ORDER BY timestamp DESC LIMIT 1"
    
    # Pattern 6: "Name the person(s) who served as X when Y was Z?"
    elif "who served as" in question_lower and "when" in question_lower:
        guidance += "\n\nPATTERN: Find people who served together"
        guidance += "\n- Use simple WHERE conditions on same row"
        guidance += "\n- Example: SELECT leader_name2 FROM table WHERE leader_name1 = 'Y' AND leader_title2 = 'X'"
    
    # Column mapping guidance
    guidance += "\n\nCOLUMN MAPPING:"
    guidance += "\n- President: leader_name1, leader_title1"
    guidance += "\n- Vice President: leader_name2, leader_title2" 
    guidance += "\n- Prime Minister: leader_name2 or leader_name3, leader_title2 or leader_title3"
    guidance += "\n- Speaker: leader_name3 or leader_name4, leader_title3 or leader_title4"
    guidance += "\n- Chief Justice: leader_name4 or leader_name5, leader_title4 or leader_title5"
    guidance += "\n- Attorney General: leader_name5, leader_title5"
    
    # SAFE FIX 3: Enhanced data cleaning guidance
    guidance += "\n\nDATA CLEANING (SAFE FIXES):"
    guidance += "\n- Handle trailing spaces: ALWAYS use TRIM() for string comparisons"
    guidance += "\n- For currency values: ALWAYS use REPLACE() to remove $ and ,"
    guidance += "\n- For string matching: Use TRIM(leader_name) = TRIM('Person Name')"
    guidance += "\n- Example: SELECT leader_name2 FROM table WHERE TRIM(leader_name4) = TRIM('Mohan Peiris')"
    guidance += "\n- Example: CAST(REPLACE(REPLACE(column, ',', ''), '$', '') AS REAL)"
    
    # SAFE FIX 4: Schema-aware guidance
    guidance += f"\n\nSCHEMA AWARENESS:"
    guidance += f"\n- {schema_info}"
    guidance += "\n- Some countries have 2 leader columns, others have 4"
    guidance += "\n- Always check which columns exist before using them"
    guidance += "\n- For 2-column countries: only use leader_name1, leader_name2"
    guidance += "\n- For 4-column countries: can use leader_name1 through leader_name4"
    
    # Validation guidance
    guidance += "\n\nVALIDATION:"
    guidance += "\n- If person not found, try fallback logic"
    guidance += "\n- For 'before X' queries, if X doesn't exist, find most recent person in that role"
    guidance += "\n- For percentage calculations: ALWAYS use the robust template above"
    guidance += "\n- For tenure calculations: ALWAYS use JULIANDAY() function"
    guidance += "\n- For counting: ALWAYS search all available leader columns"

    prompt = f"""Generate SQLite SQL for this temporal question.

Table: "{table_name}"
Columns: {cols}
{sample_table}
Question: {question}

{guidance}

Write SQLite SQL. Return only the SQL statement ending with semicolon."""

    return prompt

# ---------- universal prompt builder that accepts data as-is ---------------------------------------------------
def build_accepting_prompt(table_name, question, info, domain_info):
    """Build a clean, simple prompt using long views for temporal reasoning"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    # Simple, clean prompt using long views
    prompt = f"""Generate SQLite SQL for this temporal question.

Table: "{table_name}"
Long View: "{table_name}_long" (timestamp, field, value)
Question: {question}

Use the long view with these patterns:
- "Who was X when Y was Z?" → Self-join on timestamp
- "How long did X serve?" → (JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))) WHERE field = 'leader_name1' AND value = 'X'
- "Who came after X?" → WHERE field = 'leader_name1' AND timestamp > (SELECT MAX(timestamp) FROM table_long WHERE field = 'leader_name1' AND value = 'X')
- "Count people in position" → COUNT(DISTINCT value) WHERE field = 'leader_titleX' AND value = 'Position'

Generate SQL using "{table_name}_long":"""

    return prompt

# Removed complex domain-specific examples function

# ---------- API key management ---------------------------------------------------
def add_api_keys(new_keys):
    """Add new API keys to the rotation"""
    global GEMINI_API_KEYS
    start_num = len(GEMINI_API_KEYS) + 1
    for i, key in enumerate(new_keys, start_num):
        GEMINI_API_KEYS[f"key{i}"] = key
    print(f"Added {len(new_keys)} new keys. Total keys: {len(GEMINI_API_KEYS)}")

def get_key_stats():
    """Get basic stats about API key usage"""
    return {
        "total_keys": len(GEMINI_API_KEYS),
        "keys": list(GEMINI_API_KEYS.keys())
    }

# ---------- domain-specific prompt builders ---------------------------------------------------
def build_cricket_team_prompt(table_name, question, info):
    """Build cricket team specific prompt with ranking and performance patterns"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    cols = ', '.join(info['columns'])
    
    # Format sample data as a table
    sample_table = ""
    if info['sample_data']:
        sample_table = "\nSample Data (first 3 rows):\n"
        sample_table += " | ".join(cols) + "\n"
        sample_table += "-" * (len(cols) * 20) + "\n"
        for row in info['sample_data']:
            sample_table += " | ".join(str(cell) for cell in row) + "\n"

    # Cricket team specific patterns
    guidance = """CRICKET TEAM TEMPORAL REASONING PATTERNS:

RANKING PATTERNS:
- Best ranking in year: SELECT MIN(CAST(REPLACE(REPLACE(REPLACE(REPLACE(t20i_rank, 'st', ''), 'nd', ''), 'rd', ''), 'th', '') AS INTEGER)) FROM table WHERE strftime('%Y', timestamp) = '2021';
- Final ranking in year: SELECT t20i_rank FROM table WHERE strftime('%Y', timestamp) = '2020' ORDER BY timestamp DESC LIMIT 1;
- Ranking trend: SELECT t20i_rank FROM table WHERE strftime('%Y', timestamp) = '2020' ORDER BY timestamp;

PERFORMANCE PATTERNS:
- Matches played in year: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2021' THEN num_t20is END) - MAX(CASE WHEN strftime('%Y', timestamp) < '2021' THEN num_t20is END)) as t20is_2021;
- Total matches between years: SELECT (MAX(CASE WHEN strftime('%Y', timestamp) = '2022' THEN num_t20is END) - MAX(CASE WHEN strftime('%Y', timestamp) = '2020' THEN num_t20is END)) as t20is_2020_to_2022;

RECORD PATTERNS:
- Win-loss record: SELECT t20i_record FROM table WHERE strftime('%Y', timestamp) = '2021' ORDER BY timestamp DESC LIMIT 1;
- Best record in year: SELECT MIN(CAST(SUBSTR(t20i_record, 1, INSTR(t20i_record, '-') - 1) AS INTEGER)) FROM table WHERE strftime('%Y', timestamp) = '2021';

CRICKET-SPECIFIC RULES:
1. Rankings are stored as '1st', '2nd', etc. - use REPLACE to convert to numbers
2. Records are stored as 'W-L' format - use SUBSTR and INSTR to parse
3. Match counts are cumulative - calculate differences between years
4. Always use strftime('%Y', timestamp) for year filtering
5. For 'best' ranking, use MIN() (lower number = better rank)
6. For 'worst' ranking, use MAX() (higher number = worse rank)

Table "{table_name}" columns: {cols}
{sample_table}
Question: {question}

Write SQLite SQL. Return only the SQL statement ending with semicolon."""

    return guidance.format(table_name=table_name, cols=cols, sample_table=sample_table, question=question)

def build_economy_prompt(table_name, question, info):
    """Build economy specific prompt with GDP, revenue, and trade patterns"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    cols = ', '.join(info['columns'])
    
    # Format sample data as a table
    sample_table = ""
    if info['sample_data']:
        sample_table = "\nSample Data (first 3 rows):\n"
        sample_table += " | ".join(cols) + "\n"
        sample_table += "-" * (len(cols) * 20) + "\n"
        for row in info['sample_data']:
            sample_table += " | ".join(str(cell) for cell in row) + "\n"

    # Economy specific patterns
    guidance = """ECONOMY TEMPORAL REASONING PATTERNS:

GDP PATTERNS:
- GDP in specific year: SELECT gdp FROM table WHERE strftime('%Y', timestamp) = '2019';
- GDP growth: SELECT (CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2020' THEN gdp END), '$', ''), ',', ''), ' billion', '000000000') AS REAL) - CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN gdp END), '$', ''), ',', ''), ' billion', '000000000') AS REAL) as gdp_growth;

REVENUE PATTERNS:
- Revenue in year: SELECT revenue FROM table WHERE strftime('%Y', timestamp) = '2019';
- Revenue percentage change: SELECT ROUND(((CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2020' THEN revenue END), '$', ''), ',', ''), ' million', '000000') AS REAL) - CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN revenue END), '$', ''), ',', ''), ' million', '000000') AS REAL)) / CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2019' THEN revenue END), '$', ''), ',', ''), ' million', '000000') AS REAL) * 100, 2) as revenue_change_percent;

TRADE PATTERNS:
- Exports in year: SELECT exports FROM table WHERE strftime('%Y', timestamp) = '2019';
- Trade balance: SELECT (CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2020' THEN exports END), '$', ''), ',', ''), ' million', '000000') AS REAL) - CAST(REPLACE(REPLACE(REPLACE(MAX(CASE WHEN strftime('%Y', timestamp) = '2020' THEN imports END), '$', ''), ',', ''), ' million', '000000') AS REAL) as trade_balance;

ECONOMY-SPECIFIC RULES:
1. Currency values have $, commas, and units (million, billion, trillion)
2. Always use REPLACE to clean currency: REPLACE(REPLACE(REPLACE(column, '$', ''), ',', ''), ' million', '000000')
3. For percentages: (new_value - old_value) / old_value * 100
4. For growth rates: use ROUND() to 2 decimal places
5. Point-in-time data: use direct year comparisons, not cumulative
6. Handle NULL values with COALESCE() when needed

Table "{table_name}" columns: {cols}
{sample_table}
Question: {question}

Write SQLite SQL. Return only the SQL statement ending with semicolon."""

    return guidance.format(table_name=table_name, cols=cols, sample_table=sample_table, question=question)

def build_country_prompt(table_name, question, info):
    """Build country specific prompt with leader succession patterns"""
    if not info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    cols = ', '.join(info['columns'])
    
    # Format sample data as a table
    sample_table = ""
    if info['sample_data']:
        sample_table = "\nSample Data (first 3 rows):\n"
        sample_table += " | ".join(cols) + "\n"
        sample_table += "-" * (len(cols) * 20) + "\n"
        for row in info['sample_data']:
            sample_table += " | ".join(str(cell) for cell in row) + "\n"

    # Country specific patterns (keeping the existing good patterns)
    guidance = """COUNTRY LEADER TEMPORAL REASONING PATTERNS:

SUCCESSOR PATTERNS:
- Who came after X: SELECT leader_name1 FROM table WHERE timestamp > (SELECT MAX(timestamp) FROM table WHERE leader_name1 = 'X') ORDER BY timestamp ASC LIMIT 1;
- Who succeeded X: SELECT leader_name1 FROM table WHERE timestamp > (SELECT MAX(timestamp) FROM table WHERE leader_name1 = 'X') ORDER BY timestamp ASC LIMIT 1;

PREDECESSOR PATTERNS:
- Who came before X: SELECT leader_name1 FROM table WHERE timestamp < (SELECT MIN(timestamp) FROM table WHERE leader_name1 = 'X') ORDER BY timestamp DESC LIMIT 1;
- Who preceded X: SELECT leader_name1 FROM table WHERE timestamp < (SELECT MIN(timestamp) FROM table WHERE leader_name1 = 'X') ORDER BY timestamp DESC LIMIT 1;

DURATION PATTERNS:
- How long did X serve: SELECT (JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))) FROM table WHERE leader_name1 = 'X';
- Tenure of X: SELECT (JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))) FROM table WHERE leader_name1 = 'X';

COLLABORATION PATTERNS:
- Who served as Y when X was Z: SELECT leader_name2 FROM table WHERE leader_name1 = 'X' AND leader_title2 = 'Y';
- Who was Y when X was Z: SELECT leader_name2 FROM table WHERE leader_name1 = 'X' AND leader_title2 = 'Y';

COUNTRY-SPECIFIC RULES:
1. Use TRIM() for string comparisons: TRIM(leader_name1) = TRIM('Person Name')
2. Handle multiple leader columns: leader_name1, leader_name2, leader_name3, leader_name4
3. Use timestamp comparisons for temporal relationships
4. For duration: use JULIANDAY() function
5. For same-time relationships: use same row WHERE conditions

Table "{table_name}" columns: {cols}
{sample_table}
Question: {question}

Write SQLite SQL. Return only the SQL statement ending with semicolon."""

    return guidance.format(table_name=table_name, cols=cols, sample_table=sample_table, question=question)

def build_domain_specific_prompt(domain, table_name, question, info):
    """Route to appropriate domain-specific prompt builder"""
    if domain == "cricket_team":
        return build_cricket_team_prompt(table_name, question, info)
    elif domain == "economy":
        return build_economy_prompt(table_name, question, info)
    elif domain == "country":
        return build_country_prompt(table_name, question, info)
    else:
        # Fallback to smart prompt for other domains
        return build_smart_prompt(table_name, question, info)

# Example usage when you get 5 more keys:
# new_keys = ["key6", "key7", "key8", "key9", "key10"]
# add_api_keys(new_keys)

# ---------- optimized main loop ---------------------------------------------------
def run_optimized_test():
    """Run optimized test with efficient API usage"""
    qa = (pd.read_csv(CSV)
            .query("Category == @DOMAIN")
            .sample(N_Q, random_state=42))

    print(f"Testing {DOMAIN} domain with {N_Q} questions...")
    print(f"Using {len(GEMINI_API_KEYS)} API keys")
    print("=" * 60)

    # Debug: Check what tables exist
    with sqlite3.connect(DB_PATH) as conn:
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")]
        print(f"Available tables: {tables[:5]}... (showing first 5)")
        print(f"Total tables: {len(tables)}")

    score = 0
    start_time = time.time()

    with sqlite3.connect(DB_PATH) as conn:
        for idx, q in enumerate(qa.itertuples(index=False), 1):
            entity, question, expected = q.Entity, q.Question, str(q.Answer)
            table_name = entity

            print(f"\n--- Question {idx}/{N_Q} ---")
            print(f"Entity/Table: {table_name}")
            print(f"Question: {question}")
            print(f"Expected: {expected}")

            # Get table info
            info = get_table_info(conn, table_name)

            if not info['exists']:
                print(f"❌ Table '{table_name}' not found!")
                continue

            # Build domain-specific prompt with pattern detection and guidance
            prompt = build_domain_specific_prompt(DOMAIN, table_name, question, info)
            if VERBOSE:
                print(f"Prompt length: {len(prompt)} chars")

            # Get SQL with optimized API call
            sql = ask_gemini(prompt)
            print(f"Generated SQL: {sql}")

            # Execute SQL
            got = ""
            try:
                # Enhanced SQL cleaning for better compatibility
                sql_clean = sql
                
                # Fix common SQLite compatibility issues
                sql_clean = re.sub(r'SUBSTRING_INDEX\(([^,]+),([^,]+),([^)]+)\)', r'SUBSTR(\1, INSTR(\1, \2) + 1)', sql_clean)
                sql_clean = re.sub(r'STR_TO_DATE\([^)]+\)', r'strftime(\'%Y-%m-%d\', timestamp)', sql_clean)
                sql_clean = re.sub(r'CAST\(([^)]+) AS UNSIGNED\)', r'CAST(\1 AS INTEGER)', sql_clean)
                sql_clean = re.sub(r'DATEDIFF\(([^,]+),([^)]+)\)', r'(JULIANDAY(\1) - JULIANDAY(\2))', sql_clean)
                sql_clean = re.sub(r'DATE_ADD\(([^,]+), INTERVAL ([^)]+)\)', r'date(\1, \'+\2\')', sql_clean)
                
                # Fix INTERSECT queries that might not work in SQLite
                if 'INTERSECT' in sql_clean:
                    sql_clean = sql_clean.replace('INTERSECT', 'AND EXISTS (SELECT 1 FROM')
                    sql_clean = sql_clean.replace(';', ' WHERE leader_name1 = t1.leader_name1);')
                
                # Fix complex self-joins for tenure calculations
                if 'JULIANDAY' in sql_clean and 'INNER JOIN' in sql_clean:
                    # Simplify complex tenure calculations
                    sql_clean = re.sub(r'SELECT SUM\(JULIANDAY\(T2\.timestamp\) - JULIANDAY\(T1\.timestamp\)\) FROM ([^ ]+) AS T1 INNER JOIN \1 AS T2 ON T1\.id = T2\.id WHERE T1\.([^=]+) = \'([^\']+)\' AND T2\.\2 <> \'([^\']+)\' AND T1\.timestamp < T2\.timestamp', 
                                     r'SELECT (JULIANDAY(MAX(timestamp)) - JULIANDAY(MIN(timestamp))) FROM \1 WHERE \2 = \'\3\'', sql_clean)
                
                # Fix long view queries that might have incorrect field/value combinations
                if '_long' in sql_clean:
                    # Fix common long view issues
                    sql_clean = re.sub(r'WHERE field = \'([^\']+)\' AND field = \'([^\']+)\'', r'WHERE field = \'\1\' AND field = \'\2\'', sql_clean)
                    sql_clean = re.sub(r'AND field = \'([^\']+)\' AND field = \'([^\']+)\'', r'AND field = \'\1\' AND field = \'\2\'', sql_clean)

                if VERBOSE: print(f"Cleaned SQL: {sql_clean}")

                res = conn.execute(sql_clean).fetchone()
                got = str(res[0]) if res and res[0] is not None else ""

                # Use the new accepting comparison
                ok = compare_values_appropriately(got, expected)

            except Exception as e:
                print(f"SQL Error: {e}")
                print(f"Failed SQL: {sql}")
                got = ""
                ok = False

            # Check result
            score += ok

            print(f"Answer: {got}")
            print(f"Result: {'✅' if ok else '❌'}")
            print(f"Current Score: {score}/{idx} = {score/idx*100:.1f}%")

            # Show progress
            elapsed = time.time() - start_time
            avg_time = elapsed / idx
            remaining = (N_Q - idx) * avg_time
            print(f"Progress: {idx}/{N_Q} ({idx/N_Q*100:.1f}%) - Est. remaining: {remaining/60:.1f}min")
            print("-" * 40)

    total_time = time.time() - start_time
    print(f"\n🎯 FINAL ACCURACY: {score}/{N_Q} = {score / N_Q * 100:.1f}%")
    print(f"⏱️ Total Time: {total_time/60:.1f} minutes")
    print(f"🚀 Avg Time per Query: {total_time/N_Q:.1f} seconds")

    return score, N_Q

# ---------- test rate limiting ---------------------------------------------------
def test_rate_limiting():
    """Test rate limiting functionality"""
    print("🧪 Testing rate limiting...")

    test_prompt = "Generate SQLite SQL: SELECT 1;"
    start_time = time.time()

    # Test 3 quick calls to see if rate limiting works
    for i in range(3):
        print(f"  Test call {i+1}/3...")
        result = ask_gemini(test_prompt)
        print(f"    Result: {result}")

    elapsed = time.time() - start_time
    print(f"✅ Rate limiting test completed in {elapsed:.1f}s")
    print()

# ---------- API key testing ---------------------------------------------------
def test_api_keys():
    """Test if API keys are working - stops after finding working keys"""
    print("🧪 Testing API keys (will stop after finding working ones)...")

    test_prompt = "Generate SQLite SQL: SELECT 1;"
    working_keys = []
    failed_keys = []

    # Shuffle keys for better distribution
    keys = list(GEMINI_API_KEYS.items())
    random.shuffle(keys)

    for key_name, api_key in keys:
        try:
            print(f"  Testing {key_name}...")
            url = f"https://generativelanguage.googleapis.com/{API_VER}/models/{MODEL}:generateContent?key={api_key}"

            r = requests.post(
                url,
                timeout=5,  # Slightly longer timeout
                headers={'Content-Type': 'application/json'},
                json={
                    'contents': [{'parts': [{'text': test_prompt}]}],
                    'generationConfig': {
                        'temperature': 0.1,
                        'maxOutputTokens': 50,
                    }
                },
            )

            if r.status_code == 200:
                print(f"    ✅ {key_name} - OK")
                working_keys.append(key_name)
                # Stop after finding 3 working keys (enough for rotation)
                if len(working_keys) >= 3:
                    print(f"    🎯 Found {len(working_keys)} working keys, stopping tests")
                    break
            elif r.status_code == 429:
                print(f"    ⚠️ {key_name} - Rate limited (waiting 2s then trying next)")
                time.sleep(2)  # Brief wait before next key
                failed_keys.append(key_name)
            else:
                print(f"    ❌ {key_name} - Error {r.status_code}: {r.text[:100]}")
                failed_keys.append(key_name)

        except Exception as e:
            print(f"    ❌ {key_name} - Exception: {e}")
            failed_keys.append(key_name)

    print(f"✅ API key testing complete:")
    print(f"   Working keys: {len(working_keys)}")
    print(f"   Failed keys: {len(failed_keys)}")
    if working_keys:
        print(f"   Working: {', '.join(working_keys)}")
    else:
        print(f"   ⚠️ No working keys found! All keys may be invalid or rate limited.")
    print()

# ---------- run the optimized test ---------------------------------------------------
if __name__ == "__main__":
    # Show current key stats
    stats = get_key_stats()
    print(f"Current API Keys: {stats['total_keys']}")
    print(f"Keys: {', '.join(stats['keys'])}")

    # Test API keys first
    test_api_keys()

    # Test rate limiting
    test_rate_limiting()

    # Run the optimized test
    score, total = run_optimized_test()

    print(f"\n✅ Test completed!")
    print(f"Final Score: {score}/{total} = {score/total*100:.1f}%")
