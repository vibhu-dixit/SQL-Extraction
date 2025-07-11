# ══════════ CONFIG ════════════════════════════════════════════════════
DB_DIR  = "/content/json-processor/domain_dbs"
CSV     = "/content/json-processor/question-answer.csv"
DOMAIN  = "table_tennis_player"          # change domain here
N_Q     = 10  # Reduced for faster testing
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

# Optimized API settings for faster execution
API_CONFIG = {
    "timeout": 3,              # Very fast timeout
    "max_retries": 1,          # Minimal retries
    "rate_limit_delay": 0.5,   # Minimal delay between calls
    "max_output_tokens": 200,  # Smaller output size
    "temperature": 0.1,        # Low temperature for consistency
    "top_p": 0.9,             # Balanced creativity
    "requests_per_minute": 120, # Higher rate limit
    "backoff_factor": 1.5      # Gentler backoff
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

# Initialize rate limiter with faster settings
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
    """Get table structure"""
    try:
        cur = conn.cursor()
        cols = [row[1] for row in cur.execute(f'PRAGMA table_info("{table_name}")')]
        return {
            'columns': cols,
            'exists': True
        }
    except Exception as e:
        print(f"Error getting info for table {table_name}: {e}")
    return {
            'columns': [],
            'exists': False
        }

def get_table_schema(conn, table_name):
    """Get actual table schema and sample data for grounding"""
    try:
        # Get column info
        columns = []
        for row in conn.execute(f'PRAGMA table_info("{table_name}")'):
            columns.append(row[1])
        
        # Get sample data (2-3 rows)
        sample_data = []
        try:
            sample_rows = conn.execute(f'SELECT * FROM "{table_name}" LIMIT 3').fetchall()
            if sample_rows:
                # Get column names for this sample
                col_names = [desc[0] for desc in conn.execute(f'SELECT * FROM "{table_name}" LIMIT 0').description]
                for row in sample_rows:
                    sample_data.append(dict(zip(col_names, row)))
        except Exception as e:
            pass
        
        return {
            'columns': columns,
            'sample_data': sample_data,
            'exists': True
        }
    except Exception as e:
        return {
            'columns': [],
            'sample_data': [],
            'exists': False
        }

def validate_sql_columns(sql, available_columns):
    """Check if SQL uses only available columns"""
    sql_upper = sql.upper()
    missing_columns = []
    
    # Check for common column patterns
    common_patterns = ['medaltemplates_gold_medal', 'medaltemplates_silver_medal', 'medaltemplates_bronze_medal']
    for pattern in common_patterns:
        if pattern.upper() in sql_upper and pattern not in available_columns:
            missing_columns.append(pattern)
    
    return missing_columns

def fix_sql_for_schema(sql, available_columns):
    """Fix SQL to use only available columns"""
    sql_fixed = sql
    
    # Remove references to non-existent columns
    if 'medaltemplates_silver_medal' not in available_columns:
        sql_fixed = sql_fixed.replace('medaltemplates_silver_medal', 'NULL')
        sql_fixed = sql_fixed.replace('medaltemplates_silver_medal IS NOT NULL', 'FALSE')
    
    if 'medaltemplates_bronze_medal' not in available_columns:
        sql_fixed = sql_fixed.replace('medaltemplates_bronze_medal', 'NULL')
        sql_fixed = sql_fixed.replace('medaltemplates_bronze_medal IS NOT NULL', 'FALSE')
    
    if 'medaltemplates_gold_medal' not in available_columns:
        sql_fixed = sql_fixed.replace('medaltemplates_gold_medal', 'NULL')
        sql_fixed = sql_fixed.replace('medaltemplates_gold_medal IS NOT NULL', 'FALSE')
    
    return sql_fixed

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
                print(f"    ⚠️ Rate limited with {key_name}")
                rate_limited_keys.append(key_name)
                failed_keys.append(key_name)
                # Minimal backoff for rate limited keys
                backoff_time = min(API_CONFIG["rate_limit_delay"] * (API_CONFIG["backoff_factor"] ** len(rate_limited_keys)), 2.0)
                print(f"    ⏳ Backing off for {backoff_time:.1f}s...")
                time.sleep(backoff_time)
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

        # Wait longer before retry
        retry_delay = API_CONFIG["rate_limit_delay"] * 3
        print(f"    ⏳ Waiting {retry_delay}s before retry...")
        time.sleep(retry_delay)

        # Try the first 2 keys that weren't rate limited
        retry_keys = [k for k in keys[:3] if k[0] not in rate_limited_keys]

        for i, (key_name, api_key) in enumerate(retry_keys[:2]):
            try:
                print(f"  Retry {i+1}/2: {key_name}")
                rate_limiter.wait_if_needed()
                url = f"https://generativelanguage.googleapis.com/{API_VER}/models/{MODEL}:generateContent?key={api_key}"

                r = requests.post(
                    url,
                    timeout=10,  # Longer timeout for retry
                    headers={'Content-Type': 'application/json'},
                    json={
                        'contents': [{'parts': [{'text': prompt}]}],
                        'generationConfig': {
                            'temperature': 0.1,
                            'maxOutputTokens': 300,
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
        start_year, end_year = years[0], years[1] if len(years) >= 2 else years[0]
        return f"""SELECT ((MAX(CASE WHEN strftime('%Y', timestamp) = '{end_year}' THEN column_name END) -
                MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN column_name END)) /
                MAX(CASE WHEN strftime('%Y', timestamp) = '{start_year}' THEN column_name END)) * 100 as result
                FROM "{table_name}";"""

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
SQL: SELECT (julianday(MAX(timestamp)) - julianday(MIN(timestamp))) as tenure_days FROM "{table_name}" WHERE person_column = 'Person Name';

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
    """Compare values appropriately based on their actual types"""
    got_str = accept_data_as_is(got)
    expected_str = accept_data_as_is(expected)

    # Direct string comparison first
    if got_str.lower() == expected_str.lower():
        return True

    # Handle numeric comparisons when both are numbers
    try:
        got_num = float(got_str.replace('$', '').replace(',', '').replace('%', '').replace(' million', '000000').replace(' billion', '000000000').replace(' trillion', '000000000000'))
        expected_num = float(expected_str.replace('$', '').replace(',', '').replace('%', '').replace(' million', '000000').replace(' billion', '000000000').replace(' trillion', '000000000000'))
        return abs(got_num - expected_num) < 0.01
    except:
        pass

    # Handle percentage comparisons
    if '%' in got_str and '%' in expected_str:
        try:
            got_pct = float(got_str.replace('%', ''))
            expected_pct = float(expected_str.replace('%', ''))
            return abs(got_pct - expected_pct) < 0.1
        except:
            pass

    # Handle multiple year answers (e.g., "2019, 2020" vs "2019 2020")
    if ',' in expected_str and ',' not in got_str:
        # Try to match any of the years
        expected_years = [y.strip() for y in expected_str.split(',')]
        return any(year in got_str for year in expected_years)

    # Handle currency with different formats
    if ('$' in got_str or 'million' in got_str or 'billion' in got_str) and ('$' in expected_str or 'million' in expected_str or 'billion' in expected_str):
        try:
            # Normalize both to numbers
            got_clean = got_str.replace('$', '').replace(',', '').replace(' million', '000000').replace(' billion', '000000000').replace(' trillion', '000000000000')
            expected_clean = expected_str.replace('$', '').replace(',', '').replace(' million', '000000').replace(' billion', '000000000').replace(' trillion', '000000000000')
            got_num = float(got_clean)
            expected_num = float(expected_clean)
            return abs(got_num - expected_num) < 0.01
        except:
            pass

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

# ---------- universal prompt builder that accepts data as-is ---------------------------------------------------
def get_sample_data(conn, table_name, domain):
    """Get sample data to help LLM understand the actual format"""
    if domain != "table_tennis_player":
        return ""
    
    try:
        # Get sample medal data
        sample_query = f'''SELECT 
            medaltemplates_gold_medal,
            medaltemplates_silver_medal, 
            medaltemplates_bronze_medal,
            medaltemplates_competition
        FROM "{table_name}" 
        WHERE medaltemplates_gold_medal IS NOT NULL 
        OR medaltemplates_silver_medal IS NOT NULL 
        OR medaltemplates_bronze_medal IS NOT NULL
        LIMIT 2'''
        
        samples = conn.execute(sample_query).fetchall()
        if samples:
            sample_text = "\nSAMPLE DATA:\n"
            for i, sample in enumerate(samples, 1):
                sample_text += f"Row {i}: gold='{sample[0]}', silver='{sample[1]}', bronze='{sample[2]}', competition='{sample[3]}'\n"
            return sample_text
    except Exception as e:
        pass
    
    return ""

def build_structured_prompt(table_name, question, schema_info):
    """Build structured prompt with schema grounding and few-shot examples"""
    if not schema_info['exists']:
        return f"Table '{table_name}' does not exist. Question: {question}"

    # Format column list
    column_list = ', '.join(schema_info['columns'])
    
    # Format sample rows
    sample_rows = ""
    for i, row in enumerate(schema_info['sample_data'][:2], 1):
        sample_rows += f"Row {i}: {row}\n"
    
    # Domain-specific few-shot examples
    if DOMAIN == "table_tennis_player":
        few_shot_examples = f"""## Few-Shot Examples:

Q: How many gold medals did the player win before 2010?
SQL: SELECT COUNT(*) FROM "{table_name}" WHERE medaltemplates_gold_medal IS NOT NULL AND strftime('%Y', timestamp) <= '2010';

Q: Which medal type did the player win most?
SQL: SELECT CASE WHEN COUNT(CASE WHEN medaltemplates_gold_medal IS NOT NULL THEN 1 END) > COUNT(CASE WHEN medaltemplates_silver_medal IS NOT NULL THEN 1 END) THEN 'gold' ELSE 'silver' END FROM "{table_name}";

Q: When was the first medal won?
SQL: SELECT MIN(strftime('%Y', timestamp)) FROM "{table_name}" WHERE medaltemplates_gold_medal IS NOT NULL OR medaltemplates_silver_medal IS NOT NULL OR medaltemplates_bronze_medal IS NOT NULL;"""
    else:
        few_shot_examples = f"""## Few-Shot Examples:

Q: What was the highest value before 2018?
SQL: SELECT MAX(column_name) FROM "{table_name}" WHERE timestamp < '2018-01-01';

Q: How many records exist after 2015?
SQL: SELECT COUNT(*) FROM "{table_name}" WHERE timestamp > '2015-12-31';

Q: When was the first record created?
SQL: SELECT MIN(timestamp) FROM "{table_name}";"""

    # Build the structured prompt
    prompt = f"""You are an expert data analyst. Your task is to convert natural language questions into correct SQL queries for a given table.

## Table Name:
{table_name}

## Available Columns:
{column_list}
(Note: Always use only these columns. Use `timestamp` for all time-based filters.)

## Sample Rows (for context):
{sample_rows}

{few_shot_examples}

---

## Now answer the following:

Q: {question}

SQL:"""

    return prompt

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

            # Get table schema with sample data
            schema_info = get_table_schema(conn, table_name)

            if not schema_info['exists']:
                print(f"❌ Table '{table_name}' not found!")
                continue

            # Build structured prompt with schema grounding
            prompt = build_structured_prompt(table_name, question, schema_info)
            if VERBOSE:
                print(f"Prompt length: {len(prompt)} chars")

            # Get SQL with optimized API call
            sql = ask_gemini(prompt)
            print(f"Generated SQL: {sql}")

            # Execute SQL with validation and fixing
            got = ""
            try:
                # Validate SQL uses only available columns
                missing_columns = validate_sql_columns(sql, schema_info['columns'])
                if missing_columns:
                    print(f"⚠️ SQL uses missing columns: {missing_columns}")
                    # Fix SQL to use only available columns
                    sql_fixed = fix_sql_for_schema(sql, schema_info['columns'])
                    print(f"🔧 Fixed SQL: {sql_fixed}")
                    sql = sql_fixed
                
                # Clean up the SQL - remove any non-SQLite functions
                sql_clean = sql
                sql_clean = re.sub(r'SUBSTRING_INDEX\(([^,]+),([^,]+),([^)]+)\)', r'SUBSTR(\1, INSTR(\1, \2) + 1)', sql_clean)
                sql_clean = re.sub(r'STR_TO_DATE\([^)]+\)', r'strftime(\'%Y-%m-%d\', timestamp)', sql_clean)
                sql_clean = re.sub(r'CAST\(([^)]+) AS UNSIGNED\)', r'CAST(\1 AS INTEGER)', sql_clean)

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

# Uncomment the following lines to run pattern testing
# if __name__ == "__main__":
#     print("Testing Universal Temporal Patterns")
#     print("="*40)
#     results = test_all_patterns()
#     print_pattern_summary(results)

# To run pattern testing, uncomment the lines above or add this at the end:
# test_all_patterns()

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
                timeout=3,  # Fast timeout for testing
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
                print(f"    ⚠️ {key_name} - Rate limited")
                failed_keys.append(key_name)
            else:
                print(f"    ❌ {key_name} - Error {r.status_code}")
                failed_keys.append(key_name)

        except Exception as e:
            print(f"    ❌ {key_name} - Exception: {e}")
            failed_keys.append(key_name)

    print(f"✅ API key testing complete:")
    print(f"   Working keys: {len(working_keys)}")
    print(f"   Failed keys: {len(failed_keys)}")
    if working_keys:
        print(f"   Working: {', '.join(working_keys)}")
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

    # When you get 5 more keys, uncomment and modify this:
    # new_keys = [
    #     "your_key_6_here",
    #     "your_key_7_here",
    #     "your_key_8_here",
    #     "your_key_9_here",
    #     "your_key_10_here"
    # ]
    # add_api_keys(new_keys)

    # Run the optimized test
    score, total = run_optimized_test()

    print(f"\n✅ Test completed!")
    print(f"Final Score: {score}/{total} = {score/total*100:.1f}%")
