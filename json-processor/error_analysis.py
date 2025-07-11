#!/usr/bin/env python3
"""
Error Analysis Script for Table Tennis Player Domain
Analyzes the output.txt file to categorize and count error types
"""

import re
from collections import defaultdict, Counter
from pathlib import Path

def analyze_table_tennis_errors():
    """Analyze errors from table_tennis_player.txt output"""
    
    output_file = Path("outputs/table_tennis_player.txt")
    
    if not output_file.exists():
        print(f"❌ Output file not found: {output_file}")
        return
    
    with open(output_file, 'r') as f:
        content = f.read()
    
    # Extract all question blocks
    question_blocks = re.findall(r'--- Question \d+/\d+ ---(.*?)(?=--- Question \d+/\d+ ---|$)', content, re.DOTALL)
    
    error_categories = {
        'table_not_found': [],
        'sql_syntax_error': [],
        'data_mismatch': [],
        'empty_result': [],
        'wrong_answer': [],
        'api_error': [],
        'other': []
    }
    
    print("🔍 Analyzing Table Tennis Player Errors...")
    print("=" * 60)
    
    for i, block in enumerate(question_blocks[:100], 1):  # Analyze first 100 questions
        lines = block.strip().split('\n')
        
        # Extract key information
        entity = ""
        question = ""
        expected = ""
        generated_sql = ""
        cleaned_sql = ""
        sql_error = ""
        answer = ""
        result = ""
        
        for line in lines:
            if line.startswith("Entity/Table:"):
                entity = line.split("Entity/Table:")[1].strip()
            elif line.startswith("Question:"):
                question = line.split("Question:")[1].strip()
            elif line.startswith("Expected:"):
                expected = line.split("Expected:")[1].strip()
            elif line.startswith("Generated SQL:"):
                generated_sql = line.split("Generated SQL:")[1].strip()
            elif line.startswith("Cleaned SQL:"):
                cleaned_sql = line.split("Cleaned SQL:")[1].strip()
            elif line.startswith("SQL Error:"):
                sql_error = line.split("SQL Error:")[1].strip()
            elif line.startswith("Answer:"):
                answer = line.split("Answer:")[1].strip()
            elif line.startswith("Result:"):
                result = line.split("Result:")[1].strip()
        
        # Categorize the error
        error_type = categorize_error(sql_error, answer, expected, result, entity)
        error_categories[error_type].append({
            'question_num': i,
            'entity': entity,
            'question': question,
            'expected': expected,
            'generated_sql': generated_sql,
            'sql_error': sql_error,
            'answer': answer,
            'result': result
        })
    
    # Print summary
    print(f"📊 ERROR ANALYSIS SUMMARY")
    print(f"Total Questions Analyzed: {len(question_blocks[:100])}")
    print(f"Successful Questions: {len([b for b in question_blocks[:100] if 'Result: ✅' in b])}")
    print(f"Failed Questions: {len([b for b in question_blocks[:100] if 'Result: ❌' in b])}")
    print()
    
    print("📈 ERROR CATEGORIES:")
    for category, errors in error_categories.items():
        if errors:
            print(f"  {category.upper()}: {len(errors)} errors")
            if len(errors) <= 5:  # Show details for small categories
                for error in errors:
                    print(f"    Q{error['question_num']}: {error['entity']} - {error['sql_error'][:50]}...")
            print()
    
    # Detailed analysis of most common errors
    print("🔍 DETAILED ANALYSIS OF TOP ERRORS:")
    
    # 1. Table not found errors
    if error_categories['table_not_found']:
        print("\n1. TABLE NOT FOUND ERRORS:")
        table_errors = Counter([e['entity'] for e in error_categories['table_not_found']])
        for table, count in table_errors.most_common(5):
            print(f"   - {table}: {count} times")
    
    # 2. Data mismatch errors (wrong answers)
    if error_categories['wrong_answer']:
        print("\n2. WRONG ANSWER PATTERNS:")
        wrong_answers = error_categories['wrong_answer'][:5]
        for error in wrong_answers:
            print(f"   Q{error['question_num']}: Expected '{error['expected']}', Got '{error['answer']}'")
            print(f"      SQL: {error['generated_sql'][:100]}...")
            print()
    
    # 3. Empty result errors
    if error_categories['empty_result']:
        print("\n3. EMPTY RESULT ERRORS:")
        empty_results = error_categories['empty_result'][:5]
        for error in empty_results:
            print(f"   Q{error['question_num']}: {error['entity']}")
            print(f"      SQL: {error['generated_sql'][:100]}...")
            print()
    
    # 4. SQL syntax errors
    if error_categories['sql_syntax_error']:
        print("\n4. SQL SYNTAX ERRORS:")
        syntax_errors = error_categories['sql_syntax_error'][:5]
        for error in syntax_errors:
            print(f"   Q{error['question_num']}: {error['sql_error']}")
            print(f"      SQL: {error['generated_sql'][:100]}...")
            print()
    
    return error_categories

def categorize_error(sql_error, answer, expected, result, entity):
    """Categorize the type of error"""
    
    # Table not found
    if "no such table" in sql_error.lower():
        return 'table_not_found'
    
    # SQL syntax errors
    if any(keyword in sql_error.lower() for keyword in [
        'syntax error', 'incomplete input', 'unrecognized token', 
        'misuse of aggregate', 'ambiguous column', 'no such column'
    ]):
        return 'sql_syntax_error'
    
    # Empty results
    if answer.strip() == "" or answer.strip() == "None":
        return 'empty_result'
    
    # Wrong answers (when we got an answer but it's wrong)
    if answer.strip() and expected.strip() and answer.strip() != expected.strip():
        return 'wrong_answer'
    
    # API errors
    if "API error" in sql_error:
        return 'api_error'
    
    return 'other'

def analyze_database_issues():
    """Analyze potential database structure issues"""
    print("\n🔍 DATABASE STRUCTURE ANALYSIS:")
    print("=" * 60)
    
    # Check if tables exist in the database
    import sqlite3
    db_path = Path("domain_dbs/table_tennis_player.db")
    
    if not db_path.exists():
        print(f"❌ Database not found: {db_path}")
        return
    
    with sqlite3.connect(db_path) as conn:
        # Get all tables
        tables = [row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
        print(f"📋 Total tables in database: {len(tables)}")
        
        # Check for problematic table names
        problematic_tables = []
        for table in tables:
            if '(' in table or ')' in table:
                problematic_tables.append(table)
        
        if problematic_tables:
            print(f"\n⚠️  Tables with parentheses in names (potential SQL issues):")
            for table in problematic_tables[:10]:
                print(f"   - {table}")
        
        # Check table sizes
        print(f"\n📊 Sample table sizes:")
        for table in tables[:5]:
            try:
                count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                print(f"   - {table}: {count} rows")
            except Exception as e:
                print(f"   - {table}: ERROR - {e}")
        
        # Check column structure for a few tables
        print(f"\n🏗️  Sample table structures:")
        for table in tables[:3]:
            try:
                columns = [row[1] for row in conn.execute(f'PRAGMA table_info("{table}")')]
                print(f"   - {table}: {columns}")
            except Exception as e:
                print(f"   - {table}: ERROR - {e}")

def analyze_llm_issues():
    """Analyze potential LLM/SQL generation issues"""
    print("\n🤖 LLM/SQL GENERATION ANALYSIS:")
    print("=" * 60)
    
    print("Common LLM Issues Identified:")
    print("1. Table name handling with parentheses")
    print("2. Incorrect column references")
    print("3. Wrong aggregation functions")
    print("4. Incomplete SQL generation")
    print("5. Misunderstanding of data structure")
    
    print("\nPotential Solutions:")
    print("1. Better table name escaping in prompts")
    print("2. Provide schema information to LLM")
    print("3. Improve SQL validation and cleaning")
    print("4. Better error handling for malformed SQL")
    print("5. Domain-specific SQL templates")

if __name__ == "__main__":
    # Run the analysis
    error_categories = analyze_table_tennis_errors()
    analyze_database_issues()
    analyze_llm_issues()
    
    print("\n🎯 RECOMMENDATIONS:")
    print("=" * 60)
    print("1. Database Issues: Check table naming conventions")
    print("2. LLM Issues: Improve prompt engineering with schema info")
    print("3. SQL Issues: Better validation and error handling")
    print("4. Data Issues: Verify expected answers match database content") 