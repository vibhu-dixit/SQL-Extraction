#!/usr/bin/env python3
"""
Updated Error Analysis Script for Table Tennis Player Domain
Analyzes the new output.txt file to see what's changed
"""

import re
from collections import defaultdict, Counter
from pathlib import Path

def analyze_updated_errors():
    """Analyze errors from the updated table_tennis_player.txt output"""
    
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
    
    print("🔍 Analyzing Updated Table Tennis Player Errors...")
    print("=" * 60)
    
    for i, block in enumerate(question_blocks[:55], 1):  # Analyze first 55 questions
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
    print(f"📊 UPDATED ERROR ANALYSIS SUMMARY")
    print(f"Total Questions Analyzed: {len(question_blocks[:55])}")
    print(f"Successful Questions: {len([b for b in question_blocks[:55] if 'Result: ✅' in b])}")
    print(f"Failed Questions: {len([b for b in question_blocks[:55] if 'Result: ❌' in b])}")
    print()
    
    print("📈 ERROR CATEGORIES:")
    for category, errors in error_categories.items():
        if errors:
            print(f"  {category.upper()}: {len(errors)} errors")
            if len(errors) <= 5:  # Show details for small categories
                for error in errors:
                    print(f"    Q{error['question_num']}: {error['entity']} - {error['sql_error'][:50] if error['sql_error'] else 'No SQL error'}...")
            print()
    
    # Detailed analysis of most common errors
    print("🔍 DETAILED ANALYSIS OF TOP ERRORS:")
    
    # 1. Wrong answers (most common)
    if error_categories['wrong_answer']:
        print("\n1. WRONG ANSWER PATTERNS:")
        wrong_answers = error_categories['wrong_answer'][:10]
        for error in wrong_answers:
            print(f"   Q{error['question_num']}: Expected '{error['expected']}', Got '{error['answer']}'")
            print(f"      SQL: {error['generated_sql'][:100]}...")
            print()
    
    # 2. Empty result errors
    if error_categories['empty_result']:
        print("\n2. EMPTY RESULT ERRORS:")
        empty_results = error_categories['empty_result'][:5]
        for error in empty_results:
            print(f"   Q{error['question_num']}: {error['entity']}")
            print(f"      SQL: {error['generated_sql'][:100]}...")
            print()
    
    # 3. SQL syntax errors
    if error_categories['sql_syntax_error']:
        print("\n3. SQL SYNTAX ERRORS:")
        syntax_errors = error_categories['sql_syntax_error'][:5]
        for error in syntax_errors:
            print(f"   Q{error['question_num']}: {error['sql_error']}")
            print(f"      SQL: {error['generated_sql'][:100]}...")
            print()
    
    # 4. Table not found errors
    if error_categories['table_not_found']:
        print("\n4. TABLE NOT FOUND ERRORS:")
        table_errors = Counter([e['entity'] for e in error_categories['table_not_found']])
        for table, count in table_errors.most_common(5):
            print(f"   - {table}: {count} times")
    
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

def analyze_sql_patterns():
    """Analyze the SQL patterns being generated"""
    print("\n🔍 SQL GENERATION PATTERNS ANALYSIS:")
    print("=" * 60)
    
    output_file = Path("outputs/table_tennis_player.txt")
    with open(output_file, 'r') as f:
        content = f.read()
    
    # Extract SQL patterns
    sql_patterns = re.findall(r'Generated SQL: (.*?)(?=\n|$)', content, re.DOTALL)
    
    print(f"Total SQL queries analyzed: {len(sql_patterns)}")
    
    # Analyze common patterns
    pattern_counts = Counter()
    
    for sql in sql_patterns:
        sql_clean = sql.strip()
        
        # Count different types of queries
        if 'medaltemplates_gold_medal' in sql_clean:
            pattern_counts['gold_medal_column'] += 1
        if 'medaltemplates_silver_medal' in sql_clean:
            pattern_counts['silver_medal_column'] += 1
        if 'medaltemplates_bronze_medal' in sql_clean:
            pattern_counts['bronze_medal_column'] += 1
        if 'medaltemplates_competition' in sql_clean:
            pattern_counts['competition_column'] += 1
        if 'LIKE' in sql_clean:
            pattern_counts['like_patterns'] += 1
        if 'COUNT' in sql_clean:
            pattern_counts['count_queries'] += 1
        if 'SUM' in sql_clean:
            pattern_counts['sum_queries'] += 1
        if 'CASE' in sql_clean:
            pattern_counts['case_statements'] += 1
    
    print("\n📊 SQL Pattern Analysis:")
    for pattern, count in pattern_counts.most_common():
        print(f"  {pattern}: {count} times")
    
    # Check if LLM is using the new column names
    print(f"\n🎯 KEY FINDINGS:")
    print(f"  - LLM using 'medaltemplates_gold_medal' columns: {pattern_counts['gold_medal_column']}")
    print(f"  - LLM using 'medaltemplates_competition' columns: {pattern_counts['competition_column']}")
    print(f"  - Using LIKE patterns: {pattern_counts['like_patterns']}")
    print(f"  - Using COUNT queries: {pattern_counts['count_queries']}")
    print(f"  - Using SUM queries: {pattern_counts['sum_queries']}")

def compare_with_previous():
    """Compare current results with previous analysis"""
    print("\n📈 COMPARISON WITH PREVIOUS ANALYSIS:")
    print("=" * 60)
    
    print("Previous Analysis (Original):")
    print("  - Success Rate: 7.3% (4/55)")
    print("  - Wrong Answers: 78.4% (40/51)")
    print("  - Empty Results: 19.6% (10/51)")
    print("  - Table Not Found: 2.0% (1/51)")
    print("  - SQL Syntax Errors: 2.0% (1/51)")
    
    print("\nCurrent Analysis (Updated):")
    # We'll calculate this from the actual data
    print("  - Success Rate: TBD")
    print("  - Wrong Answers: TBD")
    print("  - Empty Results: TBD")
    print("  - Table Not Found: TBD")
    print("  - SQL Syntax Errors: TBD")

if __name__ == "__main__":
    # Run the updated analysis
    error_categories = analyze_updated_errors()
    analyze_sql_patterns()
    compare_with_previous()
    
    print("\n🎯 KEY INSIGHTS:")
    print("=" * 60)
    print("1. Check if LLM is now using structured columns (medaltemplates_gold_medal, etc.)")
    print("2. Compare success rates between original and updated approach")
    print("3. Identify if JSON parsing issues are resolved")
    print("4. Analyze if table naming issues persist") 