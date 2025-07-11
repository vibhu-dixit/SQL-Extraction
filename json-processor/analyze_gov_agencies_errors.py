#!/usr/bin/env python3
"""
Analyze gov_agencies database errors systematically
"""

import re
import sqlite3
from pathlib import Path

def parse_gov_agencies_output(file_path):
    """Parse the gov_agencies.txt output file"""
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Split into individual questions
    questions = re.split(r'--- Question \d+/\d+ ---', content)
    
    results = []
    for i, question in enumerate(questions[1:], 1):  # Skip first empty split
        lines = question.strip().split('\n')
        
        # Extract key information
        entity = None
        question_text = None
        expected = None
        generated_sql = None
        cleaned_sql = None
        answer = None
        result = None
        sql_error = None
        
        for line in lines:
            if line.startswith('Entity/Table:'):
                entity = line.split('Entity/Table:')[1].strip()
            elif line.startswith('Question:'):
                question_text = line.split('Question:')[1].strip()
            elif line.startswith('Expected:'):
                expected = line.split('Expected:')[1].strip()
            elif line.startswith('Generated SQL:'):
                generated_sql = line.split('Generated SQL:')[1].strip()
            elif line.startswith('Cleaned SQL:'):
                cleaned_sql = line.split('Cleaned SQL:')[1].strip()
            elif line.startswith('Answer:'):
                answer = line.split('Answer:')[1].strip()
            elif line.startswith('Result:'):
                result = line.split('Result:')[1].strip()
            elif line.startswith('SQL Error:'):
                sql_error = line.split('SQL Error:')[1].strip()
            elif line.startswith('Failed SQL:'):
                # Get the full failed SQL
                sql_start = question.find('Failed SQL:')
                if sql_start != -1:
                    failed_sql = question[sql_start:].split('\n', 1)[1].split('\nAnswer:')[0].strip()
                    sql_error = failed_sql
        
        results.append({
            'question_num': i,
            'entity': entity,
            'question': question_text,
            'expected': expected,
            'generated_sql': generated_sql,
            'cleaned_sql': cleaned_sql,
            'answer': answer,
            'result': result,
            'sql_error': sql_error
        })
    
    return results

def analyze_errors(results):
    """Analyze the types of errors"""
    
    # Categorize errors
    error_categories = {
        'sql_syntax_errors': [],
        'table_not_found': [],
        'empty_results': [],
        'wrong_answers': [],
        'truncated_sql': [],
        'other_errors': []
    }
    
    for result in results:
        if result['result'] == '❌':
            if result['sql_error']:
                if 'no such table' in result['sql_error'].lower():
                    error_categories['table_not_found'].append(result)
                elif 'unrecognized token' in result['sql_error'].lower():
                    error_categories['sql_syntax_errors'].append(result)
                elif 'truncated' in result['sql_error'].lower() or len(result['cleaned_sql'] or '') > 500:
                    error_categories['truncated_sql'].append(result)
                else:
                    error_categories['sql_syntax_errors'].append(result)
            elif result['answer'] == '':
                error_categories['empty_results'].append(result)
            else:
                error_categories['wrong_answers'].append(result)
        else:
            error_categories['other_errors'].append(result)
    
    return error_categories

def analyze_table_issues():
    """Analyze specific table-related issues"""
    
    # Connect to the database
    try:
        conn = sqlite3.connect('../domain_dbs/gov_agencies.db')
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        print(f"Available tables: {tables}")
        
        # Check table structures
        table_structures = {}
        for table in tables:
            cursor.execute(f"PRAGMA table_info('{table}')")
            columns = cursor.fetchall()
            table_structures[table] = columns
            
            # Get sample data
            cursor.execute(f"SELECT * FROM '{table}' LIMIT 2")
            sample_data = cursor.fetchall()
            
            print(f"\nTable: {table}")
            print(f"Columns: {[col[1] for col in columns]}")
            print(f"Sample data: {sample_data}")
        
        conn.close()
        return table_structures
        
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def main():
    """Main analysis function"""
    print("=== GOV_AGENCIES DATABASE ERROR ANALYSIS ===\n")
    
    # Parse the output file
    results = parse_gov_agencies_output('outputs/gov_agencies.txt')
    
    print(f"Total questions analyzed: {len(results)}")
    
    # Count success/failure
    successful = sum(1 for r in results if r['result'] == '✅')
    failed = sum(1 for r in results if r['result'] == '❌')
    
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print(f"Success rate: {successful/len(results)*100:.1f}%\n")
    
    # Analyze errors
    error_categories = analyze_errors(results)
    
    print("ERROR CATEGORIES:")
    print("=" * 50)
    
    for category, errors in error_categories.items():
        if errors:
            print(f"\n{category.upper().replace('_', ' ')}: {len(errors)} errors")
            print("-" * 30)
            
            for error in errors[:3]:  # Show first 3 examples
                print(f"Q{error['question_num']}: {error['question'][:80]}...")
                if error['sql_error']:
                    print(f"  SQL Error: {error['sql_error'][:100]}...")
                elif error['answer'] == '':
                    print(f"  Empty result")
                else:
                    print(f"  Expected: {error['expected']}, Got: {error['answer']}")
    
    # Analyze table issues
    print("\n\nTABLE STRUCTURE ANALYSIS:")
    print("=" * 50)
    table_structures = analyze_table_issues()
    
    # Specific error patterns
    print("\n\nSPECIFIC ERROR PATTERNS:")
    print("=" * 50)
    
    # Check for common SQL issues
    sql_errors = [r for r in results if r['sql_error']]
    if sql_errors:
        print(f"\nSQL Errors found: {len(sql_errors)}")
        for error in sql_errors[:5]:
            print(f"\nQ{error['question_num']}: {error['question'][:60]}...")
            print(f"Error: {error['sql_error'][:100]}...")
    
    # Check for empty results
    empty_results = [r for r in results if r['result'] == '❌' and r['answer'] == '']
    if empty_results:
        print(f"\nEmpty Results: {len(empty_results)}")
        for error in empty_results[:3]:
            print(f"\nQ{error['question_num']}: {error['question'][:60]}...")
            print(f"SQL: {error['cleaned_sql'][:100]}...")

if __name__ == "__main__":
    main() 