#!/usr/bin/env python3
"""
Analyze equesterian database errors systematically
"""

import sqlite3
import json
import re
from pathlib import Path

# Database path
DB_PATH = "../db files/equesterian.db"

def analyze_table_structure(table_name):
    """Analyze the structure of a table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get schema
    cursor.execute(f'PRAGMA table_info("{table_name}")')
    columns = cursor.fetchall()
    
    # Get sample data
    cursor.execute(f'SELECT * FROM "{table_name}" LIMIT 3')
    sample_data = cursor.fetchall()
    
    conn.close()
    
    return {
        'table_name': table_name,
        'columns': columns,
        'sample_data': sample_data
    }

def analyze_medal_data(table_name):
    """Analyze medal data structure"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all medal templates
    cursor.execute(f'SELECT medaltemplates FROM "{table_name}" WHERE medaltemplates IS NOT NULL LIMIT 10')
    medal_data = cursor.fetchall()
    
    conn.close()
    
    # Analyze medal patterns
    medal_patterns = []
    for row in medal_data:
        if row[0]:
            try:
                medals = json.loads(row[0])
                medal_patterns.append(medals)
            except:
                medal_patterns.append(row[0])
    
    return medal_patterns

def test_specific_queries():
    """Test specific queries that failed in the output"""
    
    # Test cases from the output
    test_cases = [
        {
            'table': 'Jessica_Phoenix',
            'question': 'Which medal did Jessica_Phoenix won the most as of 2011?',
            'expected': 'gold',
            'sql': 'SELECT medaltemplates FROM Jessica_Phoenix WHERE strftime("%Y", timestamp) <= "2011" GROUP BY medaltemplates ORDER BY COUNT(*) DESC LIMIT 1;'
        },
        {
            'table': 'Piggy_French',
            'question': 'In which year Piggy_French won most number of medals in ?',
            'expected': '2021',
            'sql': 'SELECT STRFTIME("%Y", timestamp) FROM Piggy_French GROUP BY STRFTIME("%Y", timestamp) ORDER BY COUNT(*) DESC LIMIT 1;'
        },
        {
            'table': 'Richard_Meade',
            'question': 'How many more medals did Richard_Meade won in 1966 compared to 1971?',
            'expected': '0',
            'sql': 'SELECT SUM(CASE WHEN STRFTIME("%Y", timestamp) = "1966" THEN 1 ELSE 0 END) - SUM(CASE WHEN STRFTIME("%Y", timestamp) = "1971" THEN 1 ELSE 0 END) FROM Richard_Meade;'
        }
    ]
    
    results = []
    for test in test_cases:
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            
            # Execute the query
            cursor.execute(test['sql'])
            result = cursor.fetchone()
            
            conn.close()
            
            results.append({
                'table': test['table'],
                'question': test['question'],
                'expected': test['expected'],
                'sql': test['sql'],
                'result': result[0] if result else None,
                'success': str(result[0]) == test['expected'] if result else False
            })
            
        except Exception as e:
            results.append({
                'table': test['table'],
                'question': test['question'],
                'expected': test['expected'],
                'sql': test['sql'],
                'result': None,
                'error': str(e),
                'success': False
            })
    
    return results

def analyze_medal_extraction():
    """Analyze how to properly extract medal information from JSON"""
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Test different medal extraction approaches
    test_queries = [
        # Count gold medals
        """
        SELECT COUNT(*) 
        FROM Jessica_Phoenix 
        WHERE medaltemplates LIKE '%Gold Medal%' 
        AND strftime('%Y', timestamp) <= '2011'
        """,
        
        # Count all medals by type
        """
        SELECT 
            CASE 
                WHEN medaltemplates LIKE '%Gold Medal%' THEN 'Gold'
                WHEN medaltemplates LIKE '%Silver Medal%' THEN 'Silver'
                WHEN medaltemplates LIKE '%Bronze Medal%' THEN 'Bronze'
                ELSE 'Other'
            END as medal_type,
            COUNT(*) as count
        FROM Jessica_Phoenix 
        WHERE strftime('%Y', timestamp) <= '2011'
        GROUP BY medal_type
        ORDER BY count DESC
        """,
        
        # Extract year from medal data
        """
        SELECT DISTINCT
            SUBSTR(medaltemplates, INSTR(medaltemplates, '2011'), 4) as year
        FROM Jessica_Phoenix 
        WHERE medaltemplates LIKE '%2011%'
        """
    ]
    
    results = []
    for i, query in enumerate(test_queries):
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            results.append({
                'query_num': i+1,
                'query': query.strip(),
                'result': result,
                'success': True
            })
        except Exception as e:
            results.append({
                'query_num': i+1,
                'query': query.strip(),
                'error': str(e),
                'success': False
            })
    
    conn.close()
    return results

def main():
    """Main analysis function"""
    print("=== EQUESTERIAN DATABASE ERROR ANALYSIS ===\n")
    
    # 1. Analyze table structure
    print("1. TABLE STRUCTURE ANALYSIS")
    print("=" * 50)
    
    sample_table = analyze_table_structure("Jessica_Phoenix")
    print(f"Table: {sample_table['table_name']}")
    print("Columns:")
    for col in sample_table['columns']:
        print(f"  - {col[1]} ({col[2]})")
    
    print("\nSample data:")
    for i, row in enumerate(sample_table['sample_data']):
        print(f"  Row {i+1}: {row}")
    
    # 2. Analyze medal data structure
    print("\n\n2. MEDAL DATA STRUCTURE ANALYSIS")
    print("=" * 50)
    
    medal_patterns = analyze_medal_data("Jessica_Phoenix")
    print("Medal patterns found:")
    for i, pattern in enumerate(medal_patterns[:3]):
        print(f"  Pattern {i+1}: {pattern}")
    
    # 3. Test specific failed queries
    print("\n\n3. SPECIFIC QUERY TESTING")
    print("=" * 50)
    
    test_results = test_specific_queries()
    for result in test_results:
        print(f"\nTable: {result['table']}")
        print(f"Question: {result['question']}")
        print(f"Expected: {result['expected']}")
        print(f"SQL: {result['sql']}")
        print(f"Result: {result['result']}")
        print(f"Success: {result['success']}")
        if 'error' in result:
            print(f"Error: {result['error']}")
    
    # 4. Test medal extraction approaches
    print("\n\n4. MEDAL EXTRACTION TESTING")
    print("=" * 50)
    
    extraction_results = analyze_medal_extraction()
    for result in extraction_results:
        print(f"\nQuery {result['query_num']}:")
        print(f"SQL: {result['query']}")
        if result['success']:
            print(f"Result: {result['result']}")
        else:
            print(f"Error: {result['error']}")

if __name__ == "__main__":
    main() 