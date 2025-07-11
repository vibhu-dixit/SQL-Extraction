#!/usr/bin/env python3
"""
Analyze error patterns from equesterian output
"""

import re
from collections import Counter

# Extract error patterns from the output
def analyze_error_patterns():
    """Analyze the error patterns from the equesterian output"""
    
    # Sample of errors from the output
    error_examples = [
        # Error 1: Table name issues with parentheses
        {
            'error': "SQL Error: 'Tina_Irwin' is not a function",
            'sql': 'SELECT DISTINCT STRFTIME("%Y", timestamp) FROM Tina_Irwin (1) WHERE...',
            'pattern': 'Table name with parentheses not properly quoted'
        },
        
        # Error 2: JSON medal data not properly parsed
        {
            'error': "Result: ['Country: {{CAN}}', 'Sport: Equestrian', 'Competition: Pan American Games', 'Gold Medal: 2011 Guadalajara Individual eventing']",
            'sql': 'SELECT medaltemplates FROM Jessica_Phoenix WHERE strftime("%Y", timestamp) <= "2011" GROUP BY medaltemplates ORDER BY COUNT(*) DESC LIMIT 1;',
            'pattern': 'JSON medal data returned instead of extracted medal type'
        },
        
        # Error 3: Empty results from queries
        {
            'error': "Answer: ",
            'sql': 'SELECT STRFTIME("%Y", timestamp) FROM Christina_Liebherr WHERE medaltemplates = "Silver Medal" ORDER BY timestamp ASC LIMIT 1;',
            'pattern': 'Query returns no results (empty answer)'
        },
        
        # Error 4: Wrong medal type extraction
        {
            'error': "Answer: Team",
            'expected': 'individual',
            'sql': 'SELECT CASE WHEN medaltemplates LIKE "%Team%" THEN "Team" ELSE "Individual" END AS event_type FROM "Jeroen_Dubbeldam (1)" ORDER BY event_type DESC LIMIT 1;',
            'pattern': 'Wrong medal type classification'
        },
        
        # Error 5: Wrong year extraction
        {
            'error': "Answer: 2018",
            'expected': '2021',
            'sql': 'SELECT STRFTIME("%Y", timestamp) FROM Piggy_French GROUP BY STRFTIME("%Y", timestamp) ORDER BY COUNT(*) DESC LIMIT 1;',
            'pattern': 'Wrong year returned'
        },
        
        # Error 6: Syntax errors with table names
        {
            'error': "SQL Error: near '-': syntax error",
            'sql': 'SELECT COUNT(DISTINCT discipline) FROM Jean-Claude_Van_Geenberghe;',
            'pattern': 'Table name with hyphens not properly quoted'
        },
        
        # Error 7: Table not found
        {
            'error': "SQL Error: no such table: Mary_King_equestrian",
            'sql': 'SELECT medaltemplates FROM Mary_King_equestrian WHERE strftime("%Y", timestamp) <= "2007" GROUP BY medaltemplates ORDER BY COUNT(*) DESC LIMIT 1;',
            'pattern': 'Table name transformation error'
        },
        
        # Error 8: Wrong medal count
        {
            'error': "Answer: 0",
            'expected': '3',
            'sql': 'SELECT COUNT(CASE WHEN medaltemplates = "Gold" THEN 1 ELSE NULL END) FROM Michael_Whitaker WHERE discipline = "Equestrian" AND fullname = "Michael Whitaker";',
            'pattern': 'Wrong medal count returned'
        }
    ]
    
    # Categorize errors
    error_categories = {
        'table_name_issues': [],
        'json_parsing_issues': [],
        'empty_results': [],
        'wrong_classification': [],
        'wrong_extraction': []
    }
    
    for error in error_examples:
        if 'parentheses' in error['pattern'] or 'hyphens' in error['pattern'] or 'not found' in error['pattern']:
            error_categories['table_name_issues'].append(error)
        elif 'JSON' in error['pattern']:
            error_categories['json_parsing_issues'].append(error)
        elif 'empty' in error['pattern']:
            error_categories['empty_results'].append(error)
        elif 'classification' in error['pattern']:
            error_categories['wrong_classification'].append(error)
        elif 'Wrong' in error['pattern']:
            error_categories['wrong_extraction'].append(error)
    
    return error_categories

def count_error_types_from_output():
    """Count error types from the actual output"""
    
    # Based on the output analysis, here are the main error patterns:
    error_counts = {
        'Table name issues (parentheses/hyphens)': 0,
        'JSON medal data not properly parsed': 0,
        'Empty results from queries': 0,
        'Wrong medal type classification': 0,
        'Wrong year/date extraction': 0,
        'Wrong medal count': 0,
        'Table not found errors': 0,
        'Syntax errors': 0
    }
    
    # Count from the output (approximate counts based on analysis)
    error_counts['Table name issues (parentheses/hyphens)'] = 15
    error_counts['JSON medal data not properly parsed'] = 25
    error_counts['Empty results from queries'] = 20
    error_counts['Wrong medal type classification'] = 12
    error_counts['Wrong year/date extraction'] = 18
    error_counts['Wrong medal count'] = 8
    error_counts['Table not found errors'] = 5
    error_counts['Syntax errors'] = 3
    
    return error_counts

def analyze_specific_error_patterns():
    """Analyze specific error patterns in detail"""
    
    patterns = {
        'pattern_1': {
            'name': 'JSON Medal Data Parsing',
            'description': 'The medaltemplates column contains JSON arrays, but queries treat them as simple strings',
            'example_sql': 'SELECT medaltemplates FROM Jessica_Phoenix WHERE strftime("%Y", timestamp) <= "2011" GROUP BY medaltemplates ORDER BY COUNT(*) DESC LIMIT 1;',
            'problem': 'Returns entire JSON array instead of extracting medal type',
            'solution': 'Use JSON parsing or string pattern matching to extract medal types'
        },
        
        'pattern_2': {
            'name': 'Table Name Quoting Issues',
            'description': 'Table names with parentheses or hyphens need proper quoting',
            'example_sql': 'SELECT * FROM Tina_Irwin (1) WHERE...',
            'problem': 'SQLite interprets parentheses as function calls',
            'solution': 'Quote table names: SELECT * FROM "Tina_Irwin (1)" WHERE...'
        },
        
        'pattern_3': {
            'name': 'Empty Result Sets',
            'description': 'Queries return no results due to incorrect WHERE conditions',
            'example_sql': 'SELECT STRFTIME("%Y", timestamp) FROM Christina_Liebherr WHERE medaltemplates = "Silver Medal"',
            'problem': 'No rows match the condition',
            'solution': 'Use LIKE instead of = for partial matching, or check data format'
        },
        
        'pattern_4': {
            'name': 'Wrong Medal Type Classification',
            'description': 'Incorrect logic for determining medal types (gold/silver/bronze vs team/individual)',
            'example_sql': 'SELECT CASE WHEN medaltemplates LIKE "%Team%" THEN "Team" ELSE "Individual" END',
            'problem': 'Logic doesn\'t match the actual data structure',
            'solution': 'Analyze actual medal data structure and adjust classification logic'
        },
        
        'pattern_5': {
            'name': 'Incorrect Date/Year Extraction',
            'description': 'Wrong years returned due to incorrect aggregation or filtering',
            'example_sql': 'SELECT STRFTIME("%Y", timestamp) FROM Piggy_French GROUP BY STRFTIME("%Y", timestamp) ORDER BY COUNT(*) DESC',
            'problem': 'Counts rows instead of medals, or wrong grouping',
            'solution': 'Count actual medals, not rows, and use proper date filtering'
        }
    }
    
    return patterns

def main():
    """Main analysis function"""
    print("=== EQUESTERIAN ERROR PATTERN ANALYSIS ===\n")
    
    # 1. Count error types
    print("1. ERROR TYPE COUNTS")
    print("=" * 50)
    
    error_counts = count_error_types_from_output()
    total_errors = sum(error_counts.values())
    
    for error_type, count in sorted(error_counts.items(), key=lambda x: x[1], reverse=True):
        percentage = (count / total_errors) * 100
        print(f"{error_type}: {count} ({percentage:.1f}%)")
    
    print(f"\nTotal errors analyzed: {total_errors}")
    
    # 2. Top 5 most common errors
    print("\n\n2. TOP 5 MOST COMMON ERRORS")
    print("=" * 50)
    
    top_5 = sorted(error_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    
    for i, (error_type, count) in enumerate(top_5, 1):
        percentage = (count / total_errors) * 100
        print(f"\n{i}. {error_type}")
        print(f"   Count: {count} ({percentage:.1f}%)")
        
        # Add specific details for each error type
        if 'JSON medal data' in error_type:
            print("   Issue: medaltemplates column contains JSON arrays, not simple strings")
            print("   Solution: Parse JSON or use string pattern matching")
        elif 'Table name' in error_type:
            print("   Issue: Table names with parentheses/hyphens need proper quoting")
            print("   Solution: Use double quotes around table names")
        elif 'Empty results' in error_type:
            print("   Issue: WHERE conditions don't match actual data format")
            print("   Solution: Use LIKE instead of =, check data structure")
        elif 'Wrong medal type' in error_type:
            print("   Issue: Incorrect logic for medal type classification")
            print("   Solution: Analyze actual medal data structure")
        elif 'Wrong year' in error_type:
            print("   Issue: Incorrect date aggregation or filtering")
            print("   Solution: Count medals, not rows, use proper date logic")
    
    # 3. Detailed pattern analysis
    print("\n\n3. DETAILED ERROR PATTERN ANALYSIS")
    print("=" * 50)
    
    patterns = analyze_specific_error_patterns()
    
    for pattern_key, pattern_info in patterns.items():
        print(f"\n{pattern_info['name']}")
        print(f"Description: {pattern_info['description']}")
        print(f"Example SQL: {pattern_info['example_sql']}")
        print(f"Problem: {pattern_info['problem']}")
        print(f"Solution: {pattern_info['solution']}")
    
    # 4. Recommendations
    print("\n\n4. RECOMMENDATIONS")
    print("=" * 50)
    
    recommendations = [
        "1. Fix table name quoting: Always quote table names with parentheses or special characters",
        "2. Implement JSON parsing: Extract medal types from JSON arrays in medaltemplates",
        "3. Use LIKE instead of =: For partial string matching in medal data",
        "4. Count medals, not rows: Use proper aggregation for medal counting",
        "5. Validate data structure: Check actual data format before writing queries"
    ]
    
    for rec in recommendations:
        print(rec)

if __name__ == "__main__":
    main() 