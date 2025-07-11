#!/usr/bin/env python3
"""
Error Analyzer for SQL Generation Output
Analyzes output files to categorize errors by type and frequency.
"""

import re
import sys
from collections import defaultdict, Counter
from pathlib import Path

def analyze_output_file(file_path):
    """Analyze an output file and categorize errors"""
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Split into questions
    questions = re.split(r'--- Question \d+/\d+ ---', content)
    
    error_types = defaultdict(int)
    sql_errors = defaultdict(int)
    rate_limit_issues = defaultdict(int)
    empty_results = 0
    correct_results = 0
    total_questions = 0
    
    for question in questions[1:]:  # Skip first empty split
        total_questions += 1
        
        # Check for correct results
        if 'Result: ✅' in question:
            correct_results += 1
            continue
            
        # Check for empty results
        if 'Answer: ' in question and 'Answer: \n' in question:
            empty_results += 1
            error_types['Empty Results'] += 1
            
        # Check for SQL errors
        if 'SQL Error:' in question:
            sql_error_match = re.search(r'SQL Error: (.+?)(?:\n|$)', question)
            if sql_error_match:
                error_msg = sql_error_match.group(1).strip()
                sql_errors[error_msg] += 1
                error_types['SQL Errors'] += 1
                
        # Check for rate limiting
        if 'Rate limited with' in question:
            rate_limit_issues['Rate Limited'] += 1
            error_types['Rate Limiting'] += 1
            
        # Check for API errors
        if 'API error' in question:
            api_error_match = re.search(r'API error (\d+)', question)
            if api_error_match:
                error_code = api_error_match.group(1)
                rate_limit_issues[f'API Error {error_code}'] += 1
                error_types['API Errors'] += 1
                
        # Check for table not found
        if 'no such table' in question:
            table_match = re.search(r'no such table: (.+?)(?:\n|$)', question)
            if table_match:
                table_name = table_match.group(1).strip()
                sql_errors[f'Table not found: {table_name}'] += 1
                
        # Check for syntax errors
        if 'syntax error' in question:
            syntax_match = re.search(r'near "([^"]+)": syntax error', question)
            if syntax_match:
                syntax_error = syntax_match.group(1)
                sql_errors[f'Syntax error near: {syntax_error}'] += 1
                
        # Check for fallback SQL usage
        if 'Generating fallback SQL' in question:
            error_types['Fallback SQL Used'] += 1
            
        # Check for wrong answers (not empty, not correct)
        if 'Result: ❌' in question and 'Answer: ' in question:
            answer_match = re.search(r'Answer: (.+?)(?:\n|$)', question)
            if answer_match and answer_match.group(1).strip():
                error_types['Wrong Answer'] += 1
    
    return {
        'total_questions': total_questions,
        'correct_results': correct_results,
        'accuracy': (correct_results / total_questions * 100) if total_questions > 0 else 0,
        'error_types': dict(error_types),
        'sql_errors': dict(sql_errors),
        'rate_limit_issues': dict(rate_limit_issues),
        'empty_results': empty_results
    }

def print_analysis(results):
    """Print formatted analysis results"""
    
    print("=" * 60)
    print("ERROR ANALYSIS REPORT")
    print("=" * 60)
    
    print(f"\n📊 OVERALL STATISTICS:")
    print(f"   Total Questions: {results['total_questions']}")
    print(f"   Correct Results: {results['correct_results']}")
    print(f"   Accuracy: {results['accuracy']:.1f}%")
    print(f"   Empty Results: {results['empty_results']}")
    
    print(f"\n🚨 ERROR TYPES (by frequency):")
    for error_type, count in sorted(results['error_types'].items(), key=lambda x: x[1], reverse=True):
        percentage = (count / results['total_questions']) * 100
        print(f"   {error_type}: {count} ({percentage:.1f}%)")
    
    if results['sql_errors']:
        print(f"\n🔧 SQL ERRORS (by frequency):")
        for error, count in sorted(results['sql_errors'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / results['total_questions']) * 100
            print(f"   {error}: {count} ({percentage:.1f}%)")
    
    if results['rate_limit_issues']:
        print(f"\n⏰ RATE LIMITING ISSUES:")
        for issue, count in sorted(results['rate_limit_issues'].items(), key=lambda x: x[1], reverse=True):
            percentage = (count / results['total_questions']) * 100
            print(f"   {issue}: {count} ({percentage:.1f}%)")
    
    print("\n" + "=" * 60)

def main():
    """Main function to analyze output files"""
    
    if len(sys.argv) < 2:
        print("Usage: python error_analyzer.py <output_file.txt>")
        print("Example: python error_analyzer.py outputs/gov_agencies.txt")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not Path(file_path).exists():
        print(f"Error: File '{file_path}' not found!")
        sys.exit(1)
    
    try:
        results = analyze_output_file(file_path)
        print_analysis(results)
        
        # Save detailed results to file
        output_file = f"{Path(file_path).stem}_error_analysis.txt"
        with open(output_file, 'w') as f:
            f.write("ERROR ANALYSIS DETAILED REPORT\n")
            f.write("=" * 50 + "\n\n")
            f.write(f"File: {file_path}\n")
            f.write(f"Total Questions: {results['total_questions']}\n")
            f.write(f"Correct Results: {results['correct_results']}\n")
            f.write(f"Accuracy: {results['accuracy']:.1f}%\n\n")
            
            f.write("ERROR TYPES:\n")
            for error_type, count in sorted(results['error_types'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"  {error_type}: {count}\n")
            
            f.write("\nSQL ERRORS:\n")
            for error, count in sorted(results['sql_errors'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"  {error}: {count}\n")
                
            f.write("\nRATE LIMITING:\n")
            for issue, count in sorted(results['rate_limit_issues'].items(), key=lambda x: x[1], reverse=True):
                f.write(f"  {issue}: {count}\n")
        
        print(f"\n📄 Detailed report saved to: {output_file}")
        
    except Exception as e:
        print(f"Error analyzing file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 