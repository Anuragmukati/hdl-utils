import re
import sys
import os
import csv
import argparse
from pathlib import Path

# Define table name pattern globally
# Updated to handle both backticks and double quotes for quoted identifiers
# Supports multi-part identifiers like "schema"."table" or schema.table
table_name_pattern = r'(?:"[^"]+"\."[^"]+"|`[^`]+`\.`[^`]+`|"[^"]+"|`[^`]+`|[\w\&\-\<\>\$\#\@\{\}\[\]]+(?:\.[\w\&\-\<\>\$\#\@\{\}\[\]]+)*)'

def extract_table_name_from_line(line, pattern, obj_type):
    """
    Extract the actual table name from a line using the regex pattern.
    Also determine if the table is a source or target.
    """
    match = pattern.search(line)
    if not match:
        return None, None
    
    # Special handling for JOIN patterns
    if 'join' in obj_type:
        # For JOIN, extract the table name after JOIN keyword
        join_pattern = r'(?:INNER\s+|RIGHT\s+|LEFT\s+|FULL\s+|CROSS\s+)?(?:OUTER\s+)?JOIN\s+(' + table_name_pattern + r')'
        join_match = re.search(join_pattern, line, re.IGNORECASE)
        if join_match:
            table_name = join_match.group(1).strip()
        else:
            table_name = None
    else:
        # Get the table name from the match groups
        table_name = None
        for group in match.groups():
            if group and ('.' in group or '`' in group or any(c.isalnum() for c in group)):
                table_name = group.strip()
                break
        
        # If no table name found in groups, try to extract from the full match
        if not table_name:
            full_match = match.group(0)
            # Extract table name after keywords
            table_extraction_pattern = r'(?:FROM|INTO|UPDATE|DELETE\s+FROM|CREATE\s+(?:OR\s+REPLACE\s+)?(?:EXTERNAL\s+|TEMP\s+)?(?:TABLE|VIEW)|DROP\s+(?:TABLE|VIEW)|TRUNCATE\s+TABLE|MERGE\s+(?:INTO\s+)?|USING|DESCRIBE|DESC)\s+(' + table_name_pattern + r')'
            table_match = re.search(table_extraction_pattern, full_match, re.IGNORECASE)
            if table_match:
                table_name = table_match.group(1)
    
    # Clean up table name (remove AS alias)
    if table_name:
        table_name = table_name.split()[0]  # Remove alias if present
        table_name = table_name.rstrip(',')  # Remove trailing comma
        
        # Remove backticks and double quotes from table name while preserving the structure
        # Handle both "schema"."table" and `schema`.`table` formats
        if '"' in table_name:
            # Replace "schema"."table" with schema.table
            table_name = table_name.replace('"', '')
        elif '`' in table_name:
            # Replace `schema`.`table` with schema.table
            table_name = table_name.replace('`', '')
        
        # Filter out SQL functions and false positives
        sql_functions = {
            'CURRENT_DATE', 'CURRENT_TIME', 'CURRENT_TIMESTAMP', 'GETDATE', 'NOW',
            'DATE_ADD', 'DATE_SUB', 'DATE_DIFF', 'EXTRACT', 'DATEPART', 'DATEDIFF',
            'COALESCE', 'CASE', 'CAST', 'CONVERT', 'SUBSTRING', 'SUBSTR', 'LENGTH',
            'CONCAT', 'UPPER', 'LOWER', 'TRIM', 'LTRIM', 'RTRIM', 'COUNT', 'SUM',
            'AVG', 'MIN', 'MAX', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD'
        }
        
        # Check if the detected "table name" is actually a SQL function
        if table_name.upper() in sql_functions:
            return None, None
            
        # Check if this FROM appears in an EXTRACT function context
        # Look for EXTRACT(...FROM table_name) pattern
        extract_pattern = r'\bEXTRACT\s*\(\s*\w+\s+FROM\s+' + re.escape(table_name) + r'\b'
        if re.search(extract_pattern, line, re.IGNORECASE):
            return None, None
            
        # Check for other function contexts like DATE_ADD(DATE_SUB(...FROM table_name))
        # Look for common date function patterns that contain FROM
        date_function_pattern = r'\b(?:DATE_ADD|DATE_SUB|DATEADD|DATESUB)\s*\([^)]*FROM\s+' + re.escape(table_name) + r'\b'
        if re.search(date_function_pattern, line, re.IGNORECASE):
            return None, None
    
    # Determine if it's source or target based on operation type
    target_operations = {
        'insert into', 'delete from', 'update', 'create table', 'create view', 
        'create external table', 'create temp table', 'create external view', 
        'create temp view', 'drop table', 'drop view', 'alter table', 
        'truncate table', 'merge', 'into table', 'create procedure', 
        'create function', 'create package', 'create macro'
    }
    
    source_operations = {
        'select from', 'join', 'merge using'
    }
    
    if obj_type in target_operations:
        category = 'target'
    elif obj_type in source_operations:
        category = 'source'
    else:
        category = 'utility'  # For operations like DESCRIBE, SHOW, etc.
    
    return table_name, category

def is_comment_line(line):
    """
    Enhanced comment detection that handles various comment formats.
    Returns True if the line should be considered a comment and skipped.
    """
    if not line:
        return False
    
    # Check for standard comment prefixes
    if line.startswith('--') or line.startswith('/*') or line.startswith('#'):
        return True
    
    # Check for comments that start with common comment keywords
    comment_keywords = [
        'revision notes:',
        'author:',
        'description:',
        'purpose:',
        'notes:',
        'comment:',
        'created by:',
        'modified by:',
        'date:',
        'version:',
        'todo:',
        'fixme:',
        'bug:',
        'issue:',
        '/**',
        '*/',
        'rem ',  # BATCH comment
        'echo off',  # BATCH comment
        'echo on',   # BATCH comment
    ]
    
    line_lower = line.lower()
    for keyword in comment_keywords:
        if line_lower.startswith(keyword):
            return True
    
    # Check for lines that look like revision/documentation comments
    # (often start with descriptive text without SQL keywords)
    if re.match(r'^\s*[A-Za-z][^:]*:\s*[A-Za-z]', line):
        # Looks like "Label: Description" format
        return True
    
    # Check for lines that contain only documentation-style text
    # (no typical SQL keywords but contains descriptive words)
    if not re.search(r'\b(SELECT|FROM|WHERE|INSERT|UPDATE|DELETE|CREATE|DROP|ALTER|MERGE|WITH|JOIN|UNION|TRUNCATE)\b', line, re.IGNORECASE):
        # If it contains words like "updated", "pull", "version" without SQL context, it's likely a comment
        doc_indicators = ['updated', 'pull', 'version', 'notes', 'revision', 'change', 'fix', 'bug', 'issue']
        if any(word in line_lower for word in doc_indicators):
            return True
    
    return False

def should_skip_line(line, all_lines, line_index):
    """
    Check if a line should be skipped due to being part of SQL function contexts
    that might have false positive table detections.
    """
    line_upper = line.upper()
    
    # Skip lines that contain CURRENT_DATE function calls (even if nested)
    if 'CURRENT_DATE(' in line_upper:
        return True
    
    # Skip lines that look like they are part of multi-line EXTRACT functions
    # Pattern: "FROM column_name) [operator] [value]" or just "column_name) [operator]"
    multiline_extract_pattern = r'^\s*(?:FROM\s+)?[\w_]+\s*\)\s*[><=!]'
    if re.search(multiline_extract_pattern, line_upper):
        return True
    
    # Skip lines that are part of multi-line DATE function calls
    # Look for patterns like ") DAY), INTERVAL" which are typical in nested date functions
    nested_date_pattern = r'\)\s*DAY\s*\)\s*,\s*INTERVAL'
    if re.search(nested_date_pattern, line_upper):
        return True
    
    # Skip lines that contain RAISE USING MESSAGE patterns
    if 'RAISE USING MESSAGE' in line_upper or line.strip().startswith('MESSAGE'):
        return True
    
    # Check if the previous line contains EXTRACT or DATE functions
    # to catch multi-line function calls
    if line_index > 0:
        prev_line = all_lines[line_index - 1].upper()
        if ('EXTRACT(' in prev_line or 'DATE_ADD(' in prev_line or 'DATE_SUB(' in prev_line) and 'FROM' in line_upper:
            # This might be a continuation of a function call
            return True
            
        # Also check for lines that are continuation of EXTRACT with column names
        if 'EXTRACT(' in prev_line and re.search(r'^\s*[\w_]+\s*\)', line_upper):
            return True
    
    return False

def extract_db_objects_from_sql(file_path, save_output=False):
    """
    Read a SQL file and extract lines that contain database objects like tables, views, procedures, functions.
    """
    
    # Patterns to match database objects (case-insensitive)
    # Enhanced with patterns from SqlParser.py for better coverage
    
    pattern_types = [
        # DELETE patterns (must come before FROM)
        (r'\bDELETE\s+FROM\s+' + table_name_pattern, 'delete from'),
        (r'(\bDELETE|\bDEL)\s+' + table_name_pattern, 'delete from'),  # Alternative DELETE syntax
        
        # INSERT patterns
        (r'\b(?:INSERT|INS)\s+(?:INTO|OVERWRITE)\s*(?:TABLE)?\s*' + table_name_pattern, 'insert into'),
        (r'\bINSERT\s+INTO\s+' + table_name_pattern, 'insert into'),
        
        # CREATE TABLE patterns (enhanced) - must come before SELECT FROM
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SET|MULTISET|GLOBAL)?\s*(?:TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(' + table_name_pattern + r')', 'create table'),
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:WRITABLE\s+)?(?:EXTERNAL)\s+(?:WEB\s+)?(?:TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(' + table_name_pattern + r')', 'create external table'),
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:TEMPORARY|TEMP)\s+(?:TABLE)\s+(?:IF\s+NOT\s+EXISTS\s+)?(' + table_name_pattern + r')', 'create temp table'),
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SET|MULTISET|VOLATILE|GLOBAL)?\s*(?:TEMPORARY|VOLATILE|TEMP)\s+(?:TABLE)(?:\s+IF\s+NOT\s+EXISTS\s*)?\s*(' + table_name_pattern + r')', 'create temp table'),
        
        # CREATE VIEW patterns (enhanced) - must come before SELECT FROM
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SET|MULTISET|GLOBAL)?\s*(?:VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(' + table_name_pattern + r')', 'create view'),
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:WRITABLE\s+)?(?:EXTERNAL)\s+(?:WEB\s+)?(?:VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?(' + table_name_pattern + r')', 'create external view'),
        (r'CREATE\s+(?:OR\s+REPLACE\s+)?(?:SET|MULTISET|VOLATILE|TEMPORARY|TEMP|GLOBAL)?\s*(?:TEMPORARY|VOLATILE|TEMP)\s+(?:VIEW)(?:\s+IF\s+NOT\s+EXISTS\s*)?\s*(' + table_name_pattern + r')', 'create temp view'),
        (r'(?:CREATE\s+OR\s+REPLACE|CREATE|REPLACE)\s+(?:FORCE)?\s*(?:RECURSIVE|TEMPORARY|MATERIALIZED|EDITIONABLE|NONEDITIONABLE)?\s*(?:VIEW)\s+(' + table_name_pattern + r')', 'create view'),
        
        # SELECT FROM pattern - moved after CREATE patterns
        (r'\bFROM\s+' + table_name_pattern, 'select from'),
        
        # JOIN patterns
        (r'((INNER|RIGHT|LEFT|FULL|CROSS)\s+)?(OUTER\s+)?\bJOIN\s+' + table_name_pattern, 'join'),
        (r'\bJOIN\s+' + table_name_pattern, 'join'),  # Fallback JOIN pattern
        
        # UPDATE patterns (exclude MERGE statement UPDATE clauses)
        (r'(?<!THEN\s)\bUPDATE\s+' + table_name_pattern, 'update'),
        
        # CREATE PROCEDURE/FUNCTION patterns
        (r'(?:CREATE\s+OR\s+REPLACE|CREATE|REPLACE)\s+(?:EDITIONABLE|NONEDITIONABLE)?\s*PROCEDURE\s+' + table_name_pattern, 'create procedure'),
        (r'(?:CREATE\s+OR\s+REPLACE|CREATE|REPLACE)\s+(?:EDITIONABLE|NONEDITIONABLE)?\s*FUNCTION\s+' + table_name_pattern, 'create function'),
        (r'(?:CREATE\s+OR\s+REPLACE|CREATE|REPLACE)\s+(?:EDITIONABLE|NONEDITIONABLE)?\s*PACKAGE\s+(?:BODY)?\s*' + table_name_pattern, 'create package'),
        (r'(?:CREATE\s+OR\s+REPLACE|CREATE|REPLACE)\s+(?:EDITIONABLE|NONEDITIONABLE)?\s*MACRO\s+' + table_name_pattern, 'create macro'),
        
        # DROP patterns
        (r'DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?(' + table_name_pattern + r')', 'drop table'),
        (r'DROP\s+VIEW\s+(?:IF\s+EXISTS\s+)?(' + table_name_pattern + r')', 'drop view'),
        (r'DROP\s+PROCEDURE\s+(?:IF\s+EXISTS\s+)?(' + table_name_pattern + r')', 'drop procedure'),
        (r'DROP\s+FUNCTION\s+(?:IF\s+EXISTS\s+)?(' + table_name_pattern + r')', 'drop function'),
        
        # ALTER patterns
        (r'(?:ALTER)\s+(?:TABLE)\s+' + table_name_pattern, 'alter table'),
        
        # TRUNCATE patterns
        (r'TRUNCATE\s+TABLE\s+' + table_name_pattern, 'truncate table'),
        
        # MERGE patterns (enhanced)
        (r'MERGE\s+(?:INTO\s+)?(' + table_name_pattern + r')', 'merge'),
        (r'\bUSING\s+(' + table_name_pattern + r')', 'merge using'),
        
        # EXECUTE patterns
        (r'\bEXEC\s+' + table_name_pattern, 'exec procedure'),
        (r'\bEXECUTE\s+' + table_name_pattern, 'execute procedure'),
        (r'\bCALL\s+' + table_name_pattern, 'call procedure/function'),
        
        # Utility patterns
        (r'\b(?:DESCRIBE|DESC)\s+(?:FORMATTED)?\s*' + table_name_pattern, 'describe'),
        (r'\bSHOW\s+TABLE\s+STATS\s*' + table_name_pattern, 'show table'),
        (r'\bMSCK\s+REPAIR\s+TABLE\s*' + table_name_pattern, 'msck repair'),
        (r'LOAD\s+DATA\s+INPATH\s+.+\s+INTO\s*TABLE\s*' + table_name_pattern, 'load data'),
        
        # CTE patterns
        (r'\bWITH\s+\w+\s+AS\s*\(', 'cte'),
        
        # Additional INTO patterns
        (r'INTO\s*TABLE\s*' + table_name_pattern, 'into table'),
        (r'\bINTO\s+' + table_name_pattern, 'insert into'),
    ]
    
    # Compile patterns for better performance
    compiled_patterns = [(re.compile(pattern, re.IGNORECASE), obj_type) for pattern, obj_type in pattern_types]
    
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
            
        matching_lines = []
        
        # Track multi-line comment state
        in_multiline_comment = False
        
        # Process lines individually first
        for line_num, line in enumerate(lines, 1):
            line_stripped = line.strip()
            
            # Skip empty lines
            if not line_stripped:
                continue
            
            # Handle multi-line comments
            # Check for single-line comments enclosed in /* */
            if re.search(r'/\*.*\*/', line_stripped):
                continue
                
            # Check if we're entering a multi-line comment
            if '/*' in line_stripped:
                # If the line also contains */ then it's a single-line comment (already handled above)
                if '*/' not in line_stripped:
                    in_multiline_comment = True
                continue
            
            # Check if we're exiting a multi-line comment
            if in_multiline_comment:
                if '*/' in line_stripped:
                    in_multiline_comment = False
                continue
                
            # Enhanced single-line comment detection
            if is_comment_line(line_stripped):
                continue
                
            # Skip lines that are likely part of multi-line EXTRACT or DATE functions
            if should_skip_line(line_stripped, lines, line_num - 1):
                continue
                
            # Check if line matches any of our patterns
            for pattern, obj_type in compiled_patterns:
                if pattern.search(line):
                    # Extract table name and category
                    table_name, category = extract_table_name_from_line(line, pattern, obj_type)
                    
                    # Skip if table_name is None (filtered out as SQL function)
                    if table_name is not None:
                        matching_lines.append({
                            'line_number': line_num,
                            'content': line.strip(),
                            'operation_type': obj_type,
                            'table_name': table_name,
                            'category': category,
                            'file_path': file_path
                        })
                    break  # Avoid duplicate matches for the same line
        
        # Now handle multi-line patterns
        # Look for patterns that span across lines (like FROM on one line, table on next)
        multi_line_patterns = [
            (r'\bFROM\s*$', r'^\s*' + table_name_pattern, 'select from'),
            (r'\bJOIN\s*$', r'^\s*' + table_name_pattern, 'join'),
            (r'\bINTO\s*$', r'^\s*' + table_name_pattern, 'insert into'),
            (r'\bUPDATE\s*$', r'^\s*' + table_name_pattern, 'update'),
            (r'\bMERGE\s*$', r'^\s*' + table_name_pattern, 'merge'),
            (r'\bUSING\s*$', r'^\s*' + table_name_pattern, 'merge using'),
            (r'\bDELETE\s+FROM\s*$', r'^\s*' + table_name_pattern, 'delete from'),
            (r'(?:INSERT|INS)\s+(?:INTO|OVERWRITE)\s*$', r'^\s*(?:TABLE\s+)?' + table_name_pattern, 'insert into'),
        ]
        
        compiled_multi_patterns = [(re.compile(p1, re.IGNORECASE), re.compile(p2, re.IGNORECASE), obj_type) 
                                 for p1, p2, obj_type in multi_line_patterns]
        
        # Reset multi-line comment state for second pass
        in_multiline_comment = False
        
        for i, line in enumerate(lines[:-1]):  # Don't check last line since we need next line
            line_stripped = line.strip()
            
            # Handle multi-line comments for second pass
            if re.search(r'/\*.*\*/', line_stripped):
                continue
                
            if '/*' in line_stripped:
                if '*/' not in line_stripped:
                    in_multiline_comment = True
                continue
            
            if in_multiline_comment:
                if '*/' in line_stripped:
                    in_multiline_comment = False
                continue
            
            # Enhanced single-line comment detection
            if is_comment_line(line_stripped):
                continue
            
            # Skip empty lines
            if not line_stripped:
                continue
            
            next_line = lines[i + 1].strip()
            next_line_num = i + 2
            
            # Handle multi-line comments for next line
            next_line_in_comment = False
            if re.search(r'/\*.*\*/', next_line):
                next_line_in_comment = True
            elif '/*' in next_line or in_multiline_comment:
                next_line_in_comment = True
            elif is_comment_line(next_line):
                next_line_in_comment = True
            
            if not next_line or next_line_in_comment:
                continue
                
            # Skip if the next line should be filtered (multi-line EXTRACT patterns, etc.)
            if should_skip_line(next_line, lines, i + 1):
                continue
            
            # Check for multi-line patterns
            for pattern1, pattern2, obj_type in compiled_multi_patterns:
                if pattern1.search(line) and pattern2.search(next_line):
                    # Check if we already found this line
                    line_already_found = any(match['line_number'] == i + 2 for match in matching_lines)
                    if not line_already_found:
                        # For multi-line, extract table name from the second line
                        table_match = pattern2.search(next_line)
                        table_name = None
                        if table_match:
                            table_name = table_match.group(0).strip()
                            # Clean up table name
                            table_name = table_name.split()[0] if table_name else None
                            if table_name:
                                table_name = table_name.rstrip(',')
                        
                        # Determine category
                        target_operations = {
                            'insert into', 'delete from', 'update', 'merge'
                        }
                        source_operations = {
                            'select from', 'join', 'merge using'
                        }
                        
                        if obj_type in target_operations:
                            category = 'target'
                        elif obj_type in source_operations:
                            category = 'source'
                        else:
                            category = 'utility'
                        
                        matching_lines.append({
                            'line_number': i + 2,  # Next line number (1-indexed)
                            'content': next_line,
                            'operation_type': obj_type,
                            'table_name': table_name,
                            'category': category,
                            'file_path': file_path
                        })
                        break
        
        # Sort by line number
        matching_lines.sort(key=lambda x: x['line_number'])
        
        # Save output to file if requested
        if save_output and matching_lines:
            output_file = f"{os.path.splitext(file_path)[0]}_lineage_output.txt"
            save_results_to_file(matching_lines, output_file)
            print(f"\nResults saved to: {output_file}")
        
        return matching_lines
        
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return []
    except Exception as e:
        print(f"Error reading file '{file_path}': {str(e)}")
        return []

def save_results_to_file(matching_lines, output_file):
    """
    Save the results to a text file.
    """
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"SQL Lineage Analysis Results\n")
            f.write(f"Found {len(matching_lines)} lines containing database objects:\n")
            f.write("=" * 120 + "\n\n")
            
            for match in matching_lines:
                table_name = match.get('table_name', 'N/A')
                if table_name is None:
                    table_name = 'N/A'
                category = match.get('category', 'N/A')
                if category is None:
                    category = 'N/A'
                f.write(f"Line {match['line_number']:4d} | {match['operation_type']:20s} | {category:8s} | {table_name:40s} | {match['content']}\n")
                
        print(f"Results saved to: {output_file}")
    except Exception as e:
        print(f"Error saving to file: {str(e)}")

def analyze_lineage_mapping(matching_lines):
    """
    Analyze the lineage mapping to create source-target relationships.
    Returns a list of relationships in format: source_table, source_operation, target_table, target_operation
    """
    # Group lines by SQL statement boundaries (targets start new statements)
    statement_groups = []
    current_statement = []
    
    for line in matching_lines:
        # Target operations typically start new SQL statements
        if line['category'] == 'target':
            # Save previous statement if it exists
            if current_statement:
                statement_groups.append(current_statement)
            # Start new statement with this target
            current_statement = [line]
        else:
            # Add source to current statement
            if current_statement:  # Only add if we have a target
                current_statement.append(line)
    
    # Add the last statement group
    if current_statement:
        statement_groups.append(current_statement)
    
    lineage_relationships = []
    
    # Analyze each statement group
    for group in statement_groups:
        targets = [line for line in group if line['category'] == 'target']
        sources = [line for line in group if line['category'] == 'source']
        
        # For each target in this group, create relationships with all sources in the same group
        for target in targets:
            target_table = target['table_name'] if target['table_name'] != 'N/A' else 'N/A'
            target_operation = target['operation_type']
            
            if sources:
                # Create relationship for each source to this target
                for source in sources:
                    source_table = source['table_name'] if source['table_name'] != 'N/A' else 'N/A'
                    source_operation = source['operation_type']
                    
                    lineage_relationships.append({
                        'source_table': source_table,
                        'source_operation': source_operation,
                        'target_table': target_table,
                        'target_operation': target_operation,
                        'source_line': source['line_number'],
                        'target_line': target['line_number'],
                        'relative_file_path': target.get('relative_file_path', source.get('relative_file_path', 'N/A'))
                    })
            else:
                # Target with no sources (e.g., TRUNCATE operations)
                lineage_relationships.append({
                    'source_table': 'N/A',
                    'source_operation': 'N/A',
                    'target_table': target_table,
                    'target_operation': target_operation,
                    'source_line': 'N/A',
                    'target_line': target['line_number'],
                    'relative_file_path': target.get('relative_file_path', 'N/A')
                })
    
    return lineage_relationships

def print_lineage_mapping(lineage_relationships):
    """Print the lineage mapping in the requested format."""
    print("\n" + "="*80)
    print("LINEAGE MAPPING")
    print("="*80)
    print("Format: source_table, source_operation, target_table, target_operation")
    print("-"*80)
    
    if not lineage_relationships:
        print("No lineage relationships found.")
        return
    
    for relationship in lineage_relationships:
        print(f"{relationship['source_table']}, {relationship['source_operation']}, "
              f"{relationship['target_table']}, {relationship['target_operation']}")
    
    print(f"\nTotal relationships: {len(lineage_relationships)}")
    print("="*80)

def save_lineage_mapping(lineage_relationships, output_file):
    """Save the lineage mapping to a file in the requested format."""
    try:
        base_name = os.path.splitext(output_file)[0]
        lineage_file = f"{base_name}_lineage_mapping.txt"
        
        with open(lineage_file, 'w', encoding='utf-8') as f:
            f.write("LINEAGE MAPPING\n")
            f.write("="*80 + "\n")
            f.write("Format: source_table, source_operation, target_table, target_operation\n")
            f.write("-"*80 + "\n")
            
            if lineage_relationships:
                for relationship in lineage_relationships:
                    f.write(f"{relationship['source_table']}, {relationship['source_operation']}, "
                           f"{relationship['target_table']}, {relationship['target_operation']}\n")
                f.write(f"\nTotal relationships: {len(lineage_relationships)}\n")
            else:
                f.write("No lineage relationships found.\n")
            
            f.write("="*80 + "\n")
        
        print(f"Lineage mapping saved to: {lineage_file}")
    except Exception as e:
        print(f"Error saving lineage mapping: {str(e)}")

def print_results(matching_lines):
    """
    Print the results in a formatted way.
    """
    if not matching_lines:
        print("No database objects found in the file.")
        return
    
    print(f"Found {len(matching_lines)} lines containing database objects:")
    print("-" * 120)
    print(f"{'Line':>4} | {'Operation':20} | {'Category':8} | {'Table Name':40} | {'Content'}")
    print("-" * 120)
    
    for match in matching_lines:
        table_name = match.get('table_name', 'N/A')
        if table_name is None:
            table_name = 'N/A'
        category = match.get('category', 'N/A')
        if category is None:
            category = 'N/A'
        content = match['content'][:60] + "..." if len(match['content']) > 60 else match['content']
        print(f"{match['line_number']:4d} | {match['operation_type']:20s} | {category:8s} | {table_name:40s} | {content}")

def process_all_files_in_directory(directory_path, save_output=False, show_lineage=False, base_dir=None):
    """
    Process all files in a directory recursively (not just SQL files).
    """
    if base_dir is None:
        base_dir = directory_path
        
    directory = Path(directory_path)
    all_results = []
    
    # Find all files recursively (not just .sql files)
    all_files = []
    for file_path in directory.rglob('*'):
        if file_path.is_file():
            all_files.append(file_path)
    
    if not all_files:
        print(f"No files found in directory: {directory_path}")
        return []
    
    print(f"Found {len(all_files)} files in directory: {directory_path}")
    print("=" * 80)
    
    for file_path in all_files:
        try:
            # Get relative path from base directory
            relative_path = file_path.relative_to(base_dir)
            
            # Add the input folder name to the relative path
            folder_name = Path(directory_path).name
            relative_path_with_folder = Path(folder_name) / relative_path
            
            print(f"Processing: {relative_path_with_folder}")
            
            # Try to extract database objects (will work for any text file)
            matching_lines = extract_db_objects_from_sql(str(file_path), save_output)
            
            if matching_lines:
                # Add relative file path to each result
                for match in matching_lines:
                    match['relative_file_path'] = str(relative_path_with_folder)
                all_results.extend(matching_lines)
            else:
                # Add entry for files with no tables found
                all_results.append({
                    'line_number': 0,
                    'content': 'No tables found',
                    'operation_type': 'none',
                    'table_name': 'No tables',
                    'category': 'none',
                    'file_path': str(file_path),
                    'relative_file_path': str(relative_path_with_folder)
                })
                
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            # Add error entry
            relative_path = file_path.relative_to(base_dir) if file_path.exists() else Path(str(file_path).replace(str(base_dir), '').lstrip(os.sep))
            folder_name = Path(directory_path).name
            relative_path_with_folder = Path(folder_name) / relative_path
            all_results.append({
                'line_number': 0,
                'content': f'Error: {str(e)}',
                'operation_type': 'error',
                'table_name': 'Error',
                'category': 'error',
                'file_path': str(file_path),
                'relative_file_path': str(relative_path_with_folder)
            })
    
    return all_results

def save_results_to_csv(results, output_path):
    """
    Save results to CSV file.
    """
    if not results:
        print("No results to save.")
        return
        
    fieldnames = ['relative_file_path', 'line_number', 'operation_type', 'category', 'table_name', 'content']
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for result in results:
            # Clean up content for CSV
            content = result.get('content', '')
            if len(content) > 200:  # Truncate very long content
                content = content[:197] + "..."
            
            writer.writerow({
                'relative_file_path': result.get('relative_file_path', ''),
                'line_number': result.get('line_number', 0),
                'operation_type': result.get('operation_type', ''),
                'category': result.get('category', ''),
                'table_name': result.get('table_name', ''),
                'content': content.replace('\n', ' ').replace('\r', '')  # Remove newlines for CSV
            })
    
    print(f"Results saved to: {output_path}")

def save_lineage_mapping_to_csv(lineage_relationships, output_path):
    """
    Save lineage mapping to CSV file.
    """
    if not lineage_relationships:
        print("No lineage relationships to save.")
        return
        
    fieldnames = ['relative_file_path', 'source_table', 'source_operation', 'target_table', 'target_operation']
    
    with open(output_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for relationship in lineage_relationships:
            writer.writerow({
                'relative_file_path': relationship.get('relative_file_path', ''),
                'source_table': relationship.get('source_table', ''),
                'source_operation': relationship.get('source_operation', ''),
                'target_table': relationship.get('target_table', ''),
                'target_operation': relationship.get('target_operation', '')
            })
    
    print(f"Lineage mapping saved to: {output_path}")

def print_results_csv_format(results):
    """
    Print results in a table format similar to CSV.
    """
    if not results:
        print("No database objects found.")
        return
    
    print(f"Found {len(results)} lines containing database objects:")
    print("-" * 120)
    print(f"{'File':<40} | {'Line':<4} | {'Operation':<20} | {'Category':<8} | {'Table Name':<40} | {'Content':<30}")
    print("-" * 120)
    
    for result in results:
        file_path = result.get('relative_file_path', 'N/A')
        if len(file_path) > 37:
            file_path = "..." + file_path[-34:]
            
        table_name = result.get('table_name', 'N/A')
        if table_name is None:
            table_name = 'N/A'
        if len(table_name) > 37:
            table_name = table_name[:34] + "..."
            
        category = result.get('category', 'N/A')
        if category is None:
            category = 'N/A'
            
        content = result.get('content', '')
        if len(content) > 27:
            content = content[:24] + "..."
        content = content.replace('\n', ' ').replace('\r', '')
        
        print(f"{file_path:<40} | {result.get('line_number', 0):4d} | {result.get('operation_type', ''):20s} | {category:8s} | {table_name:<40} | {content}")

def main():
    """
    Main function to handle command line arguments and process SQL files or directories.
    """
    parser = argparse.ArgumentParser(
        description='Extract database objects and lineage from SQL files or all files in directories.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python lineage.py file.sql                           # Process single file
  python lineage.py file.sql --save                    # Process and save to CSV
  python lineage.py file.sql --lineage                 # Show lineage mapping
  python lineage.py /path/to/directory                 # Process all files in directory recursively
  python lineage.py /path/to/directory --save          # Process directory and save to CSV
  python lineage.py /path/to/directory --lineage       # Process directory with lineage
        """
    )
    
    parser.add_argument('path', help='Path to file or directory containing files')
    parser.add_argument('--save', action='store_true', help='Save output to CSV files')
    parser.add_argument('--lineage', action='store_true', help='Generate lineage mapping analysis')
    parser.add_argument('--output-dir', default='.', help='Output directory for CSV files (default: current directory)')
    
    args = parser.parse_args()
    
    input_path = Path(args.path)
    
    # Check if input path exists
    if not input_path.exists():
        print(f"Error: Path '{args.path}' does not exist.")
        sys.exit(1)
    
    # Process based on whether it's a file or directory
    if input_path.is_file():
        print(f"Processing file: {input_path}")
        print("=" * 80)
        
        # Extract database objects
        matching_lines = extract_db_objects_from_sql(str(input_path), False)
        
        # Add relative file path (just the filename for single file)
        for match in matching_lines:
            match['relative_file_path'] = input_path.name
            
        if not matching_lines:
            matching_lines = [{
                'line_number': 0,
                'content': 'No tables found',
                'operation_type': 'none',
                'table_name': 'No tables',
                'category': 'none',
                'file_path': str(input_path),
                'relative_file_path': input_path.name
            }]
        
        # Print results
        print_results_csv_format(matching_lines)
        
        # Save to CSV if requested
        if args.save:
            output_file = Path(args.output_dir) / f"{input_path.stem}_analysis.csv"
            save_results_to_csv(matching_lines, output_file)
        
        # Show lineage mapping if requested
        if args.lineage:
            print("\n" + "=" * 80)
            print("LINEAGE MAPPING ANALYSIS")
            print("=" * 80)
            lineage_relationships = analyze_lineage_mapping(matching_lines)
            print_lineage_mapping(lineage_relationships)
            
            if args.save:
                lineage_output_file = Path(args.output_dir) / f"{input_path.stem}_lineage.csv"
                save_lineage_mapping_to_csv(lineage_relationships, lineage_output_file)
    
    elif input_path.is_dir():
        print(f"Processing all files in directory: {input_path}")
        print("=" * 80)
        
        # Process all files in directory
        all_results = process_all_files_in_directory(str(input_path), False, args.lineage, input_path)
        
        # Print results
        print_results_csv_format(all_results)
        
        # Save to CSV if requested
        if args.save:
            output_file = Path(args.output_dir) / f"{input_path.name}_directory_analysis.csv"
            save_results_to_csv(all_results, output_file)
        
        # Show lineage mapping if requested
        if args.lineage:
            print("\n" + "=" * 80)
            print("LINEAGE MAPPING ANALYSIS")
            print("=" * 80)
            lineage_relationships = analyze_lineage_mapping(all_results)
            print_lineage_mapping(lineage_relationships)
            
            if args.save:
                lineage_output_file = Path(args.output_dir) / f"{input_path.name}_directory_lineage.csv"
                save_lineage_mapping_to_csv(lineage_relationships, lineage_output_file)
    
    else:
        print(f"Error: '{args.path}' is neither a file nor a directory.")
        sys.exit(1)

if __name__ == "__main__":
    main()