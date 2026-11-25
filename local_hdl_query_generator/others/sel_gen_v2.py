import re
import json
import os
from typing import List, Dict, Optional, Tuple
from datetime import datetime

# --- CONFIGURABLE FILE PATHS ---
# The script will read all .sql files from this directory.
input_directory = r'C:\Users\mukatan\Downloads\query_generator\query_generator\ddls' 
config_output_file = 'generated_config.json'
# -------------------------------

def parse_single_table_ddl(ddl_content: str) -> Tuple[Optional[str], List[Dict]]:
    """
    Parses a single DDL statement, correctly handling parentheses within string literals.
    """
    columns = []
    table_name = None
    string_types = {'VARCHAR', 'CHAR', 'CHARACTER', 'TEXT'}

    name_match = re.search(r'CREATE\s+.*?TABLE\s+([a-zA-Z0-9_.]+)', ddl_content, re.IGNORECASE | re.DOTALL)
    if not name_match:
        return None, []
    table_name = name_match.group(1)

    try:
        start_paren_pos = ddl_content.index('(', name_match.end())
        
        paren_level = 1
        in_string = False
        end_paren_pos = -1
        
        for i, char in enumerate(ddl_content[start_paren_pos + 1:]):
            if char == "'":
                in_string = not in_string

            if not in_string:
                if char == '(':
                    paren_level += 1
                elif char == ')':
                    paren_level -= 1

            if paren_level == 0:
                end_paren_pos = start_paren_pos + 1 + i
                break

        if end_paren_pos == -1:
            print(f"DEBUG: Could not find the closing parenthesis for columns in table '{table_name}'.")
            return table_name, []
            
        column_definitions_text = ddl_content[start_paren_pos + 1 : end_paren_pos]

    except ValueError:
        print(f"DEBUG: Could not find the main column block for table '{table_name}'.")
        return table_name, []

    for line in column_definitions_text.split('\n'):
        line = line.strip().upper()
        if not line or line.startswith('--') or line.startswith('/*'):
            continue

        parts = re.split(r'\s+', line)
        if len(parts) < 2:
            continue

        col_name = parts[0].replace('`', '').replace('"', '')
        if col_name in ('PRIMARY', 'UNIQUE', 'CONSTRAINT', 'FOREIGN', 'CHECK'):
            continue

        col_type = parts[1].split('(')[0]
        is_not_null = 'NOT NULL' in line
        is_string_type = any(st in col_type for st in string_types)

        length = None
        if is_string_type:
            length_match = re.search(r'\((\d+)\)', parts[1])
            if length_match:
                length = int(length_match.group(1))

        columns.append({
            'name': col_name,
            'is_string': is_string_type,
            'not_null': is_not_null,
            'type': col_type,
            'length': length
        })

    if not columns:
        print(f"DEBUG: Could not parse any valid column definitions for table '{table_name}'.")
        
    return table_name, columns

def parse_ddl_file(ddl_file_path: str) -> List[Tuple[str, List[Dict]]]:
    """
    Parses a DDL file by splitting it before each 'CREATE TABLE' statement.
    """
    try:
        with open(ddl_file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
    except FileNotFoundError:
        print(f"FATAL ERROR: The DDL file was not found at the path: '{ddl_file_path}'")
        return []
    except Exception as e:
        print(f"FATAL ERROR: An OS error occurred while reading the file '{ddl_file_path}': {e}")
        return []

    ddl_statements = re.split(r'(?=CREATE\s+(?:SET|MULTISET)?\s*TABLE)', content, flags=re.IGNORECASE)
    
    parsed_tables = []
    for ddl_block in ddl_statements:
        if ddl_block.strip():
            table_name, columns = parse_single_table_ddl(ddl_block)
            if table_name and columns:
                parsed_tables.append((table_name, columns))
                
    return parsed_tables

def generate_select_statement(table_name: str, columns: List[Dict]) -> str:
    """
    Generates the SELECT statement based on the parsed column metadata.
    """
    select_clauses = []
    for col in columns:
        col_name = col['name']
        clause = ""

        if col['is_string']:
            if col['not_null']:
                base_expression = f"CASE WHEN RTRIM({col_name}) = '' THEN ' ' ELSE RTRIM({col_name}) END"
            else:
                base_expression = f"RTRIM({col_name})"

            if col.get('length') and col['length'] > 3000000000000:
                final_expression = f"OREPLACE(OREPLACE({base_expression}, CHR(13), ''), CHR(10), '')"
            else:
                final_expression = base_expression

            clause = f"{final_expression} AS {col_name}"
        elif col['type'] == 'TIME':
            clause = f"CAST({col_name} AS VARCHAR(16)) AS {col_name}"
        elif col['type'] == 'DATE':
            clause = f"CAST({col_name} AS DATE FORMAT 'YYYY-MM-DD') AS {col_name}"
        elif col['type'] == 'TIMESTAMP':
            clause = f"CAST(CAST({col_name} AS TIMESTAMP FORMAT 'YYYY-MM-DDBHH:MI:SS.S(6)') AS VARCHAR(50)) AS {col_name}"
        elif col['type'] == 'FLOAT':
            clause = f"ROUND(CAST({col_name} AS DECIMAL(38,16)),16) AS {col_name}"
        else:
            clause = col_name
            
        select_clauses.append(clause)
        
    formatted_clauses = ",\n  ".join(select_clauses)
    return f"SELECT\n  {formatted_clauses}\nFROM\n  {table_name}"

def generate_config_json(table_configs: List[Dict], output_path: str):
    """
    Generates the final JSON configuration object containing a list of all tables.
    """
    current_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
    
    table_dict_entries = []
    for config in table_configs:
        table_name = config['table_name']
        select_query = config['select_query']
        
        if '.' in table_name:
            td_schema_name, td_table_name = table_name.split('.', 1)
        else:
            td_schema_name = "default_schema" 
            td_table_name = table_name
        
        fetch_query = select_query.replace('\n', ' ').replace('  ', ' ')

        table_entry = {
            "td_schema_name": td_schema_name.lower(),
            "td_table_name": td_table_name.lower(),
            "bq_schema_name": td_schema_name.lower(),
            "bq_table_name": td_table_name.lower(),
            "fetch_query": fetch_query,
            "LoadType": "FULL_LOAD",
            "partition_column": "NA",
            "partition_type": "NA",
            "partition_format": "NA",
            "partition_minvalue": "NA",
            "partition_range": "NA",
            "status": "INCOMPLETE",
            "appname": "NOS_First",
            "DateRequested": current_time,
            "DateCompleted": current_time,
            "schema_fields": "Not Found",
            "table_size": "Large",
            "partition_maxvalue": "NA"
        }
        table_dict_entries.append(table_entry)

    final_config = {
        "ple_flag": "1",
        "project": "gk1v",
        "email":"anurag.mukati@verizon.com",
        "table_metadata_dict": table_dict_entries
    }

    try:
        with open(output_path, 'w') as f:
            json.dump(final_config, f, indent=4)
        print(f"\nSuccessfully generated and saved JSON config for {len(table_dict_entries)} tables to '{output_path}'")
    except Exception as e:
        print(f"FATAL ERROR: Could not write JSON config file: {e}")


if __name__ == '__main__':
    print(f"--- Starting SQL Generation Process ---")
    print(f"Reading files from directory: {input_directory}")

    # A master list to hold all tables from all files
    all_tables_from_all_files = []

    try:
        # Loop through each file in the specified directory
        for filename in os.listdir(input_directory):
            # Process only files with a .sql extension
            if filename.lower().endswith('.txt'):
                full_path = os.path.join(input_directory, filename)
                print(f"\n-> Processing file: {filename}")

                # Use the existing function to parse all DDLs in the current file
                tables_from_this_file = parse_ddl_file(full_path)
                
                if tables_from_this_file:
                    print(f"  Found {len(tables_from_this_file)} table(s) in this file.")
                    # Add the parsed tables to our master list
                    all_tables_from_all_files.extend(tables_from_this_file)
                else:
                    print(f"  No valid DDLs found in {filename}.")
        
        # Now, process the master list of all tables found
        if all_tables_from_all_files:
            print(f"\n--- Total tables parsed from all files: {len(all_tables_from_all_files)} ---")
            
            table_configurations = []
            for table_name, columns in all_tables_from_all_files:
                # Generate SELECT statement for each table
                select_statement = generate_select_statement(table_name, columns)
                
                table_configurations.append({
                    'table_name': table_name,
                    'select_query': select_statement
                })
            
            # Generate a single JSON file with all the tables
            generate_config_json(table_configurations, config_output_file)
            
        else:
            print("\nNo tables were successfully parsed from any file in the directory.")

    except FileNotFoundError:
        print(f"FATAL ERROR: The specified directory does not exist: '{input_directory}'")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        
    print("\n--- Process Finished ---")