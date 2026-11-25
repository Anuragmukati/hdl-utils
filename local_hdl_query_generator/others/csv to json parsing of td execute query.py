import json
import csv
import io
import os
import re

def parse_query_log_to_json(input_filepath, output_filepath):
    """
    Parses a file containing SQL query logs and CSV outputs into a JSON file.
    Extracts the table name from the query and nests data under that table name.
    """
    # We use a dictionary now to nest data by table name
    # Structure: { "TableName": [ {row1}, {row2} ... ] }
    nested_data = {}
    
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # The file is structured in blocks starting with -- QUERY START --
        blocks = content.split('-- QUERY START --')

        print(f"Found {len(blocks)} potential blocks. Processing...")

        for block in blocks:
            # Skip empty blocks
            if not block.strip():
                continue

            # Each valid block must have a -- QUERY END --
            if '-- QUERY END --' not in block:
                continue

            # Split the block: [0] is the SQL query, [1] is the CSV data
            parts = block.split('-- QUERY END --')
            
            query_text = parts[0].strip()
            raw_csv_text = parts[1].strip()

            if not raw_csv_text:
                continue

            # --- Extract Table Name ---
            # We look for the pattern "FROM <TableName>" (case insensitive)
            # It captures alphanumeric chars, underscores, and dots (for Schema.Table)
            match = re.search(r'\bFROM\s+([a-zA-Z0-9_.]+)', query_text, re.IGNORECASE)
            
            if match:
                table_name = match.group(1)
            else:
                table_name = "UNKNOWN_TABLE"

            # --- Parse CSV Data ---
            f_io = io.StringIO(raw_csv_text)
            reader = csv.reader(f_io)
            
            try:
                rows = list(reader)
                
                if not rows:
                    continue

                # Process Header
                raw_headers = rows[0]
                headers = [h.replace("'", "").strip() for h in raw_headers]

                # Process Rows
                data_rows = rows[1:]
                
                # Prepare list for this specific block
                block_records = []

                for row in data_rows:
                    row_dict = {}
                    for index, header in enumerate(headers):
                        value = row[index] if index < len(row) else None
                        
                        if isinstance(value, str):
                            value = value.strip()
                            if value == "":
                                value = None
                                
                        row_dict[header] = value
                    
                    block_records.append(row_dict)
                
                # --- Add to Nested Dictionary ---
                if table_name not in nested_data:
                    nested_data[table_name] = []
                
                # Append these records to the table's list
                nested_data[table_name].extend(block_records)

            except csv.Error as e:
                print(f"Error parsing CSV block for table {table_name}: {e}")
                continue

        # Write the nested data to a JSON file
        with open(output_filepath, 'w', encoding='utf-8') as json_file:
            json.dump(nested_data, json_file, indent=4)

        print(f"Successfully processed data for {len(nested_data)} tables.")
        print(f"JSON output saved to: {output_filepath}")

    except FileNotFoundError:
        print(f"Error: The file '{input_filepath}' was not found.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# --- Execution Block ---
if __name__ == "__main__":
    input_csv = "teradataexecute_output_td_results_20260123203548.csv" 
    output_json = "td_outputs_converted.json"
    
    # Check if file exists, otherwise create a dummy one for demonstration
    if not os.path.exists(input_csv):
        print(f"'{input_csv}' not found. Creating sample file...")
        with open(input_csv, 'w') as f:
            f.write("""-- QUERY START --
Select 'MMG_NOTIFICATION',Max(INSERT_DT), Max(LAST_UPD_DT), Max(PROCESSED_DT_EST), Max(PROCESSED_TM), Max(PROCESSED_TM_EST)  FROM UDM_PRD_TBLS.MMG_NOTIFICATION
-- QUERY END --
'MMG_NOTIFICATION',INSERT_DT,LAST_UPD_DT,PROCESSED_DT_EST,PROCESSED_TM,PROCESSED_TM_EST
MMG_NOTIFICATION,2025-06-19,2025-06-19,2025-06-19,2025-06-19 04:59:59.999,2025-06-19 00:59:59.999


-- QUERY START --
Select 'NE_AMT_ERR2',Max(dtmBillDate), Max(dtmCreated), Max(dtmInsertedIntoThisTable), Max(dtmNoteDeleted), Max(dtmVNSSSENT)  FROM FTTP_TBLS.NE_AMT_ERR2
-- QUERY END --
'NE_AMT_ERR2',dtmBillDate,dtmCreated,dtmInsertedIntoThisTable,dtmNoteDeleted,dtmVNSSSENT
NE_AMT_ERR2,,2025-06-18 15:33:45.803,2025-06-18 16:30:01.317,,
""")
    
    parse_query_log_to_json(input_csv, output_json)