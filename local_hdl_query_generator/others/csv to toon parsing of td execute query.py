import csv
import io
import os
import re

def parse_and_convert_to_toon(input_filepath, output_filepath):
    """
    Parses a SQL query log file and converts it into TOON format.
    TOON (Token-Oriented Object Notation) is optimized for LLMs.
    Format: TableName[Count]{col1,col2}: val1,val2...
    """
    nested_data = {}
    
    try:
        with open(input_filepath, 'r', encoding='utf-8') as f:
            content = f.read()

        # --- Step 1: Parse the Input File (Same logic as before) ---
        blocks = content.split('-- QUERY START --')
        print(f"Found {len(blocks)} blocks. Parsing...")

        for block in blocks:
            if not block.strip() or '-- QUERY END --' not in block:
                continue

            parts = block.split('-- QUERY END --')
            query_text = parts[0].strip()
            raw_csv_text = parts[1].strip()

            if not raw_csv_text:
                continue

            # Extract Table Name
            match = re.search(r'\bFROM\s+([a-zA-Z0-9_.]+)', query_text, re.IGNORECASE)
            table_name = match.group(1) if match else "UNKNOWN_TABLE"

            # Parse CSV
            f_io = io.StringIO(raw_csv_text)
            reader = csv.reader(f_io)
            
            try:
                rows = list(reader)
                if not rows: continue

                raw_headers = rows[0]
                headers = [h.replace("'", "").strip() for h in raw_headers]
                data_rows = rows[1:]
                
                block_records = []
                for row in data_rows:
                    row_dict = {}
                    for index, header in enumerate(headers):
                        value = row[index] if index < len(row) else ""
                        if isinstance(value, str):
                            value = value.strip()
                        row_dict[header] = value
                    block_records.append(row_dict)
                
                if table_name not in nested_data:
                    nested_data[table_name] = []
                nested_data[table_name].extend(block_records)

            except csv.Error:
                continue

        # --- Step 2: Generate TOON Output ---
        toon_lines = []
        
        for table_name, rows in nested_data.items():
            if not rows:
                continue
            
            # Get headers from the first row
            headers = list(rows[0].keys())
            count = len(rows)
            
            # 1. Write the Header Line
            # Format: TableName[N]{col1,col2,col3}:
            header_str = ",".join(headers)
            toon_lines.append(f"{table_name}[{count}]{{{header_str}}}:")
            
            # 2. Write the Data Rows (CSV-style compact values)
            for row in rows:
                vals = []
                for h in headers:
                    v = row.get(h, "")
                    # Basic "Smart Quoting": Only quote if contains special chars
                    if "," in v or "\n" in v or '"' in v:
                        # Escape quotes by doubling them (CSV standard)
                        v_escaped = v.replace('"', '""')
                        vals.append(f'"{v_escaped}"')
                    else:
                        vals.append(v)
                
                toon_lines.append(",".join(vals))
            
            # Add an empty line between tables for readability
            toon_lines.append("")

        # Write to file
        with open(output_filepath, 'w', encoding='utf-8') as f:
            f.write("\n".join(toon_lines))

        print(f"Successfully converted {len(nested_data)} tables to TOON format.")
        print(f"Output saved to: {output_filepath}")

    except Exception as e:
        print(f"Error: {e}")

# --- Execution Block ---
if __name__ == "__main__":
    input_csv = "teradataexecute_output_td_results_20260123130248.csv" 
    output_toon = "td_outputs.toon"
    
    # Create dummy file if not exists (for testing)
    if not os.path.exists(input_csv):
        with open(input_csv, 'w') as f:
            f.write("""-- QUERY START --
Select 'MU_SAME_DIFF_OCC1',INACTIVE_DT FROM PRE_TBLS.MU_SAME_DIFF_OCC1
-- QUERY END --
'MU_SAME_DIFF_OCC1',INACTIVE_DT,IONT_DISCONNECT_DATE
MU_SAME_DIFF_OCC1,2025-06-14,2025-06-14

-- QUERY START --
Select 'TK_MODEL_5GADDPRED5',Max(ACCT_TERM_DT) FROM COE_UAT_TBLS.TK_MODEL_5GADDPRED5
-- QUERY END --
'TK_MODEL_5GADDPRED5',ACCT_TERM_DT,COHORT_CLOSE_DT
TK_MODEL_5GADDPRED5,2025-01-01,2025-02-01
""")
    
    parse_and_convert_to_toon(input_csv, output_toon)