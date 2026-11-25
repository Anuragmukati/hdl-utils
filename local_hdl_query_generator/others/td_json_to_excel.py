import json
import csv
import os

def convert_json_to_pipe_csv(input_filename, output_filename):
    # Check if file exists
    if not os.path.exists(input_filename):
        print(f"Error: The file '{input_filename}' was not found.")
        return

    try:
        # Open and load the JSON data
        with open(input_filename, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Open the output CSV file
        with open(output_filename, 'w', newline='', encoding='utf-8') as f:
            # Initialize CSV writer with pipe delimiter
            writer = csv.writer(f, delimiter='|')
            
            # Write the header row
            writer.writerow(['table name', 'column name', 'column value'])

            # Iterate through the JSON structure
            # Structure is: { "TableName": [ { "ColName": "Value" }, ... ] }
            for table_name, rows in data.items():
                for row in rows:
                    for col_name, col_value in row.items():
                        # Handle null/None values by converting them to an empty string
                        if col_value is None:
                            col_value = ""
                        
                        # Write the row: Table | Column | Value
                        writer.writerow([table_name, col_name, col_value])
                        
        print(f"Success! File saved as: {output_filename}")

    except json.JSONDecodeError:
        print("Error: Failed to decode JSON. Please check the input file format.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Run the function
if __name__ == "__main__":
    convert_json_to_pipe_csv('td_outputs_converted.json', 'td_output_excel.csv')