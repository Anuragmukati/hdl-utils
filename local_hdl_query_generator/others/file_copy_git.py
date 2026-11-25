import shutil
import json
import os

def copy_files_from_json_file(json_file_path):
    """
    Copies files from source paths to destination paths based on a JSON file.

    Args:
        json_file_path (str): The path to the JSON file containing the file mappings.
    """
    try:
        # Check if the JSON file exists
        if not os.path.exists(json_file_path):
            print(f"Error: The JSON file '{json_file_path}' was not found. ❌")
            return

        with open(json_file_path, 'r') as f:
            file_mappings = json.load(f)

        if not isinstance(file_mappings, list):
            print("Error: The JSON file must contain a list of objects. ❌")
            return

        for mapping in file_mappings:
            if 'SOURCE_Path' not in mapping or 'dest_path' not in mapping:
                print("Error: Each object in the JSON file must contain 'SOURCE_Path' and 'dest_path' keys. ❌")
                continue

            source_path = mapping['SOURCE_Path']
            dest_path = mapping['dest_path']

            # Normalize paths for different operating systems
            source_path_normalized = os.path.normpath(source_path)
            dest_path_normalized = os.path.normpath(dest_path)
            
            # Create the destination directory if it doesn't exist
            dest_dir = os.path.dirname(dest_path_normalized)
            if dest_dir and not os.path.exists(dest_dir):
                os.makedirs(dest_dir)
                print(f"Created directory: {dest_dir} 📂")

            try:
                shutil.copy(source_path_normalized, dest_path_normalized)
                print(f"Copied '{source_path_normalized}' to '{dest_path_normalized}' successfully. ✅")
            except FileNotFoundError:
                print(f"Error: The source file '{source_path_normalized}' was not found. ❌")
            except Exception as e:
                print(f"An error occurred while copying '{source_path_normalized}': {e} ❌")

    except json.JSONDecodeError:
        print(f"Error: Invalid JSON format in '{json_file_path}'. Please check the file's contents. ❌")
    except Exception as e:
        print(f"An unexpected error occurred: {e} ❌")


json_file_path = 'file_mappings.json'
copy_files_from_json_file(json_file_path)