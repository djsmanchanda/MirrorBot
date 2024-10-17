import os
import json

def decode_unicode(data):
    """
    Recursively decodes strings from Latin-1 to UTF-8 in a given data structure.
    
    Parameters:
    data (str, list, dict): The data to decode.

    Returns:
    Decoded data with UTF-8 encoding.
    """
    if isinstance(data, str):
        try:
            return data.encode('latin1').decode('utf-8')
        except UnicodeEncodeError:
            return data
    elif isinstance(data, list):
        return [decode_unicode(item) for item in data]
    elif isinstance(data, dict):
        return {key: decode_unicode(value) for key, value in data.items()}
    else:
        return data

def convert_json_file(input_file_path, output_file_path):
    """
    Converts a single JSON file by decoding Unicode characters.

    Parameters:
    input_file_path (str): The path to the input JSON file.
    output_file_path (str): The path to the output JSON file.
    """
    try:
        with open(input_file_path, 'r', encoding='utf-8') as infile:
            data = json.load(infile)
            decoded_data = decode_unicode(data)
            
        os.makedirs(os.path.dirname(output_file_path), exist_ok=True)
        
        with open(output_file_path, 'w', encoding='utf-8') as outfile:
            json.dump(decoded_data, outfile, ensure_ascii=False, indent=4)
    except json.JSONDecodeError:
        print(f"Failed to decode JSON file: {input_file_path}")

def convert_json_files(input_folder, output_folder):
    """
    Converts all JSON files in a given directory by decoding Unicode characters.

    Parameters:
    input_folder (str): The path to the folder containing input JSON files.
    output_folder (str): The path to the folder where output JSON files will be saved.
    """
    for root, dirs, files in os.walk(input_folder):
        for file in files:
            if file.endswith('.json'):
                input_file_path = os.path.join(root, file)
                relative_path = os.path.relpath(input_file_path, input_folder)
                output_file_path = os.path.join(output_folder, relative_path)
                convert_json_file(input_file_path, output_file_path)

def main(input_folder, output_folder):
    """
    Main function to start the conversion process.
    
    Parameters:
    input_folder (str): The path to the folder containing input JSON files.
    output_folder (str): The path to the folder where output JSON files will be saved.
    """
    convert_json_files(input_folder, output_folder)

if __name__ == "__main__":
    # Example usage; replace these paths with actual paths
    input_folder = 'Put_Inbox_Folder_Here/inbox'
    output_folder = 'Put_Inbox_Folder_Here/inbox1'
    main(input_folder, output_folder)
