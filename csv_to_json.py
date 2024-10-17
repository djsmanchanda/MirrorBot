import pandas as pd
import json
import emoji
import os

def decode_emojis(text):
    """Decodes emojis in text using emoji library."""
    if isinstance(text, str):
        return emoji.emojize(text, language='alias')
    return text

def load_data(input_file_csv):
    """Loads CSV data into a DataFrame."""
    df = pd.read_csv(input_file_csv)
    return df

def find_most_frequent_sender(df):
    """Finds the most frequent sender in the DataFrame."""
    most_frequent_sender = df['sender_name'].value_counts().idxmax()
    frequency = df['sender_name'].value_counts().max()
    first_name = most_frequent_sender.split()[0]
    print(f"The most frequent sender is: {most_frequent_sender} with {frequency} messages")
    return most_frequent_sender, first_name

def create_prompt_response_pairs(df, most_frequent_sender_first_name, most_frequent_sender):
    """Creates prompt-response pairs for JSON output."""
    json_data = []
    for i in range(len(df) - 1):
        current_row = df.iloc[i]
        next_row = df.iloc[i + 1]

        if current_row['sender_name'] == most_frequent_sender or next_row['sender_name'] != most_frequent_sender:
            continue

        prompt_text = current_row['message']
        response_text = next_row['message']

        if pd.isna(prompt_text) or pd.isna(response_text):
            continue

        json_data.append({
            "user": decode_emojis(prompt_text),
            most_frequent_sender_first_name: decode_emojis(response_text)
        })
    return json_data

def save_json(json_data, output_file):
    """Saves JSON data to a file."""
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, ensure_ascii=False, indent=4)
    print(f"Data has been saved to {output_file}")

def json_to_jsonl(input_file, output_file):
    """Converts JSON to JSONL format."""
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    with open(output_file, 'w', encoding='utf-8') as f:
        for item in data:
            json.dump(item, f, ensure_ascii=False)
            f.write('\n')
    print(f"Data has been converted to JSONL format and saved to {output_file}")

def modify_jsonl_keys(input_file, output_file, key_name):
    """Modifies keys in a JSONL file and saves to a new JSONL file."""
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8') as outfile:
        for line in infile:
            data = json.loads(line.strip())
            data['Prompt'] = data.pop('user', None)
            data['Completion'] = data.pop(key_name, None)
            json.dump(data, outfile, ensure_ascii=False)
            outfile.write('\n')
    print(f"Keys have been modified and saved to {output_file}")

if __name__ == "__main__":
    # Load data and process
    input_file_csv = 'instagram_messages.csv'  # Replace with your CSV file path
    df = load_data(input_file_csv)

    # Identify most frequent sender
    most_frequent_sender, most_frequent_sender_first_name = find_most_frequent_sender(df)

    # Create prompt-response pairs
    json_data = create_prompt_response_pairs(df, most_frequent_sender_first_name, most_frequent_sender)

    # Save to JSON
    json_output_file = 'final_dataset.json'
    save_json(json_data, json_output_file)

    # Convert JSON to JSONL
    jsonl_output_file = 'final_dataset.jsonl'
    json_to_jsonl(json_output_file, jsonl_output_file)

    # Modify keys in JSONL and save to another JSONL
    final_jsonl_output_file = 'final_dataset2.jsonl'
    modify_jsonl_keys(jsonl_output_file, final_jsonl_output_file, most_frequent_sender_first_name)
