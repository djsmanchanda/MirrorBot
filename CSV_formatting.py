import os
import json
import pandas as pd
import emoji
from concurrent.futures import ThreadPoolExecutor, as_completed

def process_json_files(json_files, folder_name):
    """
    Processes a list of JSON files to extract and combine message data.
    
    Parameters:
    json_files (list): List of file paths to JSON files.
    folder_name (str): Name of the folder being processed.

    Returns:
    pd.DataFrame: DataFrame with combined message data.
    """
    messages = []
    
    for file_path in sorted(json_files, key=lambda x: int(x.split('_')[-1].split('.')[0])):
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
            messages.extend(data['messages'])
    
    senders = {msg.get('sender_name') for msg in messages if 'sender_name' in msg}
    if len(senders) != 2:
        return None
    
    df = pd.DataFrame(messages)
    df.sort_values(by='timestamp_ms', inplace=True)
    
    def process_message(row):
        content = row.get('content', "")
        return emoji.demojize(content)
    
    df['message'] = df.apply(
        lambda x: process_message(x) if 'content' in x and isinstance(x['content'], str) else "", axis=1
    )
    df['sender_name'] = df['sender_name'].replace('𝙳𝚒𝚟𝚓𝚘𝚝 𝙼𝚊𝚗𝚌𝚑𝚊𝚗𝚍𝚊', 'Divjot')
    df['share'] = df.apply(
        lambda x: 'yes' if 'share' in x and isinstance(x['share'], dict) else 'no', axis=1
    )
    df['reactions'] = df.apply(
        lambda x: emoji.demojize(x['reactions'][0]['reaction']) if 'reactions' in x and isinstance(x['reactions'], list) and len(x['reactions']) > 0 else pd.NA, axis=1
    )
    
    combined_messages = []
    last_sender, last_timestamp, combined_message, share, reactions = None, None, "", "no", pd.NA
    
    for index, row in df.iterrows():
        sender, timestamp = row['sender_name'], row['timestamp_ms']
        
        if last_sender == sender and (timestamp - last_timestamp) <= 300000:
            combined_message += "\n\n" + row['message']
        else:
            if combined_message:
                if share == 'yes':
                    combined_message = f"replied to story/sent a reel: {combined_message.strip()}"
                combined_messages.append((last_sender, last_timestamp, combined_message.strip(), share, reactions))
            combined_message, last_sender, last_timestamp, share, reactions = row['message'], sender, timestamp, row['share'], row['reactions']
    
    if combined_message:
        if share == 'yes':
            combined_message = f"replied to story/sent a reel: {combined_message.strip()}"
        combined_messages.append((last_sender, last_timestamp, combined_message.strip(), share, reactions))
    
    combined_df = pd.DataFrame(combined_messages, columns=['sender_name', 'timestamp_ms', 'message', 'share', 'reactions'])
    combined_df['message_id'] = range(1, len(combined_df) + 1)
    combined_df['folder_name'] = folder_name
    
    return combined_df

def process_folder(folder_name, json_directory):
    """
    Processes all JSON files in a folder.
    
    Parameters:
    folder_name (str): Name of the folder to process.
    json_directory (str): Directory containing the folder.

    Returns:
    pd.DataFrame: DataFrame with combined message data or None.
    """
    account_dir = os.path.join(json_directory, folder_name)
    if os.path.isdir(account_dir):
        json_files = [os.path.join(account_dir, file) for file in os.listdir(account_dir) if file.startswith('message_') and file.endswith('.json')]
        if json_files:
            return process_json_files(json_files, folder_name)
    return None

def process_all_folders(json_directory, output_file):
    """
    Processes all folders within a specified directory and saves the results to a CSV file.
    
    Parameters:
    json_directory (str): Directory containing folders of JSON files.
    output_file (str): Path for the output CSV file.
    """
    folders = [folder for folder in os.listdir(json_directory) if os.path.isdir(os.path.join(json_directory, folder))]
    df_list = []

    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        future_to_folder = {executor.submit(process_folder, folder, json_directory): folder for folder in folders}
        for future in as_completed(future_to_folder):
            folder = future_to_folder[future]
            try:
                df = future.result()
                if df is not None:
                    df_list.append(df)
            except Exception as e:
                print(f"Error processing folder {folder}: {e}")

    final_df = pd.concat(df_list, ignore_index=True)
    final_df = final_df[['message_id', 'folder_name', 'sender_name', 'timestamp_ms', 'message', 'share', 'reactions']]
    final_df.rename(columns={'timestamp_ms': 'timestamp'}, inplace=True)
    
    final_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"DataFrame saved to {output_file}")

if __name__ == "__main__":
    json_directory = 'Put_Inbox_Folder_Here/inbox1'
    output_file = 'instagram_messages.csv'
    process_all_folders(json_directory, output_file)
