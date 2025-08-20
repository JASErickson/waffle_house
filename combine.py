#!/usr/bin/env python3
import os
import pandas as pd
import tempfile
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
import io

# ---------- CONFIG ----------
FOLDER_ID = '1yTC_EqKSAkMbK7ftPFrDnYmJBCVS_fe8'  # Google Drive folder containing CSVs
SERVICE_ACCOUNT_JSON = '/tmp/credentials.json'  # path to your key
OUTPUT_FILE = "combined.csv"
# ----------------------------

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
    return build('drive', 'v3', credentials=creds)

def list_csv_files(service, folder_id):
    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    return results.get("files", [])

def download_file(service, file_id, file_name, temp_dir):
    request = service.files().get_media(fileId=file_id)
    file_path = os.path.join(temp_dir, file_name)
    fh = io.FileIO(file_path, 'wb')
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.close()
    return file_path

def main():
    service = get_drive_service()
    csv_files = list_csv_files(service, FOLDER_ID)
    if not csv_files:
        print("No CSV files found in the Google Drive folder.")
        return

    temp_dir = tempfile.mkdtemp()
    dfs = []
    for f in csv_files:
        print(f"Downloading {f['name']}...")
        path = download_file(service, f['id'], f['name'], temp_dir)
        dfs.append(pd.read_csv(path))

    combined_df = pd.concat(dfs, ignore_index=True)

    # Determine save path
    if os.environ.get("GITHUB_ACTIONS"):
        save_path = OUTPUT_FILE
    else:
        save_path = os.path.expanduser(f"~/Downloads/{OUTPUT_FILE}")

    # Save combined CSV
    combined_df.to_csv(save_path, index=False)
    print(f"✅ Combined CSV saved to: {save_path}")

    # Clean up temp files
    for f in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, f))
    os.rmdir(temp_dir)

if __name__ == "__main__":
    main()