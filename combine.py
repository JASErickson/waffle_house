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
SERVICE_ACCOUNT_JSON = '/tmp/credentials.json'    # path to your service account key
OUTPUT_FILE = "waffle_house.dta"
# ----------------------------

def get_drive_service():
    creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_JSON)
    return build('drive', 'v3', credentials=creds)

def list_csv_files(service, folder_id):
    """List all CSV files in the folder, handling pagination"""
    files = []
    page_token = None
    query = f"'{folder_id}' in parents and mimeType='text/csv' and trashed=false"

    while True:
        response = service.files().list(
            q=query,
            spaces='drive',
            fields="nextPageToken, files(id, name)",
            pageToken=page_token
        ).execute()
        files.extend(response.get("files", []))
        page_token = response.get('nextPageToken')
        if not page_token:
            break
    return files

def download_file(service, file_id, file_name, temp_dir):
    """Download a file from Google Drive to a temporary folder"""
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
        try:
            print(f"Downloading {f['name']}...")
            path = download_file(service, f['id'], f['name'], temp_dir)
            dfs.append(pd.read_csv(path))
        except Exception as e:
            print(f"⚠️ Failed to download or read {f['name']}: {e}")

    if not dfs:
        print("No CSVs were successfully downloaded/read.")
        return

    combined_df = pd.concat(dfs, ignore_index=True)

    #clean for .dta type
    combined_df.columns = [c[:32].replace(" ", "_") for c in combined_df.columns]
    for col in combined_df.select_dtypes(include='datetime'):
        combined_df[col] = combined_df[col].astype('str')

    # Determine save path
    if os.environ.get("GITHUB_ACTIONS"):
        save_path = OUTPUT_FILE
    else:
        save_path = os.path.expanduser(f"~/Downloads/{OUTPUT_FILE}")

    combined_df.to_stata(save_path, index=False)
    print(f"✅ Combined CSV saved to: {save_path}")

    # Clean up temporary files
    for f in os.listdir(temp_dir):
        os.remove(os.path.join(temp_dir, f))
    os.rmdir(temp_dir)

if __name__ == "__main__":
    main()