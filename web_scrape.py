import requests
import re
import json
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from googleapiclient.http import MediaIoBaseDownload
import io
import os

# Set your Google Drive folder ID here
FOLDER_ID = '1bbrB90d_heokUah87yaFvoEubxSMelTV'  # Replace with your folder ID

# Path to the CSV file on the local system before uploading to Google Drive
LOCAL_CSV_PATH = '/tmp/waffle_house_data.csv'  # Use a temporary location on GitHub Actions or local machine

def get_drive_service():
    """Returns an authenticated Google Drive service instance."""
    credentials = service_account.Credentials.from_service_account_file('/tmp/credentials.json')
    return build('drive', 'v3', credentials=credentials)

def download_existing_csv(drive_service):
    """Downloads the existing CSV from Google Drive to the local system."""
    query = f"name='waffle_house_data.csv' and '{FOLDER_ID}' in parents"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)', pageSize=1).execute()
    files = response.get('files', [])

    if files:
        # File exists; download it
        file_id = files[0]['id']
        request = drive_service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        fh.seek(0)

        # Save to local path
        with open(LOCAL_CSV_PATH, 'wb') as f:
            f.write(fh.read())
        print("Existing CSV downloaded successfully.")
    else:
        print("No existing CSV found. A new file will be created.")

def scrape_waffle_house_data():
    url = 'https://locations.wafflehouse.com'  # Replace with the actual URL if needed
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    # Send the GET request
    response = requests.get(url, headers=headers)

    # Check if the request was successful
    if response.status_code == 200:
        raw_html = response.text  # Get the raw HTML from the page

        # Step 2: Extract the JSON from the __NEXT_DATA__ script tag using regex
        pattern = r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>'
        match = re.search(pattern, raw_html, re.DOTALL)

        if match:
            # Step 3: Parse the JSON data
            json_data = match.group(1)
            try:
                data = json.loads(json_data)

                # Step 4: Extract the locations data
                locations = data['props']['pageProps']['locations']

                # Create a list to hold all the store information
                store_info_list = []

                # Extract relevant details for each store
                for location in locations:
                    store_info = {
                        'storeCode': location['storeCode'],
                        'businessName': location['businessName'],
                        'addressLines': location['addressLines'],
                        'city': location['city'],
                        'state': location['state'],
                        'country': location['country'],
                        'operated_by': location['custom']['operated_by'],
                        'postalCode': location['postalCode'],
                        'latitude': location['latitude'],
                        'longitude': location['longitude'],
                        'phoneNumbers': location['phoneNumbers'],
                        'websiteURL': location['websiteURL'],
                        'businessHours': location['businessHours'],
                        'formattedBusinessHours': location['formattedBusinessHours'],
                        'slug': location['slug'],
                        'localPageUrl': location['localPageUrl'],
                        '_status': location['_status'],
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # Add timestamp column
                    }
                    store_info_list.append(store_info)

                # Convert the list of dictionaries to a Pandas DataFrame
                df_new = pd.DataFrame(store_info_list)

                # Authenticate with Google Drive and download existing CSV
                drive_service = get_drive_service()
                download_existing_csv(drive_service)

                # Append new data to the existing CSV
                if os.path.exists(LOCAL_CSV_PATH):
                    df_existing = pd.read_csv(LOCAL_CSV_PATH)
                    df_combined = pd.concat([df_existing, df_new], ignore_index=True)
                else:
                    df_combined = df_new

                # Save the updated CSV locally
                df_combined.to_csv(LOCAL_CSV_PATH, index=False)

                # Upload the updated CSV back to Google Drive
                upload_to_google_drive(LOCAL_CSV_PATH, drive_service)

            except json.JSONDecodeError:
                print("Error decoding JSON data.")
        else:
            print("JSON data not found on the page.")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

def upload_to_google_drive(file_path, drive_service):
    """Uploads or updates the CSV file on Google Drive."""
    query = f"name='waffle_house_data.csv' and '{FOLDER_ID}' in parents"
    response = drive_service.files().list(q=query, spaces='drive', fields='files(id, name)', pageSize=1).execute()
    files = response.get('files', [])

    if files:
        # File exists; update it
        file_id = files[0]['id']
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().update(fileId=file_id, media_body=media).execute()
        print("CSV updated successfully in Google Drive.")
    else:
        # File does not exist; create a new one
        file_metadata = {'name': 'waffle_house_data.csv', 'parents': [FOLDER_ID]}
        media = MediaFileUpload(file_path, mimetype='text/csv')
        drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        print("New CSV uploaded successfully to Google Drive.")

# Call the function to run the scraper and upload the CSV
scrape_waffle_house_data()
