import requests
import re
import json
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# Set your Google Drive folder ID here
folder_id = '1yTC_EqKSAkMbK7ftPFrDnYmJBCVS_fe8'  # Replace with your folder ID

# Path to the CSV file on the local system before uploading to Google Drive
LOCAL_CSV_PATH = '/tmp/waffle_house_data.csv'  # Use a temporary location on GitHub Actions or local machine

def get_drive_service():
    """Returns an authenticated Google Drive service instance."""
    credentials = service_account.Credentials.from_service_account_file('/tmp/credentials.json')
    return build('drive', 'v3', credentials=credentials)

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

                # Save the new CSV to the local path
                file_name = f"waffle_house_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv".replace(":", "_")
                df_new.to_csv(LOCAL_CSV_PATH, index=False)

                # Upload the new CSV file to Google Drive
                upload_to_google_drive(LOCAL_CSV_PATH, drive_service, file_name)

            except json.JSONDecodeError:
                print("Error decoding JSON data.")
        else:
            print("JSON data not found on the page.")
    else:
        print(f"Failed to retrieve the page. Status code: {response.status_code}")

def upload_to_google_drive(file_path, drive_service, fn):
    """Uploads a new CSV file to Google Drive."""
    # Set the file metadata with the target folder ID
    file_metadata = {
        'name': fn,
        'parents': [folder_id]  # The folder ID you provided
    }

    # Upload the CSV to Google Drive
    media = MediaFileUpload(file_path, mimetype='text/csv')
    drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

    print(f"New CSV uploaded successfully: {fn}")

# Call the function to run the scraper and upload the CSV
scrape_waffle_house_data()
