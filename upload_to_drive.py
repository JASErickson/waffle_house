from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload

# Constants
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
SERVICE_ACCOUNT_FILE = "/tmp/credentials.json"
FOLDER_ID = "10iofKpOAVkhJx_Tnr9-WvyFTKV_oZd0x"
CSV_FILE = "waffle_house.csv"  # Name of your combined CSV

# Authenticate
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("drive", "v3", credentials=credentials)

# Prepare the file to upload
media = MediaFileUpload(CSV_FILE, mimetype="text/csv")

# Check if a file with the same name already exists in the folder
query = f"name='{CSV_FILE}' and '{FOLDER_ID}' in parents and trashed=false"
results = service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if files:
    # File exists → update it
    file_id = files[0]['id']
    service.files().update(fileId=file_id, media_body=media).execute()
    print(f"Updated existing file: {CSV_FILE}")
else:
    # File does not exist → create it
    file_metadata = {"name": CSV_FILE, "parents": [FOLDER_ID]}
    service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"Uploaded new file: {CSV_FILE}")