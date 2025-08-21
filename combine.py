import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload
import io
import plotly.express as px

# -------------------------------
# CONFIG
# -------------------------------
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
SERVICE_ACCOUNT_FILE = "/tmp/credentials.json"
DAILY_FOLDER_ID = "1yTC_EqKSAkMbK7ftPFrDnYmJBCVS_fe8"
COMBINED_FOLDER_ID = "10iofKpOAVkhJx_Tnr9-WvyFTKV_oZd0x"
COMBINED_CSV = "waffle_house.csv"   # name of combined CSV
NEW_CSV = "daily_waffle_house.csv"  # local name for latest daily CSV

# -------------------------------
# AUTH
# -------------------------------
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("drive", "v3", credentials=credentials)

# -------------------------------
# DOWNLOAD latest daily CSV
# -------------------------------
query = f"'{DAILY_FOLDER_ID}' in parents and trashed=false"
results = service.files().list(q=query, orderBy="createdTime desc", pageSize=1, fields="files(id, name)").execute()
files = results.get("files", [])

if not files:
    raise Exception("No daily CSV found in DAILY_FOLDER_ID")

daily_file_id = files[0]['id']
request = service.files().get_media(fileId=daily_file_id)
fh = io.BytesIO()
downloader = MediaIoBaseDownload(fh, request)
done = False
while not done:
    status, done = downloader.next_chunk()
fh.seek(0)
df_new = pd.read_csv(fh)
print(f"Downloaded latest daily CSV: {files[0]['name']}")

# -------------------------------
# DOWNLOAD previous combined CSV if it exists
# -------------------------------
query = f"name='{COMBINED_CSV}' and '{COMBINED_FOLDER_ID}' in parents and trashed=false"
results = service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if files:
    combined_file_id = files[0]['id']
    request = service.files().get_media(fileId=combined_file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    df_combined = pd.read_csv(fh)
    print("Downloaded previous combined CSV.")
else:
    df_combined = pd.DataFrame()
    print("No previous combined CSV found, starting fresh.")

# -------------------------------
# APPEND new CSV to combined
# -------------------------------
df_combined = pd.concat([df_combined, df_new], ignore_index=True)
df_combined.to_csv(COMBINED_CSV, index=False)
print(f"Updated combined CSV saved locally as {COMBINED_CSV}")

# -------------------------------
# UPLOAD updated combined CSV back to Drive
# -------------------------------
media = MediaFileUpload(COMBINED_CSV, mimetype="text/csv")
if files:
    service.files().update(fileId=combined_file_id, media_body=media).execute()
    print(f"Updated existing file on Drive: {COMBINED_CSV}")
else:
    file_metadata = {"name": COMBINED_CSV, "parents": [COMBINED_FOLDER_ID]}
    service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"Uploaded new combined file to Drive: {COMBINED_CSV}")

# -------------------------------
# CREATE ANIMATED PLOTLY MAP
# -------------------------------
df_combined['timestamp'] = pd.to_datetime(df_combined['timestamp'])
df_combined['date'] = df_combined['timestamp'].dt.date

status_color = {'A': 'green', 'CT': 'red', 'OS': 'blue'}
df_combined['color'] = df_combined['_status'].map(status_color)

fig = px.scatter_geo(
    df_combined,
    lat='latitude',
    lon='longitude',
    color='_status',
    color_discrete_map=status_color,
    hover_name='storeCode',
    animation_frame=df_combined['date'].astype(str),
    scope='usa',
    title='Waffle House Status Over Time'
)

MAP_HTML = "waffle_house_map.html"
fig.write_html(MAP_HTML)
print(f"Interactive map saved locally as {MAP_HTML}")

# -------------------------------
# UPLOAD MAP HTML TO DRIVE
# -------------------------------
media = MediaFileUpload(MAP_HTML, mimetype="text/html")
# Check if map already exists in the combined folder
query = f"name='{MAP_HTML}' and '{COMBINED_FOLDER_ID}' in parents and trashed=false"
results = service.files().list(q=query, fields="files(id, name)").execute()
files = results.get("files", [])

if files:
    map_file_id = files[0]['id']
    service.files().update(fileId=map_file_id, media_body=media).execute()
    print(f"Updated existing map on Drive: {MAP_HTML}")
else:
    file_metadata = {"name": MAP_HTML, "parents": [COMBINED_FOLDER_ID]}
    service.files().create(body=file_metadata, media_body=media, fields="id").execute()
    print(f"Uploaded new map to Drive: {MAP_HTML}")