from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd
import plotly.express as px

SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
SERVICE_ACCOUNT_FILE = "/tmp/credentials.json"
FILE_ID = "1YUJd4T8w17YNSn_l5x8409_wvA908jYH"

credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES
)
service = build("drive", "v3", credentials=credentials)

request = service.files().get_media(fileId=FILE_ID)
with open("waffle_house.csv", "wb") as f:
    f.write(request.execute())

# Now your plotting code reads "waffle_house.csv"
df = pd.read_csv("waffle_house.csv")

# 2. Ensure timestamp is datetime and extract the date only
df['timestamp'] = pd.to_datetime(df['timestamp'])
df['date'] = df['timestamp'].dt.date

# 3. Map _status to colors
status_color = {'A': 'green', 'CT': 'red', 'OS': 'blue'}
df['color'] = df['_status'].map(status_color)

# 4. Create the animated scatter map
fig = px.scatter_geo(
    df,
    lat='latitude',
    lon='longitude',
    color='_status',  # colors assigned automatically; or use 'color' column
    color_discrete_map=status_color,  # enforce your color mapping
    hover_name='storeCode',
    animation_frame=df['date'].astype(str),  # animation by date
    scope='usa',
    title='Waffle House Status Over Time'
)

# 5. Show the map
fig.show(renderer="browser")