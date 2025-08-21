import pandas as pd
import plotly.express as px

# 1. Load your dataset
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
fig.show()