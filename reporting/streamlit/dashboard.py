import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import base64
from streamlit_lottie import st_lottie
import json
from google.cloud import secretmanager

# Secret manager and database connection setup
project_id = 'group2-ba882'
region_id = "us-east-1"
secret_id = 'project_key'
version_id = 'latest'
db = 'city_services_boston'
schema = "stage"
db_schema = f"{db}.{schema}"

# Secret manager setup
sm = secretmanager.SecretManagerServiceClient()
name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
response = sm.access_secret_version(request={"name": name})
db_token = response.payload.data.decode("UTF-8")
conn = duckdb.connect(f'md:?token={db_token}')

# Load Lottie animation
# def load_lottiefile(filepath: str):
#     with open(filepath, "r") as f:
#         return json.load(f)
# Set the background image using CSS
# Function to load and encode the background image
def load_background_image(image_file):
    with open(image_file, "rb") as f:
        data = base64.b64encode(f.read()).decode()
    return data
background_image = load_background_image("Boston city.webp")
st.markdown(f"""
    <style>
    .stApp {{
        background-image: url("data:image/webp;base64,{background_image}");
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
    }}
    </style>
""", unsafe_allow_html=True)
# Background image loading
# def load_background_image(image_file):
#     with open(image_file, "rb") as f:
#         data = base64.b64encode(f.read()).decode()
#     return data

# Fetch service request types from DuckDB
def get_request_types():
    query = "SELECT DISTINCT type FROM city_services_boston.stage.case_duration"
    types_df = conn.execute(query).df()
    return types_df['type'].tolist()

# Set page config
#st.set_page_config(page_title="Boston 311 Analytics Dashboard", layout="wide")

# Load and encode background image
#background_image = load_background_image("boston_skyline.jpg")

# Apply Boston styling
st.markdown(f"""
<style>
.stApp {{
    background-size: cover;
    background-position: center;
    background-repeat: no-repeat;
}}
body {{
    color: white;
}}
.stSelectbox {{
    color: white;
}}
.stButton>button {{
    color: white;
    background-color: #FFD700;  /* Changed button color to yellow */;
    border-color: #FFD700;
}}
.stButton>button:hover {{
    color: white;
    background-color: #FFC107;  /* Slightly darker yellow on hover */
    border-color: #FFC107;
}}
.css-1d391kg {{
    background-color: rgba(0, 0, 0, 0.05);
}}
[data-testid="stDataFrame"] {{
    width: 80% !important;
    margin: auto;
}}
.block-container {{
    max-width: 1200px;
    padding-top: 1rem;
    padding-bottom: 1rem;
    margin: auto;
}}
</style>
""", unsafe_allow_html=True)

# Boston logo and title
#st.image("https://www.boston.gov/sites/default/files/img/b/boston-black.svg", width=100)
st.write("""<h1 style="color:white;">Boston 311 Analytics Dashboard</h1>""", unsafe_allow_html=True)

# Load Lottie animation
# Replace animation with text
col1, col2, col3 = st.columns([1, 2, 1])
with col2:
    st.markdown("<h2 style='text-align: center;'>Welcome to Boston City Services</h2>", unsafe_allow_html=True)

# Create dashboard tabs
tab1, tab2, tab3 = st.tabs(["Service Request Types 📊", "Response Time Analysis ⏱️", "Neighborhood Insights 🏙️"])

with tab1:
    st.header('Top Service Request Types')
    
    # Source filter
    source_query = """
    SELECT DISTINCT reason 
    FROM city_services_boston.stage.case_duration
    WHERE reason IS NOT NULL 
    ORDER BY reason
    """
    sources = conn.execute(source_query).df()['reason'].tolist()
    selected_source = st.selectbox("Select Reason", ['All'] + sources)
    
    # Query for top service request types
    base_query = """
    SELECT type, COUNT(*) as count
    FROM city_services_boston.stage.case_duration
    """
    
    if selected_source != 'All':
        query = f"""
        {base_query}
        WHERE reason = '{selected_source}'
        GROUP BY type
        ORDER BY count DESC
        LIMIT 10
        """
    else:
        query = f"""
        {base_query}
        GROUP BY type
        ORDER BY count DESC
        LIMIT 10
        """
    
    top_types = conn.execute(query).df()
    
    # Create bar chart with yellow colour scheme
    fig = px.bar(top_types, x='count', y='type', orientation='h',
                 title='Top 10 Service Request Types',

                 labels={'count': 'Number of Requests', 'type': 'Request Type'},
                 color_discrete_sequence=['#FFD700'])  # changed to yellow
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig)

with tab2:
    st.header('Response Time Analysis')
    
    # Service type filter
    service_types = get_request_types()
    selected_type = st.selectbox("Select Service Type", service_types)
    
    # Query for response times
    query = f"""
    SELECT 
        DATE_TRUNC('month', open_dt) as month,
        AVG(DATEDIFF('hour', open_dt, closed_dt)) as avg_response_time
    FROM city_services_boston.stage.case_duration
    WHERE type = '{selected_type}'
    GROUP BY month
    ORDER BY month
    """
    response_times = conn.execute(query).df()
    
    # Create line chart
    fig = px.line(response_times, x='month', y='avg_response_time',
                  title=f'Average Response Time for {selected_type}',
                  labels={'month': 'Month', 'avg_response_time': 'Average Response Time (hours)'},
                  color_discrete_sequence=['#FFD700']) #for yellow
    st.plotly_chart(fig)

with tab3:
    st.header('Neighborhood Insights')
    
    # Query for location data with coordinates
    query = """
    SELECT 
        neighborhood,
        COUNT(*) as count,
        AVG(latitude) as lat,
        AVG(longitude) as lon
    FROM city_services_boston.stage.locations
    WHERE latitude IS NOT NULL 
    AND longitude IS NOT NULL
    GROUP BY neighborhood
    ORDER BY count DESC
    """
    neighborhood_data = conn.execute(query).df()
    

    # Create scatter mapbox
    fig = px.scatter_mapbox(
        neighborhood_data,
        lat='lat',
        lon='lon',
        size='count',
        color='count',
        hover_name='neighborhood',
        hover_data={'count': True, 'lat': False, 'lon': False},
        color_continuous_scale='YlGnBu',
        zoom=11,
        title='Service Requests by Neighborhood',
        size_max=40,
        mapbox_style='carto-positron'
    )
    
    # Update layout
    fig.update_layout(
        margin={"r":0,"t":30,"l":0,"b":0},
        mapbox=dict(
            center=dict(lat=42.3601, lon=-71.0589),  # Boston coordinates
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

    # # Create choropleth map
    # fig = px.choropleth(neighborhood_data,
    #                     geojson="https://raw.githubusercontent.com/codeforboston/boston-neighborhoods/main/boston_neighborhoods.geojson",
    #                     locations='neighborhood',
    #                     color='count',
    #                     featureidkey="properties.Name",
    #                     color_continuous_scale="YlGnBu",
    #                     title='Service Requests by Neighborhood')
    # fig.update_geos(fitbounds="locations", visible=False)
    # st.plotly_chart(fig)

    # Top 5 neighborhoods table
    st.subheader("Top 5 Neighborhoods by Service Requests")
    st.table(neighborhood_data.head())