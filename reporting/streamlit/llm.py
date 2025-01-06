import streamlit as st
import duckdb
import json
from vertexai.generative_models import GenerativeModel, Content, Part, GenerationConfig
import networkx as nx 
import matplotlib.pyplot as plt
from google.cloud import secretmanager
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from vertexai.language_models import TextEmbeddingInput, TextEmbeddingModel
from vertexai.generative_models import GenerationConfig
import base64
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
# Title for the page
st.title("**311 Service Genius: Effortlessly Generate Service Request Queries!**")

# Styling
st.markdown("""
    <style>
        .reportview-container {
            background-color: #f9f9f9;
        }
        .sidebar .sidebar-content {
            background-color: #3b3b3b;
        }
        h1 {
            color: #2A7E8C;
            font-size: 40px;
            font-weight: bold;
        }
        h3 {
            color: #0077cc;
        }
        .stTextInput textarea {
            font-size: 16px;
            background-color: #f1f1f1;
            border-radius: 8px;
            padding: 12px;
        }
        .stButton button {
            background-color: #2A7E8C;
            color: white;
            font-size: 18px;
            border-radius: 8px;
            padding: 12px;
        }
        .stButton button:hover {
            background-color: #0077cc;
        }
        .stMarkdown {
            font-size: 16px;
            color: #333;
        }
        .stDataFrame {
            border: 1px solid #ddd;
            border-radius: 8px;
            margin-top: 20px;
        }
    </style>
""", unsafe_allow_html=True)

# Setup
GCP_PROJECT = 'group2-ba882'
project_id = 'group2-ba882'
GCP_REGION = "us-central1"
version_id = 'latest'

# Secret Manager for MotherDuck token
sm = secretmanager.SecretManagerServiceClient()
vector_name = f"projects/{project_id}/secrets/project_key/versions/latest"
response = sm.access_secret_version(request={"name": vector_name})
md_token = response.payload.data.decode("UTF-8")

# Initialize MotherDuck connection
conn = duckdb.connect(f'md:city_services_boston?motherduck_token={md_token}')

# Define table info - using stage schema
schema_name = "stage"
tables = ["case_duration", "department_assignment", "locations", 
          "requests", "response_time", "status_history"]

# Initialize GenAI model
model = GenerativeModel(model_name="gemini-1.5-flash-002")
generation_config = GenerationConfig(temperature=0, response_mime_type="application/json")

# Streamlit App UI
st.markdown("""
    <p style="color: yellow; font-size: 18px; font-weight: bold;">
        **Leverage the power of SQL for 311 Service Request Data**  
        Provide a brief description of your query, and our AI will generate an optimized SQL query for you in just seconds.  
        Choose a relevant service request table, outline your needs, and let our AI streamline the process to deliver precise insights.
    </p>
""", unsafe_allow_html=True)


# Add table selection dropdown
selected_table = st.selectbox("📝 Select the Table to Query:", tables, key="table_select")
fully_qualified_table = f"city_services_boston.stage.{selected_table}"

# User input prompt
user_input = st.text_area("💬 What Do You Need? Describe Your Query in Plain Language:", height=150)

# Handle user input
if st.button("Generate & Run SQL Query"):
    if user_input:
        # Fetch schema from MotherDuck
        try:
            schema_query = f"DESCRIBE {fully_qualified_table}"
            schema_result = conn.execute(schema_query).fetchall()
            schema = {row[0]: row[1] for row in schema_result}
        except Exception as e:
            st.error(f"Oops! Something went wrong while fetching the schema: {e}")
            schema = {}

        # Construct the schema description and prompt
        if schema:
            schema_description = "\n".join([f"{name} ({dtype})" for name, dtype in schema.items()])
            prompt = f"""
            You are an expert SQL developer. Based on the user's request and the provided schema, generate a valid SQL query.
            Use the table `{fully_qualified_table}`. Ensure the query references the table and its columns correctly.
            Return the result as a valid JSON with a key `SQL` containing the SQL query.

            ### Schema
            {schema_description}

            ### User Request
            {user_input}

            ### SQL Query (as JSON)
            """

            # Use GenAI to generate the SQL query
            user_prompt_content = Content(
                role="user",
                parts=[Part.from_text(prompt)],
            )

            try:
                response = model.generate_content(user_prompt_content, generation_config=generation_config)

                # Extract the generated SQL query from the response
                llm_query = json.loads(response.text)
                sql_query = llm_query.get("SQL")

                if sql_query:
                    # Display the generated SQL query
                    st.markdown("### 🔍 **Generated SQL Query**")
                    st.code(sql_query, language='sql')

                    # Execute the SQL query against MotherDuck
                    try:
                        result = conn.execute(sql_query).fetchdf()

                        # Remove rows with null values
                        df_cleaned = result.dropna()

                        # Display the query results
                        st.markdown("### 📊 **Query Results**")
                        st.dataframe(df_cleaned, use_container_width=True)  # Show result table
                    except Exception as e:
                        st.error(f"😓 An error occurred while running the query: {e}")
                else:
                    st.error("No SQL query was generated. Please check your input.")
            except Exception as e:
                st.error(f"⚠️ Something went wrong with generating the SQL query: {e}")
        else:
            st.error("Failed to fetch schema from MotherDuck. Please try again later.")
    else:
        st.warning("⚠️ Please enter a query description to get started.")

# Close the MotherDuck connection
conn.close()
