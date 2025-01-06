import functions_framework
from google.cloud import secretmanager
import duckdb

# settings
project_id = 'group2-ba882'
secret_id = 'project_key'   #<---------- this is the name of the secret you created
version_id = 'latest'

# db setup
db = 'city_services_boston'
schema = "stage"
db_schema = f"{db}.{schema}"

@functions_framework.http
def task(request):

    # instantiate the services 
    sm = secretmanager.SecretManagerServiceClient()

    # Build the resource name of the secret version
    name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"

    # Access the secret version
    response = sm.access_secret_version(request={"name": name})
    md_token = response.payload.data.decode("UTF-8")

    # initiate the MotherDuck connection through an access token through
    md = duckdb.connect(f'md:?motherduck_token={md_token}') 

    ##################################################### create the schema

    # define the DDL statement with an f string
    create_db_sql = f"CREATE DATABASE IF NOT EXISTS {db};"   

    # execute the command to create the database
    md.sql(create_db_sql)

    # confirm it exists
    print(md.sql("SHOW DATABASES").show())

    # create the schema
    md.sql(f"CREATE SCHEMA IF NOT EXISTS {db_schema};") 

    ##################################################### create the core tables in stage

    # Requests Table
    raw_tbl_name = f"{db_schema}.requests"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        _id INT 
        ,case_enquiry_id VARCHAR
        ,case_title VARCHAR
        ,subject VARCHAR
        ,reason VARCHAR
        ,type VARCHAR
        ,queue VARCHAR
        ,source VARCHAR
        ,submitted_photo VARCHAR
        ,closed_photo VARCHAR
        ,PRIMARY KEY (case_enquiry_id)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # locations
    raw_tbl_name = f"{db_schema}.locations"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        location VARCHAR
        ,fire_district VARCHAR
        ,pwd_district VARCHAR
        ,city_council_district VARCHAR
        ,police_district VARCHAR
        ,neighborhood VARCHAR
        ,neighborhood_services_district VARCHAR
        ,ward VARCHAR
        ,precinct VARCHAR
        ,location_street_name VARCHAR
        ,location_zipcode VARCHAR
        ,latitude FLOAT
        ,longitude FLOAT
        ,geom_4326 GEOMETRY
        ,PRIMARY KEY (location)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # department_assignment
    raw_tbl_name = f"{db_schema}.department_assignment"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        case_enquiry_id VARCHAR
        ,department VARCHAR
        ,PRIMARY KEY (case_enquiry_id, department)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    # status_history
    raw_tbl_name = f"{db_schema}.status_history"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        case_enquiry_id VARCHAR
        ,open_dt TIMESTAMP
        ,sla_target_dt TIMESTAMP
        ,closed_dt TIMESTAMP
        ,case_status VARCHAR
        ,closure_reason VARCHAR
        ,PRIMARY KEY (case_enquiry_id, open_dt)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

        # response_time
    raw_tbl_name = f"{db_schema}.response_time"
    raw_tbl_sql = f"""
    CREATE TABLE IF NOT EXISTS {raw_tbl_name} (
        case_enquiry_id VARCHAR
        ,on_time BOOLEAN
        ,PRIMARY KEY (case_enquiry_id)
    );
    """
    print(f"{raw_tbl_sql}")
    md.sql(raw_tbl_sql)

    
    return {}, 200