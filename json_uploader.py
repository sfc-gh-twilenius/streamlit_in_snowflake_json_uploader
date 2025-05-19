import streamlit as st
import snowflake.connector
import json
import pandas as pd
from io import StringIO
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
from snowflake.snowpark.types import StructType, StructField, StringType, IntegerType, TimestampType, VariantType

# Streamlit configuration
st.set_page_config(page_title="JSON File Uploader to Snowflake", page_icon=":snowflake:")

# Function to establish Snowpark session using get_active_session

# Function to create table if it doesn't exist
def create_table_if_not_exists(table_name: str):
    """
    Creates the Snowflake table if it does not exist using Snowpark.
    Includes file metadata columns.
    Handles potential errors and displays them in the Streamlit UI.

    Args:
        session: The Snowpark session.
        table_name: The name of the table to create.
    """
    session = get_active_session()
    try:
        # Define the schema for the table, *without* upload_timestamp
        schema = StructType([
            StructField("file_name", StringType()),
            StructField("file_size_bytes", IntegerType()),
            StructField("json_content", VariantType())
        ])

        # Create the table if it does not exist
        session.sql(f"CREATE TABLE IF NOT EXISTS {table_name} (file_name VARCHAR, file_size_bytes INTEGER, json_content VARIANT)").collect()

        st.success(f"Table '{table_name}' created (if it did not exist).")
    except Exception as e:
        st.error(f"Error creating table: {e}")

# Function to insert data into Snowflake using Snowpark
def insert_json_data(table_name: str, file_name: str, file_size_bytes: int, json_content):
    """
    Inserts the JSON data and metadata into the Snowflake table using Snowpark.
    Handles potential errors and displays them in the Streamlit UI.
    *Excludes* upload_timestamp.

    Args:
        session: The Snowpark session.
        table_name: Name of the table to insert into.
        file_name: Name of the uploaded file.
        file_size_bytes: Size of the uploaded file in bytes.
        json_content: The JSON data as a Python object (dict or list).
    """
    session = get_active_session()
    try:
        # Create a DataFrame with the data to insert, *without* upload_timestamp
        df = session.create_dataframe(
            [[file_name, file_size_bytes, json_content]],
            schema=["file_name", "file_size_bytes", "json_content"]
        )

        # Use Snowpark to write the DataFrame to the table
        df.write.mode("append").save_as_table(table_name)
        st.success(f"Data from file '{file_name}' successfully inserted into table '{table_name}'.")
    except Exception as e:
        st.error(f"Error inserting data: {e}")

# Main function
def main():
    """
    Main function to run the Streamlit app.
    """
    # Get Snowpark session
    session = get_active_session()

    # Get a logo for you streamlit app, upload the photo to a snowflake stage
    image=session.file.get_stream('@"STREAMLIT"."PUBLIC"."PHOTOS"/valtiokonttori.png' , decompress=False).read()
    st.image(image, width=400) 
    st.title("Upload JSON to Snowflake with Snowpark")

 
    
    if session is None:
        return  # Stop if session fails

    # Get table name from user input
    table_name = st.text_input("Enter the name of the Snowflake table (it will be created if it does not exist):", "my_json_table")

    # Create table if it doesn't exist
    create_table_if_not_exists(table_name)

    # File uploader
    uploaded_files = st.file_uploader("Upload JSON files", type=["json"], accept_multiple_files=True)

    if uploaded_files:
        for uploaded_file in uploaded_files:
            # Read file metadata
            file_name = uploaded_file.name
            file_size_bytes = len(uploaded_file.getvalue())
            upload_timestamp = pd.Timestamp.now()

            # Read JSON content, handling potential errors in JSON structure
            try:
                json_content = json.load(uploaded_file)  # Use json.load() directly
            except json.JSONDecodeError as e:
                st.error(f"Error: Invalid JSON format in file '{file_name}': {e}")
                continue  # Skip to the next file

            # Insert data into Snowflake
            insert_json_data(table_name, file_name, file_size_bytes, json_content)

        # Option to display data
        if st.checkbox("Show data from Snowflake table"):
            try:
                # Use Snowpark to read the data from the table
                df = session.table(table_name)
                st.dataframe(df.to_pandas())  # Display the DataFrame

                # Additionally, show JSON content in expanded format (optional, for detailed view)
                st.write("JSON Content from Snowflake (first 5 rows):")
                if 'json_content' in df.columns:
                    for i in range(min(5, df.count())):  # Limit to first 5 rows
                        try:
                            # Use to_pandas() to convert the Snowpark row to a Pandas Series, then access the 'json_content'
                            json_content_dict = json.loads(df.select(col("json_content")).collect()[i][0])
                            st.json(json_content_dict)
                        except (TypeError, json.JSONDecodeError) as e:
                            st.write(f"Error displaying JSON for row {i+1}: {e}.  The data in Snowflake may not be a valid JSON string.")
                            st.write(df.select(col("json_content")).collect()[i][0])
                else:
                    st.write("The 'json_content' column does not exist in the table.")


            except Exception as e:
                st.error(f"Error fetching data from Snowflake: {e}")

    if session:
        session.close()

if __name__ == "__main__":
    main()
