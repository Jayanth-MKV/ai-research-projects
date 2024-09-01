import psycopg2
from psycopg2 import sql
import pandas as pd
from typing import Dict
connection_params = {
        "host": "127.0.0.1",
        "database": "db",
        "user": "root",
        "password": "root"
    }
# Example DataFrames function (from your previous code)
def create_sample_dataframes() -> Dict[str, pd.DataFrame]:
    events_df = pd.DataFrame({
    'event_url': ['e1', 'e2', 'e3', 'e4', 'e5'],
    'event_name': ['Tech Conf', 'Oil Expo', 'Green Energy', 'TEch', 'Data Summit'],
    'event_start_date': ['2023-09-01', '2023-10-15', '2023-11-20', '2023-11-20', '2023-12-05'],
    'event_city': ['San Francisco', 'Houston', 'Berlin', 'Berlin', 'New York'],
    'event_country': ['USA', 'USA', 'Germany', 'Germany', 'USA'],
    'event_industry': ['Technology', 'Oil & Gas', 'Renewable Energy', 'Technology', 'Technology']
    })

    event_attendees_df = pd.DataFrame({
        'event_url': ['e1', 'e1', 'e2', 'e2', 'e3', 'e4', 'e4', 'e5'],
        'company_url': ['c1', 'c2', 'c3', 'c4', 'c2', 'c1', 'c3', 'c4'],
        'company_relation_to_event': ['Sponsor', 'Attendee', 'Sponsor', 'Attendee', 'Sponsor', 'Attendee', 'Sponsor', 'Sponsor']
    })

    companies_df = pd.DataFrame({
        'company_url': ['c1', 'c2', 'c3', 'c4'],
        'company_name': ['TechCorp', 'OilGiant', 'GreenEnergy', 'DataFirm'],
        'company_industry': ['Technology', 'Oil & Gas', 'Renewable Energy', 'Technology'],
        'company_revenue': [1000000, 5000000, 2000000, 3000000],
        'company_country': ['USA', 'USA', 'Germany', 'USA']
    })

    company_contacts_df = pd.DataFrame({
        'company_url': ['c1', 'c2', 'c3', 'c4'],
        'office_city': ['San Francisco', 'Houston', 'Berlin', 'New York'],
        'office_country': ['USA', 'USA', 'Germany', 'USA'],
        'office_address': ['123 Tech St', '456 Oil Ave', '789 Green Rd', '101 Data Ln'],
        'office_email': ['contact@techcorp.com', 'info@oilgiant.com', 'hello@greenenergy.de', 'support@datafirm.com']
    })

    employees_df = pd.DataFrame({
        'company_url': ['c1', 'c1', 'c2', 'c2', 'c3', 'c3', 'c4'],
        'person_id': ['p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'],
        'person_first_name': ['John', 'Jane', 'Bob', 'Alice', 'Max', 'Anna', 'Tom'],
        'person_last_name': ['Doe', 'Smith', 'Johnson', 'Brown', 'Mueller', 'Schmidt', 'Davis'],
        'person_email': ['john@techcorp.com', 'jane@techcorp.com', 'bob@oilgiant.com', 'alice@oilgiant.com', 'max@greenenergy.de', 'anna@greenenergy.de', 'tom@datafirm.com'],
        'person_city': ['San Francisco', 'San Francisco', 'Houston', 'Houston', 'Berlin', 'Berlin', 'New York'],
        'person_country': ['USA', 'USA', 'USA', 'USA', 'Germany', 'Germany', 'USA'],
        'person_seniority': ['Director', 'Manager', 'Director', 'Engineer', 'Manager', 'Director', 'Manager'],
        'person_department': ['Engineering', 'Marketing', 'Operations', 'Engineering', 'Sales', 'Engineering', 'Data Science']
    })


    return {
        "events": events_df,
        "event_attendees": event_attendees_df,
        "companies": companies_df,
        "company_contact": company_contacts_df,
        "people": employees_df
    }

def transform_to_sql_format(df: pd.DataFrame, key_column: str, value_columns: list) -> list:
    """Transform DataFrame to a list of tuples for SQL insertion."""
    result = []
    for _, row in df.iterrows():
        for col in value_columns:
            result.append((row[key_column], col, row[col]))
    return result

def create_tables(cursor):
    # Create event_attributes table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS event_attributes (
        event_url TEXT,
        attribute TEXT,
        value TEXT
    );
    """)

    # Create company_attributes table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS company_attributes (
        company_url TEXT,
        attribute TEXT,
        value TEXT
    );
    """)

    # Create people_attributes table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS people_attributes (
        person_id TEXT,
        attribute TEXT,
        value TEXT
    );
    """)

def insert_data(cursor, data):
    # Insert event data
    insert_event_query = sql.SQL("""
    INSERT INTO event_attributes (event_url, attribute, value)
    VALUES (%s, %s, %s);
    """)
    cursor.executemany(insert_event_query, data['event_attributes'])

    # Insert company data
    insert_company_query = sql.SQL("""
    INSERT INTO company_attributes (company_url, attribute, value)
    VALUES (%s, %s, %s);
    """)
    cursor.executemany(insert_company_query, data['company_attributes'])

    # Insert people data
    insert_people_query = sql.SQL("""
    INSERT INTO people_attributes (person_id, attribute, value)
    VALUES (%s, %s, %s);
    """)
    cursor.executemany(insert_people_query, data['people_attributes'])

def main():
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Create tables
        create_tables(cursor)

        # Create sample dataframes
        dataframes = create_sample_dataframes()

        # Transform DataFrames to SQL format
        data = {
            'event_attributes': transform_to_sql_format(dataframes['events'], 'event_url', ['event_name', 'event_start_date', 'event_city', 'event_country', 'event_industry']),
            'company_attributes': transform_to_sql_format(dataframes['companies'], 'company_url', ['company_name', 'company_industry', 'company_revenue', 'company_country']),
            'people_attributes': transform_to_sql_format(dataframes['people'], 'person_id', ['company_url', 'person_first_name', 'person_last_name', 'person_email', 'person_city', 'person_country', 'person_seniority', 'person_department'])
        }

        # Insert sample data
        insert_data(cursor, data)

        # Commit changes
        conn.commit()
        print("Sample data inserted successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
