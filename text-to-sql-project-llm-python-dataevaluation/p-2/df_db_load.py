import psycopg2
import pandas as pd
from psycopg2 import sql

# Connection parameters
connection_params = {
        "host": "127.0.0.1",
        "database": "db",
        "user": "root",
        "password": "root"
    }


# Sample DataFrames
events_df = pd.DataFrame({
    'event_url': ['e1', 'e2', 'e3', 'e4', 'e5'],
    'event_name': ['Tech Conf', 'Oil Expo', 'Green Energy', 'TEch', 'Data Summit'],
    'event_start_date': ['2023-09-01', '2023-10-15', '2023-11-20', '2023-11-20', '2023-12-05'],
    'event_city': ['San Francisco', 'Houston', 'Berlin', 'Berlin', 'New York'],
    'event_country': ['USA', 'USA', 'Germany', 'Germany', 'USA'],
    'event_industry': ['Technology', 'Oil & Gas', 'Renewable Energy', 'Technology', 'Technology']
})

attendees_df = pd.DataFrame({
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

def create_tables(cursor):
    # Create events table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_url TEXT PRIMARY KEY,
        event_name TEXT,
        event_start_date DATE,
        event_city TEXT,
        event_country TEXT,
        event_industry TEXT
    );
    """)

    # Create attendees table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS attendees (
        event_url TEXT,
        company_url TEXT,
        company_relation_to_event TEXT,
        PRIMARY KEY (event_url, company_url)
    );
    """)

    # Create companies table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS companies (
        company_url TEXT PRIMARY KEY,
        company_name TEXT,
        company_industry TEXT,
        company_revenue INT,
        company_country TEXT
    );
    """)

    # Create company_contacts table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS company_contacts (
        company_url TEXT,
        office_city TEXT,
        office_country TEXT,
        office_address TEXT,
        office_email TEXT,
        PRIMARY KEY (company_url)
    );
    """)

    # Create employees table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS employees (
        company_url TEXT,
        person_id TEXT,
        person_first_name TEXT,
        person_last_name TEXT,
        person_email TEXT,
        person_city TEXT,
        person_country TEXT,
        person_seniority TEXT,
        person_department TEXT,
        PRIMARY KEY (person_id)
    );
    """)

def insert_data(cursor):
    # Insert events data
    events_insert_query = sql.SQL("""
    INSERT INTO events (event_url, event_name, event_start_date, event_city, event_country, event_industry)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (event_url) DO NOTHING;
    """)
    cursor.executemany(events_insert_query, events_df.values.tolist())

    # Insert attendees data
    attendees_insert_query = sql.SQL("""
    INSERT INTO attendees (event_url, company_url, company_relation_to_event)
    VALUES (%s, %s, %s)
    ON CONFLICT (event_url, company_url) DO NOTHING;
    """)
    cursor.executemany(attendees_insert_query, attendees_df.values.tolist())

    # Insert companies data
    companies_insert_query = sql.SQL("""
    INSERT INTO companies (company_url, company_name, company_industry, company_revenue, company_country)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (company_url) DO NOTHING;
    """)
    cursor.executemany(companies_insert_query, companies_df.values.tolist())

    # Insert company contacts data
    company_contacts_insert_query = sql.SQL("""
    INSERT INTO company_contacts (company_url, office_city, office_country, office_address, office_email)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (company_url) DO NOTHING;
    """)
    cursor.executemany(company_contacts_insert_query, company_contacts_df.values.tolist())

    # Insert employees data
    employees_insert_query = sql.SQL("""
    INSERT INTO employees (company_url, person_id, person_first_name, person_last_name, person_email, person_city, person_country, person_seniority, person_department)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (person_id) DO NOTHING;
    """)
    cursor.executemany(employees_insert_query, employees_df.values.tolist())

def main():
    # Connect to PostgreSQL
    try:
        conn = psycopg2.connect(**connection_params)
        cursor = conn.cursor()

        # Create tables
        create_tables(cursor)

        # Insert sample data
        insert_data(cursor)

        # Commit changes
        conn.commit()
        print("Data inserted successfully!")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the cursor and connection
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()