import psycopg2
import pandas as pd
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Tuple

class QueryBuilder(ABC):
    @abstractmethod
    def build_base_query(self) -> str:
        pass

    @abstractmethod
    def build_main_query(self, output_columns: List[str]) -> str:
        pass

    @abstractmethod
    def build_from_clause(self, required_tables: set) -> str:
        pass

    @abstractmethod
    def build_where_clause(self, filter_arguments: List[Tuple[str, str, Any]]) -> Tuple[str, List[Any]]:
        pass

class PostgreSQLQueryBuilder(QueryBuilder):
    def build_base_query(self) -> str:
        return """
        WITH event_data AS (
        SELECT event_url,
               MAX(CASE WHEN attribute = 'event_name' THEN value END) AS event_name,
               MAX(CASE WHEN attribute = 'event_city' THEN value END) AS event_city,
               MAX(CASE WHEN attribute = 'event_country' THEN value END) AS event_country,
               MAX(CASE WHEN attribute = 'event_start_date' THEN value END) AS event_start_date,
               MAX(CASE WHEN attribute = 'event_industry' THEN value END) AS event_industry
        FROM event_attributes
        GROUP BY event_url
    ),
    company_data AS (
        SELECT company_url,
               MAX(CASE WHEN attribute = 'company_name' THEN value END) AS company_name,
               MAX(CASE WHEN attribute = 'company_country' THEN value END) AS company_country,
               MAX(CASE WHEN attribute = 'company_industry' THEN value END) AS company_industry,
               MAX(CASE WHEN attribute = 'company_revenue' THEN value END) AS company_revenue
        FROM company_attributes
        GROUP BY company_url
    ),
    people_data AS (
        SELECT person_id,
               MAX(CASE WHEN attribute = 'company_url' THEN value END) AS company_url,
               MAX(CASE WHEN attribute = 'person_first_name' THEN value END) AS person_first_name,
               MAX(CASE WHEN attribute = 'person_last_name' THEN value END) AS person_last_name,
               MAX(CASE WHEN attribute = 'person_email' THEN value END) AS person_email,
               MAX(CASE WHEN attribute = 'person_seniority' THEN value END) AS person_seniority,
               MAX(CASE WHEN attribute = 'person_department' THEN value END) AS person_department
        FROM people_attributes
        GROUP BY person_id
    )
        """

    def build_main_query(self, output_columns: List[str]) -> str:
        return "SELECT DISTINCT " + ", ".join(output_columns)

    def build_from_clause(self, required_tables: set) -> str:
        from_clause = "FROM attendees "
        for table in required_tables:
            if table == 'event_data':
                from_clause += f"JOIN {table} USING (event_url) "
            elif table == 'company_data':
                from_clause += f"JOIN {table} USING (company_url) "
            elif table == 'people_data':
                from_clause += f"LEFT JOIN company_contacts USING (company_url) "
                from_clause += f"LEFT JOIN {table} USING (company_url) "
        return from_clause

    def build_where_clause(self, filter_arguments: List[Tuple[str, str, Any]]) -> Tuple[str, List[Any]]:
        where_conditions = []
        params = []
        for col, condition, value in filter_arguments:
            if condition == 'includes':
                where_conditions.append(f"{col} IN ({', '.join(['%s'] * len(value))})")
                params.extend(value)
            elif condition in ['greater-than-equal-to', 'less-than-equal-to']:
                op = '>=' if condition == 'greater-than-equal-to' else '<='
                where_conditions.append(f"{col} {op} %s")
                params.append(value)
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        return where_clause, params

class QueryExecutor:
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config

    def execute(self, query: str, params: List[Any]) -> pd.DataFrame:
        with psycopg2.connect(**self.db_config) as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                columns = [desc[0] for desc in cur.description]
                results = cur.fetchall()
        return pd.DataFrame(results, columns=columns)

class QueryGenerator:
    def __init__(self, query_builder: QueryBuilder):
        self.query_builder = query_builder

    def generate_query(self, filter_arguments: List[Tuple[str, str, Any]], output_columns: List[str]) -> Tuple[str, List[Any]]:
        required_tables = self._get_required_tables(filter_arguments, output_columns)
        base_query = self.query_builder.build_base_query()
        main_query = self.query_builder.build_main_query(output_columns)
        from_clause = self.query_builder.build_from_clause(required_tables)
        where_clause, params = self.query_builder.build_where_clause(filter_arguments)
        
        full_query = f"{base_query}\n{main_query}\n{from_clause}\n{where_clause}"
        # print(full_query)
        return full_query, params

    def _get_required_tables(self, filter_arguments: List[Tuple[str, str, Any]], output_columns: List[str]) -> set:
        required_tables = set()
        for col in output_columns + [arg[0] for arg in filter_arguments]:
            if col.startswith('event_'):
                required_tables.add('event_data')
            elif col.startswith('company_'):
                required_tables.add('company_data')
            elif col.startswith('person_'):
                required_tables.add('people_data')
        return required_tables

class DataQueryService:
    def __init__(self, query_generator: QueryGenerator, query_executor: QueryExecutor):
        self.query_generator = query_generator
        self.query_executor = query_executor

    def query_data(self, filter_arguments: List[Tuple[str, str, Any]], output_columns: List[str]) -> pd.DataFrame:
        query, params = self.query_generator.generate_query(filter_arguments, output_columns)
        return self.query_executor.execute(query, params)

# Usage
if __name__ == "__main__":
    db_config = {
        "host": "127.0.0.1",
        "database": "db",
        "user": "root",
        "password": "root"
    }

    query_builder = PostgreSQLQueryBuilder()
    query_generator = QueryGenerator(query_builder)
    query_executor = QueryExecutor(db_config)
    data_query_service = DataQueryService(query_generator, query_executor)
    
    
    # Example 1: Tech & (Oil & Gas) companies attending events in San Francisco
    print("""Tech & (Oil & Gas) companies attending events in San Francisco""")
    filter_arguments = [
        ['event_city', 'includes', ['San Francisco']],
        ['company_industry', 'includes', ['Technology', 'Oil & Gas']],
    ]
    output_columns = ['event_city', 'event_name', 'event_country', 'company_industry', 'company_name', 'company_url']

    # Example 2: Directors of tech & Oil & Gas companies attending events in San Francisco in Sept 2023
    print("""Directors of tech & Oil & Gas companies attending events in San Francisco in Sept 2023""")
    filter_arguments = [
        ['event_city', 'includes', ['San Francisco']],
        ['event_start_date', 'less-than-equal-to', '2023-09-30'],
        ['event_start_date', 'greater-than-equal-to', '2023-09-01'],
        ['company_industry', 'includes', ['Technology', 'Oil & Gas']],
        ['person_seniority', 'includes', ['Director']],
    ]
    output_columns = ['event_city', 'event_name', 'event_country', 'company_industry', 'company_name', 'company_url', 'person_first_name', 'person_last_name', 'person_seniority']

    # Example 3: Directors/Managers of tech & Oil & Gas companies attending events in San Francisco/New York from Sept 2023 - Sept 2024
    print("""Directors/Managers of tech & Oil & Gas companies attending events in San Francisco/New York from Sept 2023 - Sept 2024""")
    filter_arguments = [
        ['event_city', 'includes', ['San Francisco','New York']],
        ['event_start_date', 'less-than-equal-to', '2024-09-30'],
        ['event_start_date', 'greater-than-equal-to', '2023-09-01'],
        ['company_industry', 'includes', ['Technology', 'Oil & Gas']],
        ['person_seniority', 'includes', ['Director','Manager']],
    ]
    output_columns = ['event_city', 'event_name', 'event_country', 'company_industry', 'company_name', 'company_url', 'person_first_name', 'person_last_name', 'person_seniority']

    result_df = data_query_service.query_data(filter_arguments, output_columns)
    print(result_df)