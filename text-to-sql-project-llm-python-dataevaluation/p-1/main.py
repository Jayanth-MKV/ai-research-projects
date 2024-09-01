import pandas as pd
from typing import Dict, List, Any
from abc import ABC, abstractmethod

class FilterStrategy(ABC):
    @abstractmethod
    def apply(self, df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        pass

class EqualityFilter(FilterStrategy):
    def apply(self, df: pd.DataFrame, column: str, value: Any) -> pd.DataFrame:
        return df[df[column].str.lower() == str(value).lower()]

class InFilter(FilterStrategy):
    def apply(self, df: pd.DataFrame, column: str, value: List[Any]) -> pd.DataFrame:
        return df[df[column].str.lower().isin([str(v).lower() for v in value])]

class FilterFactory:
    @staticmethod
    def get_filter(value: Any) -> FilterStrategy:
        if isinstance(value, list):
            return InFilter()
        return EqualityFilter()

class DataFrameFilter:
    def __init__(self, dataframes: Dict[str, pd.DataFrame], relationships: Dict[str, Dict[str, str]]):
        self.dataframes = dataframes
        self.relationships = relationships

    def filter(self, conditions: Dict[str, Dict[str, Any]]) -> Dict[str, pd.DataFrame]:
        filtered_dfs = {}

        # Apply initial filters
        for df_name, df_conditions in conditions.items():
            if df_name in self.dataframes:
                filtered_dfs[df_name] = self._apply_conditions(self.dataframes[df_name], df_conditions)

        # Propagate filters
        self._propagate_filters(filtered_dfs)

        return filtered_dfs

    def _apply_conditions(self, df: pd.DataFrame, conditions: Dict[str, Any]) -> pd.DataFrame:
        for column, value in conditions.items():
            if column in df.columns:
                filter_strategy = FilterFactory.get_filter(value)
                df = filter_strategy.apply(df, column, value)
            else:
                print(f"Warning: Column '{column}' not found in DataFrame. Skipping this condition.")
        return df

    def _propagate_filters(self, filtered_dfs: Dict[str, pd.DataFrame]) -> None:
        changed = True
        while changed:
            changed = False
            for df_name in self.dataframes:
                if df_name not in filtered_dfs:
                    filtered_dfs[df_name] = self.dataframes[df_name]

                for related_df, key in self.relationships.get(df_name, {}).items():
                    if related_df in filtered_dfs:
                        if key in filtered_dfs[df_name].columns and key in filtered_dfs[related_df].columns:
                            new_df = filtered_dfs[df_name][filtered_dfs[df_name][key].isin(filtered_dfs[related_df][key])]
                            if len(new_df) != len(filtered_dfs[df_name]):
                                filtered_dfs[df_name] = new_df
                                changed = True

class DataFrameManager:
    @staticmethod
    def create_sample_dataframes() -> Dict[str, pd.DataFrame]:
        events_df = pd.DataFrame({
            'event_url': ['e1', 'e2', 'e3', 'e4','e5'],
            'event_name': ['Tech Conf', 'Oil Expo', 'Green Energy','TEch' ,'Data Summit'],
            'event_start_date': ['2023-09-01', '2023-10-15', '2023-11-20','2023-11-20', '2023-12-05'],
            'event_city': ['San Francisco', 'Houston', 'Berlin','Berlin', 'New York'],
            'event_country': ['USA', 'USA', 'Germany', 'Germany', 'USA'],
            'event_industry': ['Technology', 'Oil & Gas', 'Renewable Energy','Technology', 'Technology']
        })

        attendees_df = pd.DataFrame({
            'event_url': ['e1', 'e1', 'e2', 'e2', 'e3', 'e4', 'e4','e5'],
            'company_url': ['c1', 'c2', 'c3', 'c4', 'c2', 'c1', 'c3','c4'],
            'company_relation_to_event': ['Sponsor', 'Attendee', 'Sponsor', 'Attendee', 'Sponsor', 'Attendee', 'Sponsor','Sponsor']
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
            'events': events_df,
            'attendees': attendees_df,
            'companies': companies_df,
            'company_contacts': company_contacts_df,
            'employees': employees_df
        }

    @staticmethod
    def get_relationships() -> Dict[str, Dict[str, str]]:
        return {
            'events': {'attendees': 'event_url'},
            'attendees': {'events': 'event_url', 'companies': 'company_url'},
            'companies': {'attendees': 'company_url', 'company_contacts': 'company_url', 'employees': 'company_url'},
            'company_contacts': {'companies': 'company_url'},
            'employees': {'companies': 'company_url'}
        }

# Example usage
if __name__ == "__main__":
    dataframes = DataFrameManager.create_sample_dataframes()
    relationships = DataFrameManager.get_relationships()
    df_filter = DataFrameFilter(dataframes, relationships)

    # Example 1: Case-insensitive filtering for events in the USA attended by companies in the Technology industry
    # print("Example 1: Events in the USA attended by Technology companies (case-insensitive)")
    # conditions1 = {
    #     'events': {'event_country': 'usa'},
    #     'companies': {'company_industry': 'TECHNOLOGY'}
    # }
    # filtered_data1 = df_filter.filter(conditions1)
    # for df_name, df in filtered_data1.items():
    #     print(f"\nFiltered {df_name}:")
    #     print(df)

    # Example 2: Case-insensitive filtering for employees who are Directors or Managers and work for companies attending the Tech Conf
    print("\nExample 2: Directors/Managers working for companies attending the Tech Conf (case-insensitive)")
    conditions2 = {
        'events': {'event_name': 'TECH CONF'},
        'employees': {'person_seniority': ['director','manager']}
    }
    filtered_data2 = df_filter.filter(conditions2)
    for df_name, df in filtered_data2.items():
        print(f"\nFiltered {df_name}:")
        print(df)

    # # Example 3: Case-insensitive filtering for companies sponsoring events in the Technology industry, along with their office details
    # print("\nExample 3: Companies sponsoring Technology events and their office details (case-insensitive)")
    # conditions3 = {
    #     'events': {'event_industry': 'technology'},
    #     'attendees': {'company_relation_to_event': 'SPONSOR'}
    # }
    # filtered_data3 = df_filter.filter(conditions3)
    # for df_name, df in filtered_data3.items():
    #     print(f"\nFiltered {df_name}:")
    #     print(df)