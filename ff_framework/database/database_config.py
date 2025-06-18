from ff_framework.context.context_logging import ContextLogger
from ff_framework.database.supabase_config import SupabaseConfig
from typing import Literal, Optional
import pandas as pd

class DatabaseConfig:
    def __init__(self, logger: ContextLogger, db_url: str, db_service_role: str, db_type: str = Optional[Literal['SUPABASE']], db_api_key: str = None):
        self.logger = logger
        self.db_type = db_type
        self.db_url = db_url
        self.db_service_role = db_service_role
        self.db_api_key = db_api_key
        self.connection = self._get_connection()
        self.logger.log("Database connection established successfully.", level='info')
        
    def _get_connection(self):
        auth_dict = {
            "url": self.db_url,
            "key": self.db_api_key,
            "service_role": self.db_service_role
        }
        
        if self.db_type == "SUPABASE": 
            self.logger.log("Connecting to Supabase...", level='info')
            return SupabaseConfig(auth_dict)
        else:
            self.logger.log(f"Unsupported database type: {self.db_type}", level='error')
            raise ValueError(f"Unsupported database type: {self.db_type}")
        
    def select(self, table_name: str, columns: list = ['*'], condition: str = None):
        self.logger.log(f"Selecting data from: {table_name}", level='info')
        str_columns = ', '.join(columns) if len(columns) > 1 else columns[0]
        data = self.connection.select(table_name, str_columns, condition)
        if data.empty:
            self.logger.log("No data found", level='warning')
            return pd.DataFrame()
        self.logger.log("Data selected successfully", level='info')
        return data

    def insert(self, table_name: str, columns: list, data: list | dict | tuple):
        self.logger.log(f"Inserting data into table: {table_name}", level='info')
        if data is None:
            raise ValueError("Data cannot be None")
        try:
            if isinstance(data, tuple):
                data = [value for value in data]
            self.connection.insert(table_name, columns, data)
            self.logger.log("Data inserted successfully", level='info')
        except Exception as e:
            self.logger.log(f"Error inserting data: {e}", level='error')
            raise e

    def update(self, table_name: str, columns: list, data: list, condition: str = None):
        self.logger.log(f"Updating data in table: {table_name}", level='info')
        if not data:
            raise ValueError("Data cannot be empty")
        try:
            if not isinstance(columns, list):
                columns = [columns]
            if not isinstance(data, list):
                data = [data]
            self.connection.update(table_name, columns, data, condition)
            self.logger.log("Data updated successfully", level='info')
        except Exception as e:
            self.logger.log(f"Error updating data: {e}", level='error')
            raise e
        