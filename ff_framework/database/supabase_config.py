from supabase import create_client, Client
import pandas as pd
import regex as re

class SupabaseConfig:
    def __init__(self, auth_dict: dict):
        self.url = auth_dict.get("url")
        self.key = auth_dict.get("key")
        self.service_role = auth_dict.get("service_role")
        self.client: Client = create_client(self.url, self.service_role)
        
    def _row_builder(self, columns, data):
        rows = []
        row = {}
        if isinstance(data, dict):
            data = list(data.values())
        while len(data) > 0:
            for i in range(len(columns)):
                if len(data) > 0:
                    row[columns[i].strip()] = data[0]
                    data = data[1:]
            rows.append(row)
        return rows
        
    def insert(self, table_name: str, columns: list, data: list | dict):
        insert_data = {}
        if len(columns) != len(data):
            insert_data = self._row_builder(columns, data)
        else:
            for i in range(len(columns)):
                insert_data[columns[i].strip()] = data[i]
        try:
            self.client.table(table_name).insert(insert_data).execute()
        except Exception as e:
            raise e
        
    def select(self, table_name: str, columns: str, condition: dict = None):
        query = self.client.table(table_name).select(columns)
        if condition is not None:
            query = self._condition_handler(query, condition)
        try:
            response = query.execute()
            if response.data is None:
                return pd.DataFrame()
            if isinstance(response.data, list):
                return pd.DataFrame(response.data)
            else:
                return pd.DataFrame([response.data])
        except Exception as e:
            raise e
        
    def update(self, table_name: str, columns: list, data: list, condition: dict = None):
        update_data = self._row_builder(columns, data)
        query = self.client.table(table_name).update(update_data)
        if condition is not None:
            query = self._condition_handler(query, condition)
        try:
            response = query.execute()
            if response.data is None:
                return pd.DataFrame()
            elif isinstance(response.data, list):
                return pd.DataFrame(response.data)
            else:
                return pd.DataFrame([response.data])
        except Exception as e:
            raise e

    def delete(self, table_name: str, condition: dict):
        query = self.client.table(table_name).delete()
        conditioned_query = self._condition_handler(query, condition)
        try:
            response = conditioned_query.execute()
            if response.data is None:
                return pd.DataFrame()
            elif isinstance(response.data, list):
                return pd.DataFrame(response.data)
            else:
                return pd.DataFrame([response.data])    
        except Exception as e:
            raise e
        
    def _condition_parser(self, condition: str):
        """
            This would be a normal where clause in sql. 
            Example: where column_name = value and column_name = value or column_name = value
        """

    def _condition_handler(self, supabase, condition: dict, query: str = None, is_or: bool = False):
        """
            basic format of condition is:
                {
                    "and": [
                        {
                            "=": ["column_name", "value"]
                        },
                        {
                            ">": ["column_name", "value"]
                        }
                    ]
                }
        """
        if condition is None and query is None:
            return supabase
        if condition is not None:
            for key, value in condition.items():
                if key == 'and': 
                    for sub_condition in value:
                        supabase = self._condition_handler(supabase, sub_condition)
                elif key == 'or': 
                    or_string = ""
                    for i, sub_condition in enumerate(value):
                        or_string += f"{self._condition_handler(supabase, sub_condition, is_or=True)}"
                        if i < len(value) - 1:
                            or_string += ","
                    supabase = supabase.or_(or_string)
                else:
                    column = value[0].strip()
                    if isinstance(value[1], str) and len(value[1].split(",")) > 1:
                        val = value[1].split(",")
                        val = [v.strip() for v in val]
                    else:
                        val = value[1]
                        if isinstance(val, str):
                            val = val.strip()
                    if key == '=':
                        if is_or:
                            return f"{column}.eq.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.eq(column, val)
                    elif key == '!=':
                        if is_or:
                            return f"{column}.neq.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.neq(column, val)
                    elif key == '>':
                        if is_or:
                            return f"{column}.gt.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.gt(column, val)
                    elif key == '<':
                        if is_or:
                            return f"{column}.lt.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.lt(column, val)
                    elif key == 'like':
                        if is_or:
                            return f"{column}.ilike.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.ilike(column, f"%{val}%")
                    elif key == 'in':
                        value = ','.join(val)
                        if is_or:
                            return f"{column}.in.({val})"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.in_(column, val)
                    elif key == 'not in':
                        value = ','.join(val)
                        if is_or:
                            return f"{column}.not.in.({val})"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.not_.in_(column, f"({val})")
                    elif key == 'is':
                        if is_or:
                            return f"{column}.is.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.is_(column, val)
                    elif key == 'is not':
                        if is_or:
                            return f"{column}.is.not.{val}"
                        else:
                            supabase.negate_next = False
                            supabase = supabase.is_not(column, val)
                    else:
                        raise ValueError(f"Unsupported operator: {key}")
        return supabase