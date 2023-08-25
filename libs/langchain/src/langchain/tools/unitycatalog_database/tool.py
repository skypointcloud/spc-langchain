# flake8: noqa
"""Tools for interacting with a SQL database."""
from typing import Any, Optional


from pydantic import BaseModel, Extra, Field

from langchain.callbacks.manager import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain.sql_database import SQLDatabase
from langchain.tools.base import BaseTool

import requests
import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import json
from sqlalchemy.exc import ProgrammingError


class BaseSQLDatabaseTool(BaseModel):
    """Base tool for interacting with a SQL database."""

    db: SQLDatabase = Field(exclude=True)

    # Override BaseTool.Config to appease mypy
    # See https://github.com/pydantic/pydantic/issues/4173
    class Config(BaseTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.allow

class InfoUnityCatalogTool(BaseTool):

    class Config(BaseTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.allow
        
    """Tool for getting metadata about a SQL database."""
    db: SQLDatabase = Field(exclude=True)
    name = "sql_db_schema"
    description = """
    Input to this tool is a comma-separated list of tables, output is the schema , comments of columns and sample rows for those tables.    

    Example Input: "table1, table2, table3"
    """
    db_token : str
    db_host : str
    db_catalog : str
    db_schema : str
    db_warehouse_id : str

    def __init__(__pydantic_self__, **data: Any ) -> None:
        """Initialize the tool."""
        super().__init__(**data)
        
    def _run(
        self,
        table_names: str,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:   
        """Get the schema for tables in a comma-separated list."""
        return self.get_table_details_from_unity_catalog(table_names.split(", "))
    
    async def _arun(
        self,
        table_name: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("SchemaSqlDbTool does not support async")

    def get_table_details_from_unity_catalog(
        self,
        table_names: list,
    ):  
        final_string : str = ""
        for table_name in table_names:
            url = f"https://{self.db_host}/api/2.1/unity-catalog/tables/{self.db_catalog}.{self.db_schema}.{table_name}"
            headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.db_token}    

            session = requests.Session()
            retries = Retry(total=10,connect=10,read=10,backoff_factor=0.3)
            adapter = HTTPAdapter(max_retries=retries)
            response = session.get(url, headers=headers)
            json_data = json.loads(response.text)
            column_data = json_data['columns']
            if 'comment' in json_data.keys():
                table_comment = json_data['comment']
            else:
                table_comment = None
            string_data = self.generate_create_table_query(table_data = column_data,
                                                           table_name = table_name,
                                                           table_comment = table_comment)
            
            
        final_string += '\n' + string_data
        return final_string

    def generate_create_table_query(self,table_data,table_name,table_comment):
        sample_rows_in_table_info : int = 3
        if table_comment:
            query = f"CREATE TABLE {table_name} COMMENT '{table_comment}' (\n"
        else:
            query = f"CREATE TABLE {table_name} (\n"
        for column_info in table_data:
            column_name = column_info['name']
            column_type = column_info['type_text'].upper()
            column_comment = column_info.get('comment', None)

            # Add column name, type, and comment to the query
            if column_comment:
                query += f"\t{column_name} {column_type} COMMENT '{column_comment}' "
            else:
                query += f"\t{column_name} {column_type} "

            # Add a comma if it's not the last column
            if column_info != table_data[-1]:
                query += ","

            query += "\n"

        query += ") USING DELTA"

        column_names = [item['name'] for item in table_data]
        columns_str = '\t'.join(column_names)

        top_3_rows = self.get_sample_rows(
            table = table_name
            )
        
        return (
            
            f"{query}\n"
            f"\n/*\n"
            f"{sample_rows_in_table_info} rows from {table_name} table:\n"
            f"{columns_str}\n"
            f"{top_3_rows}"
            f"\n\*\n"
        )
    
    def get_sample_rows(self,table : str):
        sample_rows_in_table_info : int = 3
        command = "Select * from {table} limit {sample_rows_in_table_info}".format(table=table,sample_rows_in_table_info=sample_rows_in_table_info)

        try:
            with self.db._engine.connect() as connection:
                sample_rows_result = connection.execute(command)
                sample_rows = list(
                    map(lambda ls: [str(i)[:100] for i in ls], sample_rows_result)
                )
            sample_rows_str = "\n".join(["\t".join(row) for row in sample_rows])
            
        except ProgrammingError:
            sample_rows_str = ""

        return sample_rows_str


        
