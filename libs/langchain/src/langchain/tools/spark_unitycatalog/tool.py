# flake8: noqa
"""Tools for interacting with a SQL database."""
import json
import os
import re
from typing import Any, Dict, List, Optional

import openai
import requests
from langchain.callbacks.manager import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain.chains import LLMChain
from langchain.llms import AzureOpenAI
from langchain.prompts import PromptTemplate
from langchain.sql_database import SQLDatabase
from langchain.tools.base import StateTool
from langchain.tools.spark_unitycatalog.prompt import SQL_QUERY_VALIDATOR
from pydantic import BaseModel, Extra, Field
from requests.adapters import HTTPAdapter
from sqlalchemy.exc import ProgrammingError
from urllib3.util.retry import Retry


class BaseSQLDatabaseTool(BaseModel):
    """Base tool for interacting with a SQL database."""

    db: SQLDatabase = Field(exclude=True)

    # Override BaseTool.Config to appease mypy
    # See https://github.com/pydantic/pydantic/issues/4173
    class Config(StateTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.allow


class InfoUnityCatalogTool(StateTool):
    class Config(StateTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.allow

    """Tool for getting metadata about a SQL database."""
    db: SQLDatabase = Field(exclude=True)
    name = "sql_db_schema"
    description = """
    Input to this tool is a comma-separated list of tables, output is the schema , comments of columns, and sample rows for those tables.    

    Example Input: "table1, table2, table3"
    """
    db_token: str
    db_host: str
    db_catalog: str
    db_schema: str
    db_warehouse_id: str

    def __init__(__pydantic_self__, **data: Any) -> None:
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
        final_string: str = ""
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.db_token}",
        }
        retries = Retry(total=5, backoff_factor=0.3)
        adapter = HTTPAdapter(max_retries=retries)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        # TODO: Improve performance by using asyncio or threading to make concurrent requests
        for table_name in table_names:
            url = f"https://{self.db_host}/api/2.1/unity-catalog/tables/{self.db_catalog}.{self.db_schema}.{table_name}"
            response = session.get(url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error fetching table {table_name}: {response.text}")
            json_data = json.loads(response.text)
            column_data = json_data["columns"]
            table_comment = (
                json_data["comment"] if "comment" in json_data.keys() else None
            )
            string_data = self._generate_create_table_query(
                table_data=column_data,
                table_name=table_name,
                table_comment=table_comment,
            )
        return f"{final_string}\n{string_data}"

    def _generate_create_table_query(
        self, table_data: List[Dict], table_name: str, table_comment: str
    ):
        sample_rows_in_table_info: int = 3
        if table_comment:
            query = f"CREATE TABLE {table_name} COMMENT '{table_comment}' (\n"
        else:
            query = f"CREATE TABLE {table_name} (\n"
        for column_info in table_data:
            column_name = column_info["name"]
            column_type = column_info["type_text"].upper()
            if column_comment := column_info.get("comment", None):
                query += f"\t{column_name} {column_type} COMMENT '{column_comment}' "
            else:
                query += f"\t{column_name} {column_type} "

            # Add a comma if it's not the last column
            if column_info != table_data[-1]:
                query += ","

            query += "\n"

        query += ") USING DELTA"

        column_names = [item["name"] for item in table_data]
        columns_str = "\t".join(column_names)

        top_3_rows = self._get_sample_rows(table=table_name)

        return (
            f"{query}\n"
            f"\n/*\n"
            f"{sample_rows_in_table_info} rows from {table_name} table:\n"
            f"{columns_str}\n"
            f"{top_3_rows}"
            f"\n\*\n"
        )

    def _get_sample_rows(self, table: str):
        sample_rows_in_table_info: int = 3
        command = "Select * from {table} limit {sample_rows_in_table_info}".format(
            table=table, sample_rows_in_table_info=sample_rows_in_table_info
        )

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


class ListUnityCatalogTablesTool(StateTool):
    class Config(StateTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.allow

    """Tool for getting tables names."""
    db: SQLDatabase = Field(exclude=True)
    name = "sql_db_list_tables"
    description = """Input is an empty string, output is a comma separated list tables in the database and their description in brackets."""
    db_token: str
    db_host: str
    db_catalog: str
    db_schema: str
    db_warehouse_id: str

    def __init__(__pydantic_self__, **data: Any) -> None:
        """Initialize the tool."""
        super().__init__(**data)

    def _run(
        self,
        input: str = "",
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Get the schema for tables in a comma-separated list."""
        return self.get_table_list_from_unitycatalog()

    async def _arun(
        self,
        table_name: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("ListSqlTablesTool does not support async")

    def get_table_list_from_unitycatalog(
        self,
    ) -> str:
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.db_token}",
        }
        retries = Retry(total=5, backoff_factor=0.3)
        adapter = HTTPAdapter(max_retries=retries)
        session = requests.Session()
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        url = f"https://{self.db_host}/api/2.1/unity-catalog/tables"
        params = {"catalog_name": self.db_catalog, "schema_name": self.db_schema}
        response = session.get(url, headers=headers, params=params)
        if response.status_code != 200:
            raise Exception(f"Error fetching list of tables : {response.text}")
        json_data = json.loads(response.text)
        tables = json_data["tables"]
        table_info: str = ""

        for table in tables:
            table_name = table["name"]
            comment = table["comment"] if "comment" in table.keys() else None
            table_info += f"{table_name}({comment})\n"
        return table_info


class SqlQueryValidatorTool(StateTool):
    """Tool for validating a SQL query.Use this before running the query."""

    name = "sql_db_query_validator"
    description = """
    Input to this tool is sql_query   

    Example Input: "Select * from table1"
    """

    class Config(StateTool.Config):
        """Configuration for this pydantic object."""

        arbitrary_types_allowed = True
        extra = Extra.allow

    def __init__(__pydantic_self__, **data: Any) -> None:
        """Initialize the tool."""
        super().__init__(**data)

    def _run(
        self,
        query: str,
        run_manager: Optional[CallbackManagerForToolRun] = None,
    ) -> str:
        """Get the schema for tables in a comma-separated list."""
        return self._validate_sql_query(query)

    async def _arun(
        self,
        table_name: str,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
    ) -> str:
        raise NotImplementedError("ListSqlTablesTool does not support async")

    def _setup_llm(self):
        openai.api_type = "azure"
        openai.api_version = "2022-12-01"
        openai.api_key = os.getenv("OPENAI_API_KEY")
        openai.api_base = os.getenv("OPENAI_API_BASE")
        deployment_name = os.getenv("AZURE_OPENAI_INSTRUCT_LLM_DEPLOYMENT_NAME")
        llm = AzureOpenAI(
            deployment_name=deployment_name,
            model_name=os.getenv("OPENAI_INSTRUCT_LLM_MODEL"),
            temperature=0,
        )
        return llm

    def _parse_db_schema(self):
        sql_db_schema_value = {}

        for value in self.state:
            for key, input_string in value.items():
                if "sql_db_schema" in key:
                    tool_input_match = re.search(
                        r"CREATE TABLE (.*?) COMMENT", input_string
                    )
                    if tool_input_match:
                        table_name = tool_input_match.group(1)
                        create_table_match = re.search(
                            rf"CREATE TABLE {table_name}.*?USING DELTA",
                            input_string,
                            re.DOTALL,
                        )
                        if create_table_match:
                            table_schema = create_table_match.group()
                            sql_db_schema_value[table_name] = table_schema
                else:
                    return None

        return sql_db_schema_value

    def _validate_sql_query(self, query):

        db_schema = self._parse_db_schema()

        prompt_input = PromptTemplate(
            input_variables=["db_schema", "query"],
            template=SQL_QUERY_VALIDATOR,
        )
        chain = LLMChain(llm=self._setup_llm(), prompt=prompt_input)

        query_validation = chain.run(({"db_schema": db_schema, "query": query}))

        return query_validation
