import sqlite3
import pandas as pd

from crewai.project import CrewBase, agent, crew, task
from crewai import Agent, Task, Crew, Process
from crewai_tools import tool

from langchain_community.tools.sql_database.tool import (
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
    QuerySQLCheckerTool,
    QuerySQLDataBaseTool,
)
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_groq import ChatGroq
from agentops import record_tool

df = pd.read_csv("marketing_data.csv")

connection = sqlite3.connect("marketing_data.db")
df.to_sql(name="marketing_data", con=connection, if_exists='replace')
db = SQLDatabase.from_uri("sqlite:///marketing_data.db")


@CrewBase
class WeeklySlackUpdateCrew:
    """WeeklySlackUpdateCrew Crew"""

    llm = ChatGroq(
    temperature=0,
    model_name="llama3-70b-8192"
    )

    @tool("list_tables") 
    @record_tool('list_tables')
    def list_tables() -> str:
        """List the available tables in the database"""
        return ListSQLDatabaseTool(db=db).invoke("")

    @tool("tables_schema")
    @record_tool('tables_schema')
    def tables_schema(tables: str) -> str:
        """
        Input is a comma-separated list of tables, output is the schema and sample rows
        for those tables. Be sure that the tables actually exist by calling `list_tables` first!
        Example Input: table1, table2, table3
        """
        tool = InfoSQLDatabaseTool(db=db)
        return tool.invoke(tables)

    @tool("check_sql")
    @record_tool('check_sql')
    def check_sql(sql_query: str) -> str:
        """
        Use this tool to double check if your query is correct before executing it. Always use this
        tool before executing a query with `execute_sql`.
        """
        return QuerySQLCheckerTool(db=db, llm=WeeklySlackUpdateCrew.llm).invoke({"query": sql_query})
    
    @tool("execute_sql")
    @record_tool('execute_sql')
    def execute_sql(sql_query: str) -> str:
        """Execute a SQL query against the database. Returns the result"""
        return QuerySQLDataBaseTool(db=db).invoke(sql_query)

    @agent
    def sql_dev(self) -> Agent:
        return Agent(
            config=self.agents_config["sql_dev"],
            tools=[self.list_tables, self.tables_schema, self.execute_sql, self.check_sql],
            verbose=True,
            allow_delegation=False,
            max_iter=5,
            llm=self.llm
        )

    @agent
    def data_analyst(self) -> Agent:
        return Agent(
            config=self.agents_config["data_analyst"],
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )

    @agent
    def report_writer(self) -> Agent:
        return Agent(
            config=self.agents_config["report_writer"],
            verbose=True,
            allow_delegation=False,
            llm=self.llm
        )

    @task
    def extract_data_task(self) -> Task:
        return Task(
            config=self.tasks_config["extract_data_task"],
            agent=self.sql_dev(),
            verbose=True
        )

    @task
    def analyze_data_task(self) -> Task:
        return Task(
            config=self.tasks_config["analyze_data_task"],
            agent=self.data_analyst(),
            context=[self.extract_data_task()],
            verbose=True
        )

    @task
    def write_report_task(self) -> Task:
        return Task(
            config=self.tasks_config["write_report_task"],
            agent=self.report_writer(),
            context=[self.analyze_data_task()],
            verbose=True
        )

    @crew
    def crew(self) -> Crew:
        """Creates the WeeklySlackUpdateCrew crew"""
        return Crew(
            agents=self.agents,
            tasks=self.tasks,
            process=Process.sequential,
            verbose=True,
            memory=False            
            )