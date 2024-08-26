import json
import os
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Tuple, Union
import pandas as pd
from crewai import Agent, Crew, Process, Task
from crewai_tools import tool
from langchain.schema import AgentFinish
from langchain.schema.output import LLMResult
from langchain_community.tools.sql_database.tool import (
    InfoSQLDatabaseTool,
    ListSQLDatabaseTool,
    QuerySQLCheckerTool,
    QuerySQLDataBaseTool,
)
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.callbacks.base import BaseCallbackHandler
from langchain_core.prompts import ChatPromptTemplate
from langchain_groq import ChatGroq

os.environ["GROQ_API_KEY"] = "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"

df = pd.read_csv("/Users/chandnimelwani/Documents/MMAI/Capstone/code_repo/capstone-mas/data/Bonafide_data_compiled.csv")
df.head()

connection = sqlite3.connect("bonafide_master.db")
df.to_sql(name="bonafide_master", con=connection, if_exists='replace')

@dataclass
class Event:
    event: str
    timestamp: str
    text: str


def _current_time() -> str:
    return datetime.now(timezone.utc).isoformat()


class LLMCallbackHandler(BaseCallbackHandler):
    def __init__(self, log_path: Path):
        self.log_path = log_path

    def on_llm_start(
        self, serialized: Dict[str, Any], prompts: List[str], **kwargs: Any
    ) -> Any:
        """Run when LLM starts running."""
        assert len(prompts) == 1
        event = Event(event="llm_start", timestamp=_current_time(), text=prompts[0])
        with this.log_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(asdict(event)) + "\n")

    def on_llm_end(self, response: LLMResult, **kwargs: Any) -> Any:
        """Run when LLM ends running."""
        generation = response.generations[-1][-1].message.content
        event = Event(event="llm_end", timestamp=_current_time(), text=generation)
        with this.log_path.open("a", encoding="utf-8") as file:
            file.write(json.dumps(asdict(event)) + "\n")

llm = ChatGroq(
    temperature=0,
    model_name="llama3-70b-8192",
    callbacks=[LLMCallbackHandler(Path("prompts.jsonl"))],
)

db = SQLDatabase.from_uri("sqlite:///bonafide_master.db")

@tool("list_tables")
def list_tables() -> str:
    """List the available tables in the database"""
    return ListSQLDatabaseTool(db=db).invoke("")

@tool("tables_schema")
def tables_schema(tables: str) -> str:
    """
    Input is a comma-separated list of tables, output is the schema and sample rows
    for those tables. Be sure that the tables actually exist by calling `list_tables` first!
    Example Input: table1, table2, table3
    """
    tool = InfoSQLDatabaseTool(db=db)
    return tool.invoke(tables)

@tool("execute_sql")
def execute_sql(sql_query: str) -> str:
    """Execute a SQL query against the database. Returns the result"""
    return QuerySQLDataBaseTool(db=db).invoke(sql_query)

@tool("check_sql")
def check_sql(sql_query: str) -> str:
    """
    Use this tool to double-check if your query is correct before executing it.
    Always use this tool before executing a query with `execute_sql`.
    """
    return QuerySQLCheckerTool(db=db, llm=llm).invoke({"query": sql_query})

sql_dev = Agent(
    role="Senior Database Developer",
    goal="Construct and execute SQL queries based on a request",
    backstory=dedent(
        """
        You are an experienced database engineer who is a master at creating efficient and complex SQL queries.
        You have a deep understanding of how different databases work and how to optimize queries.
        Use the `list_tables` to find available tables.
        Use the `tables_schema` to understand the metadata for the tables.
        Use the `check_sql` to check your queries for correctness.
        Use the `execute_sql` to execute queries against the database.
    """
    ),
    llm=llm,
    tools=[list_tables, tables_schema, execute_sql, check_sql],
    allow_delegation=False,
    max_iter=5
)

data_analyst = Agent(
    role="Senior Affiliate Marketing Data Analyst",
    goal="Analyze the extracted data to identify key week-over-week changes in performance metrics such as NCAC, total spend, and customer acquisition metrics.",
    backstory=dedent(
        """
       You have deep experience with affiliate marketing datasets using Python. 
       You are proficient in identifying trends, performing comparative analysis, 
       and detecting anomalies in the data.
       You identify affiliate-related performance that is happening week over week,
       and show trends in improved performance.
       You aggregate all metrics for every week and show the performance of all the affiliates on brand week over week starting from week 1
       and showing how NCAC, spend, and revenue have changed with time.
       After going through aggregate week data, you then dive into affiliate-specific data highlighting the top-performing affiliates 
       and affiliates that are in the red zone. Top-performing affiliates are affiliates with NCAC lower than $310 
       and whose NCAC reduces week over week. The affiliates in the red zone are affiliates with NCAC greater than $310 and 
       whose NCAC increases week over week.
       You support the NCAC affiliate-level analysis with supporting metrics such as highlighting the trends in revenue and conversions.
       If sales and revenue are increasing, you make it a positive point to note that even though NCAC might be high and increasing with time, 
       revenue and customers acquired are also increasing.
       Your work is always clear, detailed, and actionable.
    """
    ),
    llm=llm,
    allow_delegation=False,
    verbose=True
)



report_writer = Agent(
    role="Senior Affiliate Marketing Report Editor",
    goal="Summarize the analysis in an executive report, focusing on key changes in metrics for each affiliate week-over-week.",
    backstory=dedent(
        """
        Your reports are concise, clear, and geared towards executives who need quick insights. 
        You emphasize the most critical changes and suggest potential action items based on the analysis.
        """
    ),
    llm=llm,
    allow_delegation=False,
    verbose=True
)

extract_data = Task(
    description="Extract data for week-over-week performance comparison. The data should include all relevant metrics such as total spend, NCAC, customer acquisition, and revenue for each affiliate.",
    expected_output="A dataset containing all necessary fields for the weeks under review, structured for comparison.",
    agent=sql_dev,
    verbose=True
)

analyze_data = Task(
    description="Analyze the week-over-week performance data, highlighting key changes in metrics like NCAC, total spend, and customer acquisition. Focus on identifying trends, outliers, and significant variations and write an analysis for {query}.",
    expected_output="A detailed analysis report that compares performance across weeks, noting any significant changes and providing potential reasons or insights.",
    agent=data_analyst,
    context=[extract_data],
    verbose=True
)

write_report = Task(
    description=dedent(
        """
        Create an executive summary that captures the most critical changes in performance metrics. Highlight affiliates with the most significant improvements or declines in key metrics. 
        The summary should be actionable and provide clear insights for decision-making.
    """
    ),
    expected_output="Markdown report of a brief, clear report that can be shared with stakeholders, summarizing the key findings from the analysis.",
    agent=report_writer,
    context=[analyze_data],
    verbose=True
)

crew = Crew(
    agents=[sql_dev, data_analyst, report_writer],
    tasks=[extract_data, analyze_data, write_report],
    process=Process.sequential,
    verbose=True,
    memory=False,
    output_log_file="crew.log"
)

     
inputs = {
    "query": "Provide a summary of the week-over-week performance for Bonafide Health"
}

result = crew.kickoff(inputs=inputs)
print(result)
