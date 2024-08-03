import os
from langchain_openai import ChatOpenAI
from crewai import Agent, Task, Crew, Process
from dotenv import load_dotenv
from weekly_update.tools.google_sheet_tool import GoogleSheetTool
from crewai import Agent, Crew, Process, Task
from crewai.project import CrewBase, agent, crew, task

load_dotenv()

os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")
os.environ["OPENAI_MODEL_NAME"] = os.getenv("OPENAI_MODEL_NAME")

@CrewBase
class WeeklySlackUpdateCrew:
    """WeeklySlackUpdateCrew Crew"""

    agents_config = "config/agents.yaml"
    tasks_config = "config/tasks.yaml"

    cred_file = 'cred.json'
    spreadsheet_id = '1Plb8FFG_kfvxoQC4tA6f6Z61iny9WW3-BC1hQFdARvE'

    gsheet_tool = GoogleSheetTool(cred_file=cred_file, spreadsheet_id=spreadsheet_id)

    @agent
    def account_manager(self) -> Agent:
        return Agent(
            config=self.agents_config["account_manager"],
            tools=[self.gsheet_tool],
            verbose=True,
            allow_delegation=False,
        )

    @agent
    def qa_agent(self) -> Agent:
        return Agent(
            config=self.agents_config["qa_agent"],
            verbose=True,
            allow_delegation=False
        )

    @task
    def account_manager_task(self) -> Task:
        return Task(
            config=self.tasks_config["account_manager_task"],
            agent=self.account_manager()
        )

    @task
    def qa_agent_task(self) -> Task:
        return Task(
            config=self.tasks_config["qa_agent_task"],
            agent=self.qa_agent(),
            output_file="output/output.txt",
        )

    @crew
    def crew(self) -> Crew:
        """Creates the InstagramPlanner crew"""
        return Crew(
            agents=self.agents,  # Automatically created by the @agent decorator
            tasks=self.tasks,  # Automatically created by the @task decorator
            process=Process.sequential,
            verbose=True,
            manager_lm=ChatOpenAI(temperature=0, model="gpt-4")
        )