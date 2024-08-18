from weekly_update.crew import WeeklySlackUpdateCrew
import agentops


def run():
    agentops.init()
    inputs = {
    "query": "Get the important metrics for the company Bonafide Health like conversion rate and hits for the most recent week number (max) and highlight interesting facts about affiliates performance"
    }

    WeeklySlackUpdateCrew().crew().kickoff(inputs=inputs)