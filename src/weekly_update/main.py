from weekly_update.crew import WeeklySlackUpdateCrew
import agentops
import sys

inputs = {
    "query": """Get the important metrics for the company Bonafide Health like conversion rate and hits 
                for the most recent week number (max) and highlight interesting facts about affiliates performance
                """
    }

def run():
    agentops.init()
    crew_output = WeeklySlackUpdateCrew().crew().kickoff(inputs=inputs)
    print(f"Raw Output: {crew_output.raw}")

def train():
    WeeklySlackUpdateCrew().crew().train(n_iterations=int(sys.argv[1]), filename=sys.argv[2], inputs=inputs)
