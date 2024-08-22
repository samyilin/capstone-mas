from weekly_update.crew import WeeklySlackUpdateCrew
import agentops


def run():
    agentops.init()
    inputs = {
    "query": """Get the important metrics for all channels for the most recent week number and draw insights about channels performance.
                The performance metrics should come the the available field names only.
             """
    }

    WeeklySlackUpdateCrew().crew().kickoff(inputs=inputs)