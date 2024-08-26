from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
# from crew import WeeklySlackUpdateCrew  # Import your Crew AI class

# Initialize the Slack client with your bot token
slack_client = WebClient(token='xoxb-')


def run_weekly_update_crew():
    # Create an instance of WeeklySlackUpdateCrew and run the crew
    # crew_instance = WeeklySlackUpdateCrew()
    # crew_instance.crew().kickoff()

    # Assume the crew generates an output file or a result you want to return
    with open("output/output.txt", "r") as file:
        result = file.read()

    return result  # Return the result to be sent back to Slack


def handle_message(event):
    user_message = event['text']

    # Check if the message is asking for the weekly report
    if "weekly report" in user_message.lower():
        # Run the WeeklySlackUpdateCrew and get the result
        report = run_weekly_update_crew()
        reply = f"Here is the weekly report:\n{report}"
    else:
        reply = "Sorry, I don't understand that command."

    # Send the reply back to Slack
    try:
        slack_client.chat_postMessage(
            channel=event['channel'],
            text=reply
        )
    except SlackApiError as e:
        print(f"Error sending message: {e.response['error']}")


def event_handler(event):
    if 'bot_id' not in event:  # Make sure the message is not from another bot
        handle_message(event)
