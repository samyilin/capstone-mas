from slack_bot import event_handler

if __name__ == "__main__":
    # Your logic to start listening to Slack events, e.g., via a webhook or RTM API
    # Example event for demonstration
    slack_event = {
        'channel': '#social',
        'text': 'weekly update',
        'user': 'U12345678'
    }

    event_handler(slack_event)
