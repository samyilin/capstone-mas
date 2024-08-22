import streamlit as st
from weekly_update.crew import WeeklySlackUpdateCrew
import time

class WeeklyUpdateGenUI:

    def generate_weekly_update(self, query):
        inputs = {
            "query": query
        }
        return WeeklySlackUpdateCrew().crew().kickoff(inputs=inputs).raw.replace('$', r'\$')
    
    def stream_data(self, text):
        for word in text.split(" "):
            yield word + " "
            time.sleep(0.02)

    def weekly_update_generation(self):

        if st.session_state.generating:
            with st.spinner(text="In progress..."):
                 st.write("🚀 Hold on, the crew is working hard to get the job done! 🛠️")
                 st.session_state.weekly_update = self.generate_weekly_update(st.session_state.query)

        if st.session_state.weekly_update and st.session_state.weekly_update != "":
            with st.container():
                st.write_stream(self.stream_data(st.session_state.weekly_update))
                st.write("Summary generated successfully!")
            st.session_state.generating = False

    def sidebar(self):
        with st.sidebar:
            st.title("Weekly Update Generator")

            st.write(
                """
                To generate an executive summary, enter your query. \n
                Your team of AI agents will generate a summary for you!
                """
            )

            st.text_area("Query", key="query", height=300, value="Get the important metrics for the company Bonafide Health like conversion rate and hits for the most recent week number (max) and highlight interesting facts about affiliates performance")

            if st.button("Generate Update"):
                st.session_state.generating = True
 

    def render(self):
        st.set_page_config(page_title="Weekly Update Generation", page_icon="📧")
        st.header("Weekly Update Generation")


       # if "query" not in st.session_state:
       #     st.session_state.query = ""

        if "weekly_update" not in st.session_state:
            st.session_state.weekly_update = ""

        if "generating" not in st.session_state:
            st.session_state.generating = False

        self.sidebar()

        self.weekly_update_generation()


if __name__ == "__main__":
    WeeklyUpdateGenUI().render()