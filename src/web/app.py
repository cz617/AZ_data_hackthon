"""Main Streamlit application."""
import streamlit as st
import sys
from pathlib import Path

# Add src to path
src_path = Path(__file__).parent.parent
sys.path.insert(0, str(src_path))

from src.core.config import get_settings
from src.agent.agent import create_data_agent

# Page config
st.set_page_config(
    page_title="AZ Data Agent",
    page_icon="📊",
    layout="wide",
)

# Initialize session state
if "messages" not in st.session_state:
    st.session_state.messages = []

if "agent" not in st.session_state:
    try:
        settings = get_settings()
        st.session_state.agent = create_data_agent(settings, verbose=False)
    except Exception as e:
        st.error(f"Failed to initialize agent: {e}")
        st.session_state.agent = None


def main():
    """Main application."""
    st.title("📊 AZ Data Agent")
    st.markdown("AI-powered data analysis for AstraZeneca pharmaceutical data")

    # Sidebar
    with st.sidebar:
        st.header("About")
        st.markdown("""
        This tool helps you analyze:
        - **P&L Metrics**: Revenue, costs, budget variance
        - **Market Share**: AZ vs competitor performance
        - **Trends**: Year-over-year comparisons

        Just ask a question in natural language!
        """)

        st.divider()

        st.header("Example Questions")
        st.markdown("""
        - What is our current quarter revenue?
        - Show budget variance by product
        - What is AZ's market share in Oncology?
        - Compare revenue YoY by therapeutic area
        """)

        if st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()

    # Main chat area
    chat_container = st.container()

    with chat_container:
        # Display chat history
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("Ask about your data..."):
        # Add user message
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        # Generate response
        with st.chat_message("assistant"):
            with st.spinner("Analyzing..."):
                try:
                    if st.session_state.agent:
                        result = st.session_state.agent.invoke({"input": prompt})
                        response = result.get("output", "Unable to generate response")
                    else:
                        response = "Agent not initialized. Please check configuration."

                    st.markdown(response)
                    st.session_state.messages.append({"role": "assistant", "content": response})
                except Exception as e:
                    error_msg = f"Error: {str(e)}"
                    st.error(error_msg)
                    st.session_state.messages.append({"role": "assistant", "content": error_msg})


if __name__ == "__main__":
    main()