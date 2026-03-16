"""Chat component for Streamlit."""
import streamlit as st
from typing import List, Dict


def display_chat_messages(messages: List[Dict[str, str]]) -> None:
    """Display chat messages in Streamlit."""
    for message in messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])


def chat_input_component(placeholder: str = "Ask about your data...") -> str:
    """Create a chat input component."""
    return st.chat_input(placeholder)