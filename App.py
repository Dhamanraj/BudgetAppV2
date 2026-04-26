import time
import streamlit as st
from Orchestrator import FinanceOrchestrator

# 1. Page Configuration
st.set_page_config(
    page_title="Buddy",
    page_icon="🏼‍♂️",
    layout="centered"
)

st.title("🕵🏼‍♂️ Buddy")
st.markdown("Your local intelligent assistant for financial data analysis.")

# 2. Initialize Chat History
if "messages" not in st.session_state:
    st.session_state.messages = []

# 3. Display Chat History
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "response_time" in message:
            st.caption(f"⏱️ Response time: {message['response_time']:.2f}s")
        if "sql" in message:
            with st.expander("View SQL Query"):
                st.code(message["sql"], language="sql")

# 4. Chat Input
if prompt := st.chat_input("Ask me about your transactions (e.g., 'Total spend in Dec 2025')"):
    # Display user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # 5. Generate Response
    with st.chat_message("assistant"):
        with st.spinner("Analyzing database and generating insights..."):
            start_time = time.time()
            response = FinanceOrchestrator.get_answer(prompt)
            end_time = time.time()
            response_time = end_time - start_time
            
            if response.get("error"):
                st.error(f"Error: {response['error']}")
            else:
                answer = response["answer"]
                sql = response["sql"]
                
                st.markdown(answer)
                st.caption(f"⏱️ Response time: {response_time:.2f}s")
                with st.expander("Technical Details (SQL)"):
                    st.code(sql, language="sql")
                
                # Add to history
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": answer,
                    "sql": sql,
                    "response_time": response_time
                })