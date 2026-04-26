import os
from SchemaService import SchemaService
from OllamaService import OllamaService 
from DbManager import DatabaseManager

class FinanceOrchestrator:
    """
    The central 'Project Manager' of Buddy.
    Orchestrates the flow between Schema Discovery, LLM Inference, and Data Retrieval.
    """
    _cached_instructions = None
    _cached_schema = None

    @classmethod
    def get_answer(cls, user_question: str):
        if cls._cached_schema is None:
            print(f"\n[1/4] Gathering Database Context...")
            cls._cached_schema = SchemaService.get_formatted_schema()
        
        schema_context = cls._cached_schema

        # Reload instructions on every request during development to ensure logic changes are applied immediately
        instructions_path = os.path.join(os.path.dirname(__file__), "Instructions.md")
        try:
            with open(instructions_path, "r") as f:
                instructions_text = f.read()
        except FileNotFoundError:
            return {"question": user_question, "error": "Instructions.md not found.", "sql": None, "results": None, "answer": None}

        system_instruction = instructions_text.format(schema_context=schema_context)

        prompt = f"USER QUESTION: {user_question}\n\nSQL QUERY:"

        print(f"[2/4] Generating SQL Query...")
        generated_sql = OllamaService.ask_model(prompt, system_instruction=system_instruction)

        if not generated_sql:
            return {
                "question": user_question,
                "sql": None,
                "results": None,
                "answer": None,
                "error": "I'm sorry, I couldn't generate a query for that question."
            }

        # Logic to extract SQL if the model still returns reasoning text
        raw_output = generated_sql.strip()
        
        # If the output is huge (like in your logs), we try to find the last SELECT
        if "SELECT" in raw_output.upper():
            # Basic extraction: find where SELECT starts and take the rest
            start_idx = raw_output.upper().find("SELECT")
            sql_clean = raw_output[start_idx:].split(';')[0] + ";"
        else:
            sql_clean = raw_output.replace("```sql", "").replace("```", "").strip()

        print(f"[3/4] Executing Query: {sql_clean}")
        
        # Final step: Execute the generated SQL via our safe DatabaseManager
        data_results = DatabaseManager.execute_query(sql_clean)

        if data_results is None:
            return {
                "question": user_question,
                "sql": sql_clean,
                "results": None,
                "answer": None,
                "error": "The generated SQL was invalid or failed to execute."
            }
        
        # NEW: Synthesis Step - Turn data into a human sentence
        print(f"[4/4] Synthesizing Final Answer...")
        
        synthesis_system = (
            "You are Buddy, a friendly finance assistant. Summarize the provided database results into a concise, natural language answer. "
            "The data is already processed; DO NOT perform manual calculations or summations. "
            "If the data contains many rows, summarize the top items and provide a general overview. "
            "You MUST wrap your final friendly summary inside <answer> tags (e.g., <answer>Your message here</answer>)."
        )
        synthesis_prompt = f"QUESTION: {user_question}\nDATABASE DATA: {data_results}\nFINAL ANSWER:"
        
        human_answer = OllamaService.ask_model(synthesis_prompt, system_instruction=synthesis_system)
        
        if not human_answer:
            final_answer = "I've retrieved the data, but I'm having trouble summarizing it right now."
        else:
            # Logic to clean up the final answer if the model returns reasoning text
            final_answer = human_answer.strip()
            
            # 1. Try to extract content between <answer> tags (greedy match for the last occurrence)
            if "<answer>" in final_answer:
                # Split by <answer> and take the last part, then split by </answer> and take the first
                final_answer = final_answer.split("<answer>")[-1].split("</answer>")[0].strip()
            
            # 2. Fallback: If tags are missing or it's a huge dump of reasoning, take the last paragraph
            elif "Thinking Process:" in final_answer or "Thinking:" in final_answer or len(final_answer) > 1000:
                parts = [p.strip() for p in final_answer.split('\n\n') if p.strip()]
                # The last non-empty block is usually the conclusion
                final_answer = parts[-1] if parts else final_answer
                # Remove internal reasoning headers if they leaked into the last block
                final_answer = final_answer.replace("Thinking Process:", "").replace("Thinking:", "").strip()

        return {
            "question": user_question,
            "sql": sql_clean,
            "results": data_results,
            "answer": final_answer,
            "error": None
        }

if __name__ == "__main__":
    print("--- Dhaman Finance AI Orchestrator ---")
    user_input = "What is the total spend in dec 2025 for individual bank and name of the bank"
    
    response = FinanceOrchestrator.get_answer(user_input)
    
    print("\n--- FINAL AGENT RESPONSE ---")
    if response.get("error"):
        print(f"Error: {response['error']}")
    else:
        print(f"Question: {response['question']}")
        print(f"AI Answer: {response['answer']}")
        print(f"\n(Technical Details - SQL: {response['sql']})")