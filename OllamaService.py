import requests
import json

class OllamaService:
    """
    Middleware service to communicate with the local Ollama instance.
    This encapsulates the 'Brain' of the AI Agent.
    """
    
    # Default endpoint for local Ollama
    OLLAMA_URL = "http://localhost:11434/api/generate"
    DEFAULT_MODEL = "qwen3.5:9b" # Update this to your specific version if different

    @classmethod
    def ask_model(cls, prompt: str, system_instruction: str = "", model: str = None):
        """
        Sends a prompt to the local LLM and returns the generated text.
        """
        target_model = model or cls.DEFAULT_MODEL
        
        payload = {
            "model": target_model,
            "prompt": prompt,
            "system": system_instruction,
            "stream": False,  # We want the full answer at once for internal processing
            "options": {
                "temperature": 0,  # Deterministic SQL
                "num_ctx": 8192,   # Increased for reasoning models and large data results
                "num_predict": 4096 # Limit generation to prevent runaway thinking
            }
        }

        try:
            response = requests.post(cls.OLLAMA_URL, json=payload)

            # Better error handling: Ollama returns JSON errors even for 404s
            if response.status_code != 200:
                try:
                    error_msg = response.json().get('error', response.text)
                except:
                    error_msg = response.text
                print(f"Ollama Error ({response.status_code}): {error_msg}")
                if response.status_code == 404:
                    print(f"Suggestion: Run 'ollama pull {target_model}' in your terminal.")
                return None

            data = response.json()
            ai_response = data.get("response", "")

            # Fallback: If 'response' is empty, check the 'thinking' field 
            # (Common in reasoning models like Qwen or DeepSeek)
            if not ai_response and "thinking" in data:
                ai_response = data["thinking"]
                # If we are falling back to thinking, we might have a massive dump. 
                # We keep it for now and let the Orchestrator clean it up.

            if not ai_response:
                # Log only keys to avoid flooding terminal with huge thinking blocks
                print(f"Warning: Model returned an empty response. Keys present: {list(data.keys())}")
                
            return ai_response

        except requests.exceptions.ConnectionError:
            print("Error: Could not connect to Ollama. Is the service running?")
            return None
        except Exception as e:
            print(f"Ollama Error: {e}")
            return None

if __name__ == "__main__":
    # Test the Inference Layer
    print(f"--- Testing Inference Layer (Model: {OllamaService.DEFAULT_MODEL}) ---")
    test_prompt = "Hello! Briefly explain what a SQL SELECT statement does."
    
    answer = OllamaService.ask_model(test_prompt)
    
    if answer:
        print(f"AI Response:\n{answer}")
    else:
        print("Failed to get a response from the model.")