import os
from langchain_ollama import ChatOllama

def get_llm():
    base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
    model = os.getenv("OLLAMA_LLM_MODEL", "llama3.1:8b")
    return ChatOllama(base_url=base_url, model=model, temperature=0, num_ctx=1024,num_predict=256,repeat_penalty=1.1)