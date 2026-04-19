import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

# Carga de Modelos
load_dotenv(dotenv_path="_mientorno.env") 
dseek_api_key = os.getenv("DSEEK_API_KEY")

llm = ChatOpenAI(model="deepseek-chat", openai_api_key=dseek_api_key, openai_api_base="https://api.deepseek.com")
