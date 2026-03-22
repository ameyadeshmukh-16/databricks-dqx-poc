from dotenv import load_dotenv
import os

load_dotenv()

def get_env(key: str):
    return os.environ.get(key)