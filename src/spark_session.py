from dotenv import load_dotenv
import os
from databricks.connect import DatabricksSession

def get_spark():
    load_dotenv()
    return DatabricksSession.builder.getOrCreate()