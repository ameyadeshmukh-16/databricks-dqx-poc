#import os

#print("DEBUG BEFORE SET:", os.environ.get("DATABRICKS_CLUSTER_ID"))

#os.environ["DATABRICKS_HOST"] = "https://adb-7405616830421154.14.azuredatabricks.net/"
#os.environ["DATABRICKS_TOKEN"] = "dapia4062fb927a48f6735f13790defad13a-3"
#os.environ["DATABRICKS_CLUSTER_ID"] = "0322-053555-c8mj9uyo"

#print("DEBUG AFTER SET:", os.environ.get("DATABRICKS_CLUSTER_ID"))

#from databricks.connect import DatabricksSession

#spark = DatabricksSession.builder.getOrCreate()

#spark.sql("SELECT 1").show()


#from dotenv import load_dotenv
#import os

#load_dotenv()

#from databricks.connect import DatabricksSession

#spark = DatabricksSession.builder.getOrCreate()

#spark.sql("SELECT 1").show()


#from databricks.labs.dqx.rule import DQRule

#print(dir(DQRule))

#import sys
#print(sys.path)

from databricks.labs.dqx.rule import DQRule
print(dir(DQRule))