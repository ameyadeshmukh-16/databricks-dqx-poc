"""
Purpose: 
    Load Rules from YAML, 
    convert them into executable checks, 
    Apply DQX rules to staged sales data, 
    split into clean and quarantine

    **---FLOW---**

    sales_staging (raw data)
        ↓
    Load YAML rules
        ↓
    Convert rules → SQL expressions
        ↓
    DQX Engine applies checks
        ↓
    Adds _errors / _warnings columns
        ↓
    Split:
    → sales_clean
    → sales_quarantine
"""

#Import libraries
from databricks.labs.dqx.engine import DQEngine
from databricks.labs.dqx.rule import DQRule#, DQRuleColSet
from databricks.sdk import WorkspaceClient
import yaml
from pyspark.sql.functions import col
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# #Create Spark Session - Connect to Databricks cluster using Databricks Connect - allows us to run Spark code from this local notebook and have it execute on the Databricks cluster
from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.getOrCreate()

# COMMAND ----------
# Load rules from YAML config (versioned in Git) - this allows us to define our data quality rules in a human-readable format and keep them version controlled alongside our code. We will then convert these YAML-defined rules into executable checks that the DQX engine can apply to our data.
with open("C:\\Users\\ameya\\databricks-dqx-poc\\configs\\rules_batch.yml", "r") as f:
    rule_config = yaml.safe_load(f)

# COMMAND ----------
# Initialize DQX engine - we create an instance of the DQEngine which will be responsible for applying our data quality checks to the DataFrame. We pass in the WorkspaceClient which allows the engine to interact with our Databricks workspace, access tables, and store results.
ws = WorkspaceClient()
dqx = DQEngine(ws)

# Define custom check functions if needed - this is where we can define any custom logic that is not covered by the built-in DQRule types. For example, if we want to allow arbitrary SQL expressions as checks, we can define a custom function that takes an expression from the rule config and evaluates it against the DataFrame.
from pyspark.sql.functions import expr
"""
DQX doesnt directly understand your YAML types like:
    min_value
    is_not_null
Hence we created a bridge:  Everything → converted into SQL : SQL → executed using this function

Example YAML rule:-
type: min_value
min: 1

Becomes: "quantity >= 1"
Executed via: expr("quantity >= 1") in our custom_check_functions
"""
custom_check_functions = {
    "sql_expression": lambda df, **kwargs: expr(kwargs["expression"])
}

# COMMAND ----------
# Build DQX rule objects from config - we take the rules defined in our YAML config and convert them into a list of rule dictionaries that the DQX engine can understand. Each rule includes a name, a check definition (which specifies the function to use and its arguments), a criticality level (error or warning), and user metadata such as a message to display when the rule fails.
def build_dqx_rules(rule_config: dict) -> list:         # -> List[Dict] — we can specify the return type as a list of dictionaries which represent the rules in a format that DQX can execute
    rules = []                                          # Initialize an empty list to hold the converted rules

    for r in rule_config["rules"]:                      # Loop through each rule defined in the YAML config

        # Default condition builder - we will convert different types of rules into SQL expressions that can be evaluated by the DQX engine. This is a simple example and can be extended to support more complex rule types as needed.
        if r["type"] == "is_not_null":                                      # If the rule type is "is_not_null", we build a SQL condition that checks if the specified column is not null
            condition = f"{r['column']} IS NOT NULL"    

        elif r["type"] == "is_in_list":                                     # If the rule type is "is_in_list", we build a SQL condition that checks if the specified column's value is in a list of allowed values. We format the allowed values as a comma-separated string of quoted values for the SQL IN clause.
            values = ",".join([f"'{v}'" for v in r["allowed_values"]])      # Convert list of allowed values into a string like "'val1','val2','val3'"
            condition = f"{r['column']} IN ({values})"

        elif r["type"] == "min_value":                                      # If the rule type is "min_value", we build a SQL condition that checks if the specified column's value is greater than or equal to a minimum value defined in the rule.
            condition = f"{r['column']} >= {r['min']}"

        elif r["type"] == "matches_regex":                                  # If the rule type is "matches_regex", we build a SQL condition that checks if the specified column's value matches a regular expression pattern defined in the rule. We use the RLIKE operator for regex matching in SQL.
            condition = f"{r['column']} RLIKE '{r['regex']}'"

        elif r["type"] == "custom_sql":                                     # If the rule type is "custom_sql", we assume that the rule directly provides a SQL expression to evaluate. This allows for maximum flexibility as the rule can define any arbitrary SQL condition.
            condition = r["sql_expression"]

        else:
            continue                                                        # If the rule type is not recognized, we skip it. In a production system, we might want to log a warning or raise an error here.

# Once we have built the SQL condition for the rule, we append a dictionary to the rules list that defines how the DQX engine should execute this rule. We specify the function to use (in this case, "sql_expression" which is our custom function defined earlier), the arguments for that function (the SQL expression we built), the criticality level (error or warning), and user metadata such as the message to display when the rule fails. This allows us to provide more context in the error messages in the quarantine table. Not using the DQRule class directly because we want to leverage our custom SQL expression function for maximum flexibility in defining rules.
        rules.append({                                                      # For each rule, we append a dictionary to the rules list that defines how the DQX engine should execute this rule. We specify the function to use (in this case, "sql_expression" which is our custom function defined earlier), the arguments for that function (the SQL expression we built), the criticality level (error or warning), and user metadata such as the message to display when the rule fails.    
            "name": r["name"],                                              # Name of the rule, e.g. "customer_id_format"
            "check": {                                                      # We define the check as a dictionary that specifies the function to use and its arguments. In this case, we use our custom "sql_expression" function and pass the SQL condition we built as an argument.
                "function": "sql_expression",                               # Use our custom function that evaluates a SQL expression
                "arguments": {                                              # Pass the SQL expression we built as an argument to the function
                    "expression": condition                                 # The SQL condition that will be evaluated against the DataFrame, e.g. "customer_id IS NOT NULL"
                }
            },
            "criticality": (                                                # We set the criticality level based on the rule config. If the rule's criticality is defined as "warning", we set it to "warn" for DQX; otherwise, we default to "error".
                "warn" if r.get("criticality", "error") == "warning"        # If the rule's criticality is "warning", we set it to "warn"
                else r.get("criticality", "error")                          # Otherwise, we use the criticality defined in the rule config (defaulting to "error" if not specified)                     
            ),
            "user_metadata": {                                              # We include user metadata such as the message to display when the rule fails. This allows us to provide more context in the error messages in the quarantine table.
                "message": r["message"]                                     # The message defined in the rule config, e.g. "customer_id cannot be null"
            }
        })

    return rules                                                            # Return the list of rule dictionaries that we built from the YAML config

checks = build_dqx_rules(rule_config)                                       # Convert the rules defined in the YAML config into a list of rule dictionaries that the DQX engine can execute

# COMMAND ----------
# Load staged data
sales_df = spark.table("retail_dqx_poc.sales_staging")                      # Load the raw staged data from the "sales_staging" Delta table into a Spark DataFrame. This is the data that we will apply our DQX rules to in order to identify clean and quarantined records based on the defined data quality checks.
print(f"Input rows: {sales_df.count()}")                                    # Print the number of rows in the input DataFrame to confirm that we have loaded the expected amount of data from the staging table before we apply our DQX checks.

# COMMAND ----------
# Apply DQX checks — this is the core DQX call - we pass in our sales DataFrame, the list of checks we built from the YAML config, and any custom check functions we defined. The DQX engine will evaluate each check against the DataFrame and add columns to indicate which rows failed which checks. 
# The resulting DataFrame (dq_df) will include all the original columns from sales_df plus additional columns such as "_errors" that contain information about any failed checks for each row.

# Apply DQX checks - we pass in our sales DataFrame, the list of checks we built from the YAML config, and any custom check functions we defined. The DQX engine will evaluate each check against the DataFrame and add columns to indicate which rows failed which checks. The resulting DataFrame (dq_df) will include all the original columns from sales_df plus additional columns such as "_errors" that contain information about any failed checks for each row.
dq_df = dqx.apply_checks_by_metadata(                                       # This function applies the checks to the DataFrame based on the metadata we provided in the rules. It evaluates each check's condition against the DataFrame and populates the _errors column with any failures.
    sales_df,                                                               # The input DataFrame containing the staged sales data that we want to validate using our DQX rules.
    checks,                                                                 # The list of checks that we built from our YAML config, which defines the rules to apply to the data.
    custom_check_functions=custom_check_functions                           # Any custom check functions that we defined, which allows us to evaluate arbitrary SQL expressions as part of our checks.
)

# Clean data (NO errors) - we filter the resulting DataFrame to separate the clean records (those that passed all checks and have no errors) from the quarantined records (those that failed one or more checks and have entries in the _errors column). 
# The clean records will be written to the "sales_clean" Delta table, while the quarantined records will be written to the "sales_quarantine" Delta table for further analysis and remediation.
good_df = dq_df.filter(col("_errors").isNull())

# Quarantine data (HAS errors) - we filter the resulting DataFrame to identify the records that failed one or more checks. 
# These records will have entries in the _errors column that indicate which checks they failed and the associated error messages. We will write these quarantined records to a separate Delta table for further analysis, allowing us to understand the data quality issues and take corrective actions as needed.
quarantine_df = dq_df.filter(col("_errors").isNotNull())

# Counts - we print the counts of input rows, clean rows, and quarantined rows to get a quick summary of the results of our DQX checks. This allows us to see how many records passed all checks and how many were flagged for quarantine due to data quality issues.
print(f"Input rows: {dq_df.count()}")
print(f"Clean rows: {good_df.count()}")
print(f"Quarantined rows: {quarantine_df.count()}")

# COMMAND ----------
# Persist clean data
(good_df.write                                                              # We write the clean DataFrame to a Delta table called "sales_clean". This allows us to efficiently store and query the clean data in subsequent steps, such as for reporting, analytics, or feeding into downstream systems.
    .format("delta")                                                        # Write data in Delta format for ACID transactions and efficient querying
    .mode("overwrite")                                                      # Overwrite the table if it already exists. In a production system, we might want to use "append" mode instead to keep historical clean data.
    .option("mergeSchema", "true")                                          # Allow schema merging in case the DQX checks added new columns (like _errors) that are not in the original schema of the sales_staging table. This ensures that we can write the resulting DataFrame to the Delta table without schema conflicts.
    .saveAsTable("retail_dqx_poc.sales_clean"))                             # Save the DataFrame as a managed Delta table in the "retail_dqx_poc" database with the name "sales_clean".

# COMMAND ----------
# Persist quarantine data — DQX adds _dqx_check_status and _dqx_check_failures columns
(quarantine_df.write                                                        # We write the quarantined DataFrame to a Delta table called "sales_quarantine". This allows us to efficiently store and query the quarantined data, which includes information about which checks failed for each record. We can use this information for analysis and remediation of data quality issues.
    .format("delta")                                                        # Write data in Delta format for ACID transactions and efficient querying
    .mode("overwrite")                                                      # Overwrite the table if it already exists. In a production system, we might want to use "append" mode instead to keep historical quarantine data for trend analysis.
    .option("mergeSchema", "true")                                          # Allow schema merging in case the DQX checks added new columns (like _errors) that are not in the original schema of the sales_staging table. This ensures that we can write the resulting DataFrame to the Delta table without schema conflicts.
    .saveAsTable("retail_dqx_poc.sales_quarantine"))                        # Save the DataFrame as a managed Delta table in the "retail_dqx_poc" database with the name "sales_quarantine".

# COMMAND ----------
# Preview quarantine failure reasons - we select relevant columns from the quarantine DataFrame to preview the failure reasons for the quarantined records. This allows us to quickly see which checks are failing and what the associated error messages are, which can help us understand the data quality issues and prioritize remediation efforts.
quarantine_df.select(                                                                   # We select relevant columns from the quarantine DataFrame to preview the failure reasons for the quarantined records. This allows us to quickly see which checks are failing and what the associated error messages are, which can help us understand the data quality issues and prioritize remediation efforts.
    "transaction_id", "division", "customer_id", "quantity", "unit_price","_errors"     # We select key columns that are relevant for understanding the context of the quarantined records (e.g. transaction_id, division, customer_id, quantity, unit_price) along with the _errors column that contains information about which checks failed and the associated error messages.
).show(20, truncate=False)                                                              # We show the first 20 rows of the resulting DataFrame without truncating the columns, so we can see the full error messages in the _errors column for the quarantined records.

"""
ARCHITECTURE is as below:

    YAML Rules
        ↓
    build_dqx_rules()
        ↓
    SQL Conditions
        ↓
    DQX Engine
        ↓
    _errors column
        ↓
    Clean / Quarantine split

Compared to default DQX, this approach uses: ***Just like a mini rule engine on top of DQX***
YAML + Builder  -> Business-friendly
SQL-based rules -> Extremely flexible
Custom function -> Extensible
Metadata-driven	-> Production-ready

"""