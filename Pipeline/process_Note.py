# Databricks notebook source


# COMMAND ----------

import pandas as pd
import os
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Code

# COMMAND ----------

def transform_note(filepath):
    # import data and transpose
    with open(filepath, "rb") as f:
        file_content = f.read()
    # Load the binary data into a pandas DataFrame
    df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')

    # drop extra column
    initial_cols = list(map(str.lower, df.columns))
    if 'index' in initial_cols:
        idx = initial_cols.index('index')
        df.drop(df.columns[idx], axis=1, inplace=True)

    # drop duplicate data
    df.drop_duplicates(inplace=True)
    # rearrange column format
    df.rename(columns=str.lower,inplace=True)
    df.columns = df.columns.str.replace(' ','_')
    df.trial_id = df.trial_id.str.upper()
    df.drug = df.drug.str.lower()
    
    if 'All' in df.rfid.values:
        print(filepath)

    output_path = '/dbfs/mnt/testmount/output/Note/'
    df.to_csv(os.path.join(output_path, f.name.split('/')[-1].split('.')[0] + '.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Code

# COMMAND ----------

for f in sorted(dbutils.fs.ls('')):
    filepath = "/" + f.path.replace(':','')
    transform_note(filepath)

# COMMAND ----------

filepath = ''
# import data and transpose
with open(filepath, "rb") as f:
    file_content = f.read()
# Load the binary data into a pandas DataFrame
df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')

# drop extra column
initial_cols = list(map(str.lower, df.columns))
if 'index' in initial_cols:
    idx = initial_cols.index('index')
    df.drop(df.columns[idx], axis=1, inplace=True)

# drop duplicate data
df.drop_duplicates(inplace=True)
# rearrange column format
df.rename(columns=str.lower,inplace=True)
df.columns = df.columns.str.replace(' ','_')
df.trial_id = df.trial_id.str.upper()
df.drug = df.drug.str.lower()
df

# COMMAND ----------


