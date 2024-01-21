# Databricks notebook source


# COMMAND ----------

import pandas as pd
import os
import io
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

RFID_OXY = pd.read_csv('', index_col=0)
RFID_OXY['rfid'].isna().sum()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper Functions

# COMMAND ----------

def parse_date(date_string):
    first_date = date_string.split('-')[0]
    date = datetime.strptime(first_date, "%m/%d/%y").date()
    return date

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Code

# COMMAND ----------

def transform_vf(filepath):
    with open(filepath, "rb") as f:
        file_content = f.read()
    # Load the binary data into a pandas DataFrame
    df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
    df['Drug'] = df['Drug'].str.lower()
    df.rename(columns=str.lower,inplace=True)
    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace(' ','_')
    dff = pd.merge(df, RFID_OXY,  how='left', on = ['subject'])
    old_columns = dff.columns.tolist()
    new_columns = [old_columns[-1]] + old_columns[:-1]
    dff = dff[new_columns]

    for col in dff.columns:
        if ('date' in col) and dff[col].dtype != '<M8[ns]':
            dff[col] = dff[col].apply(lambda x: parse_date(x))

    output_path = ''
    dff.to_csv(os.path.join(output_path, f.name.split('/')[-1].split('.')[0] + '.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Code

# COMMAND ----------

for f in dbutils.fs.ls(''):
    filepath = "/" + f.path.replace(':','')
    print(filepath)
    transform_vf(filepath)

# COMMAND ----------


