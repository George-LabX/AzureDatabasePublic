# Databricks notebook source


# COMMAND ----------

import pandas as pd
import os
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

RFID_OXY = pd.read_csv('', index_col=0)
RFID_OXY.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Code

# COMMAND ----------

def transform_ti(filepath):
    with open(filepath, "rb") as f:
        file_content = f.read()
    # Load the binary data into a pandas DataFrame
    df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
    df['Drug'] = df['Drug'].str.lower()
    df.rename(columns=str.lower,inplace=True)
    df.columns = df.columns.str.replace(' ','_')
    dff = pd.merge(df, RFID_OXY,  how='left', on = ['subject'])
    old_columns = dff.columns.tolist()
    new_columns = [old_columns[-1]] + old_columns[:-1]
    dff = dff[new_columns]

    output_path = ''
    dff.to_csv(os.path.join(output_path, f.name.split('/')[-1].split('.')[0] + '.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Code

# COMMAND ----------

for f in dbutils.fs.ls(''):
    filepath = "/" + f.path.replace(':','')
    transform_ti(filepath)

# COMMAND ----------


