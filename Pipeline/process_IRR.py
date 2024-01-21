# Databricks notebook source
# COMMAND ----------

import pandas as pd
import os
import io

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configs

# COMMAND ----------

characteristics_IRR = [
 'subject',
 'rfid',
 'cohort',
 'sex',
 'exp_group',
 'def_bsl_irr_1',
 'agg_bsl_irr_1',
 'irr_bsl_tot_1',
 'irr_bsl_scorer_1',
 'def_bsl_irr_2',
 'agg_bsl_irr_2',
 'irr_bsl_total_2',
 'irr_bsl_scorer_2',
 'def_bsl_irr_3',
 'agg_bsl_irr_3',
 'irr_bsl_total_3',
 'irr_bsl_scorer_3',
 'def_bsl_ave',
 'agg_bsl_ave',
 'total_bsl_ave',
 'def_drug_irr_1',
 'agg_drug_irr_1',
 'irr_drug_tot_1',
 'irr_drug_scorer_1',
 'def_drug_irr_2',
 'agg_drug_irr_2',
 'irr_drug_total_2',
 'irr_drug_scorer_2',
 'def_drug_irr_3',
 'agg_drug_irr_3',
 'irr_drug_total_3',
 'irr_drug_scorer_3',
 'def_drug_ave',
 'agg_drug_ave',
 'total_drug_ave',
 'diff_ave_def',
 'diff_ave_agg',
 'diff_ave_total']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Code

# COMMAND ----------

def transformed_irr(filepath):
    print(filepath)
    with open(filepath, "rb") as f:
        file_content = f.read()
    # Load the binary data into a pandas DataFrame
    df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
    for col in df.columns:
        if col in ['rat','sex','group'] or 'scorer' in col:
            df[col] = df[col].fillna('N/A')
    df.columns = characteristics_IRR
    output_path = ''
    df.to_csv(os.path.join(output_path, f.name.split('/')[-1].split('.')[0] + '.csv'), index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Code

# COMMAND ----------

for f in dbutils.fs.ls(''):
    filepath = "/" + f.path.replace(':','')
    transformed_irr(filepath)

# COMMAND ----------

filepath = ''
with open(filepath, "rb") as f:
    file_content = f.read()
# Load the binary data into a pandas DataFrame
df = pd.read_excel(io.BytesIO(file_content), engine='openpyxl')
for col in df.columns:
    if col in ['rat','sex','group'] or 'scorer' in col:
        df[col] = df[col].fillna('N/A')
df.columns = characteristics_IRR
df

# COMMAND ----------


