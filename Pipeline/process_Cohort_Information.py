# Databricks notebook source
# MAGIC %md
# MAGIC #### Imports

# COMMAND ----------

import os
import pandas as pd
import numpy as np
import psycopg2
import math
from collections import defaultdict
from datetime import date, datetime
from os import listdir
from os.path import isfile, join

# COMMAND ----------

# MAGIC %md
# MAGIC #### Config file for reading in csv files and accessing DB
# MAGIC Previosuly the config.py file

# COMMAND ----------

# Previously the config.py file

from os import listdir
from os.path import isfile, join

TABLE_TRIAL_LGA = 'trial_lga'
TABLE_TRIAL_SHA = 'trial_sha'
TABLE_TRIAL_PR = 'trial_pr'
TABLE_TRIAL_SHOCK = 'trial_shock'
TABLE_TAIL_IMMERSION = 'tail_immersion'
TABLE_VON_FREY = 'von_frey'

# characteristics for TRIAL LGA, SHA data
characteristics_LGA_SHA = ['rfid','subject','room','cohort','trial_id','drug','box', 'start_time', 'end_time',
 'start_date','end_date','active_lever_presses','inactive_lever_presses','reward_presses','timeout_presses',
 'active_timestamps','inactive_timestamps','reward_timestamps','timeout_timestamps']

# characteristics for TRIAL PR data
characteristics_PR = ['rfid', 'subject', 'room', 'cohort', 'trial_id', 'drug', 'box','start_time', 'end_time', 
 'start_date', 'end_date', 'last_ratio', 'breakpoint', 'active_lever_presses', 'inactive_lever_presses',
 'reward_presses', 'reward_points']

# characteristics for TRIAL SHOCK data
characteristics_SHOCK = ['rfid', 'subject', 'room', 'cohort', 'trial_id', 'drug', 'box',
       'start_time', 'end_time', 'start_date', 'end_date',
       'total_active_lever_presses', 'total_inactive_lever_presses',
       'total_shocks', 'total_reward', 'rewards_after_first_shock',
       'rewards_got_shock', 'reward_timestamps']

# characteristics for TAIL IMMERSION data
characteristics_TI = ['rfid', 'subject', 'cohort', 'sex', 'experiment_group', 'drug', 'tail_immersion_1_time', 
    'tail_immersion_2_time','tail_immersion_3_time', 'tail_immersion_difference_tolerance',
    'tail_immersion_1_date', 'tail_immersion_2_date','tail_immersion_3_date']

# characteristics for VON FREY data
characteristics_VF = ['rfid', 'subject', 'cohort', 'sex', 'experiment_group', 'drug',
                        'vf1_right_force_1', 'vf1_right_force_2', 'vf1_right_force_3',
                        'vf1_right_force_avg', 'vf1_right_time_1', 'vf1_right_time_2',
                        'vf1_right_time_3', 'vf1_right_avg', 'vf1_left_force_1',
                        'vf1_left_force_2', 'vf1_left_force_3', 'vf1_left_force_avg',
                        'vf1_left_time_1', 'vf1_left_time_2', 'vf1_left_time_3', 'vf1_left_avg',
                        'von_frey_1_date', 'vf2_right_force_1', 'vf2_right_force_2',
                        'vf2_right_force_3', 'vf2_right_force_avg', 'vf2_right_time_1',
                        'vf2_right_time_2', 'vf2_right_time_3', 'vf2_right_avg',
                        'vf2_left_force_1', 'vf2_left_force_2', 'vf2_left_force_3',
                        'vf2_left_force_avg', 'vf2_left_time_1', 'vf2_left_time_2',
                        'vf2_left_time_3', 'vf2_left_avg', 'von_frey_2_date',
                        'von_frey_1_force', 'von_frey_1_time', 'von_frey_2_force',
                        'von_frey_2_time', 'von_frey_difference_force']

integer_columns = ['rfid','cohort','box','active_lever_presses','inactive_lever_presses','reward_presses','timeout_presses',
                   'last_ratio', 'breakpoint', 'total_active_lever_presses', 'total_inactive_lever_presses',
                   'total_shocks', 'total_reward', 'rewards_after_first_shock']

COCAINE_COHORT_DIR_FILEPATH = '/dbfs/mnt/testmount/input/cohort_information/cocaine_cohort_information'
OXY_COHORT_DIR_FILEPATH = '/dbfs/mnt/testmount/input/cohort_information/oxy_cohort_information'

COCAINE_COHORT_ALL = [join(COCAINE_COHORT_DIR_FILEPATH, f) for f in listdir(COCAINE_COHORT_DIR_FILEPATH) if isfile(join(COCAINE_COHORT_DIR_FILEPATH, f))]

OXYCODONE_COHORT_ALL = [join(OXY_COHORT_DIR_FILEPATH, f) for f in listdir(OXY_COHORT_DIR_FILEPATH) if isfile(join(OXY_COHORT_DIR_FILEPATH, f))]

SUBJECT_OUTPUT_FILEPATH = '/dbfs/mnt/testmount/output/Cohort_Information/Subject/cohort_subject.csv'
MEASUREMENT_OUTPUT_FILEPATH = '/dbfs/mnt/testmount/output/Cohort_Information/Measurement/cohort_measurement.csv'

CHARACTERISTIC_TABLE_NAME = 'subject'
MEASUREMENT_TABLE_NAME = 'measurement'
# These are all the columns that will be accessed for its value to be inserted into its respective column in the
# characteristic table
cocaine_characteristics_list = [
    'Rat', 'Experiment Group', 'Drug Group', 'Cohort', 'Sex', 'RFID', 'D.O.B', 'Date of Wean', 'Date of Ship',
    'Litter Number', 'Litter Size', 'Coat Color', 'Ear Punch',
    'Shipping Box', 'Rack', 'Arrival Date', 'Handled Collection', 'Surgery Date',
    'Surgeon', 'Surgery Assist', 'Date of Eye Bleed',
    'Short Access Start Date', 'Short Access End Date',
    'Long Access Start Date', 'Long Access End Date', 'LgA15 Date',
    'LgA16 Date', 'LgA17 Date', 'LgA18 Date', 'LgA19 Date', 'LgA20 Date',
    'Irritability 1 By', 'Irritability 1 Date', 'Irritability 2 By', 'Irritability 2 Date',
    'Progressive Ratio 1 Date', 'Progressive Ratio 2 Date', 'Progressive Ratio 3 Date'
    'Brevital', 'Brevital Date', 'Brevital By',
    'Pre-Shock Date', 'Shock (0.1mA) Date', 'Shock (0.2mA) Date',
    'Shock (0.3mA) Date', 'Group for Pre-Shock', 'Group for Shock',
    'Recatheter Surgeon', 'Recatheter Surgery Date',
    'Dissection', 'Dissection Date', 'Date of Death'

    # Saved for seoarate
    # 'Date Excluded', 'Exclude Reason'
]

cocaine_measurements_list = [
    {'measurement_name': 'urine', 'col_name': 'Urine', 'counts': [i for i in range(1, 3)], 'col_suffix': ['Date', 'Collection']},
    {'measurement_name': 'weight', 'col_name': 'Weight', 'counts': [i for i in range(1, 11)], 'col_suffix': ['Value', 'Date']},
    {'measurement_name': 'feces', 'col_name': 'Feces', 'counts': [i for i in range(1, 5)], 'col_suffix': ['Date', 'Collection']},
]

cocaine_excel_converters = {
    'Rat': str, 
    'Experiment Group': str, 
    'Drug Group': str, 
    'Cohort': int, # int
    'Sex': str, 
    'RFID': str, 
    'D.O.B': pd.Timestamp, 
    'Date of Wean': pd.Timestamp, 
    'Date of Ship': pd.Timestamp,
    'Litter Number': int, 
    'Litter Size': int, 
    'Coat Color': str, 
    'Ear Punch': str,
    'Shipping Box': str, 
    'Rack': str, 
    'Arrival Date': pd.Timestamp, 
    'Handled Collection': str, 
    'Surgery Date': pd.Timestamp,
    'Surgeon': str, 
    'Surgery Assist': str, 
    'Date of Eye Bleed': pd.Timestamp,
    'Short Access Start Date': pd.Timestamp, 
    'Short Access End Date': pd.Timestamp,
    'Long Access Start Date': pd.Timestamp, 
    'Long Access End Date': pd.Timestamp, 
    'LgA15 Date': pd.Timestamp,
    'LgA16 Date': pd.Timestamp, 
    'LgA17 Date': pd.Timestamp, 
    'LgA18 Date': pd.Timestamp, 
    'LgA19 Date': pd.Timestamp, 
    'LgA20 Date': pd.Timestamp,
    'Irritability 1 Collection': str, 
    'Irritability 1 Date': pd.Timestamp, 
    'Irritability 2 Collection': str, 
    'Irritability 2 Date': pd.Timestamp,
    'Progressive Ratio 1 Date': pd.Timestamp, 
    'Progressive Ratio 2 Date': pd.Timestamp, 
    'Progressive Ratio 3 Date': pd.Timestamp,
    'Brevital': str, # Pass/Fail 
    'Brevital Date': pd.Timestamp, 
    'Brevital Collection': str,
    'Pre-Shock Date': pd.Timestamp, 
    'Shock (0.1mA) Date': pd.Timestamp, 
    'Shock (0.2mA) Date': pd.Timestamp,
    'Shock (0.3mA) Date': pd.Timestamp, 
    'Group for Pre-Shock': int, 
    'Group for Shock': int,
    'Recatheter Surgeon': str, 
    'Recatheter Surgery Date': pd.Timestamp,
    'Dissection': str, 
    'Dissection Date': pd.Timestamp, 
    'Date of Death': pd.Timestamp
}

oxycodone_excel_converters = {
    'Rat': str, 
    'RFID': str, 
    'Cohort': int, # int
    'Sex': str, 
    'D.O.B': pd.Timestamp, 
    'Coat Color': str, 
    'Ear Punch': str, 
    'Shipping Box': str, 
    'Date of Wean': pd.Timestamp, 
    'Date of Ship': pd.Timestamp, 
    'Litter Number': int, # int
    'Litter Size': int, # int
    'Rack': str, 
    'Arrival Date': pd.Timestamp, 
    'Age at Arrival': int, # int
    'Handled Collection': str, 
    'Experiment Group': str, 
    'Drug Group': str, 
    'Surgery Date': pd.Timestamp, 
    'Age at Surgery': int, # int
    'Surgeon': str, 
    'Surgery Assist': str, 
    'Date of Eye Bleed': pd.Timestamp,
    'UV': str, 
    'Age at ShA': int, # int
    'Short Access Start Date': pd.Timestamp, 
    'Short Access End Date': pd.Timestamp,
    'Age at LgA': int, # int
    'Long Access Start Date': pd.Timestamp, 
    'Long Access End Date': pd.Timestamp, 
    'Brevital Date': pd.Timestamp, 
    'Brevital': str, 
    'Von Frey 1 Collection': str, 
    'Von Frey 1 Date': pd.Timestamp, 
    'Von Frey 2 Collection': str, 
    'Von Frey 2 Date': pd.Timestamp,
    'Tail Immersion 1 Collection': str, 
    'Tail Immersion 1 Date': pd.Timestamp, 
    'Tail Immersion 2 Collection': str, 
    'Tail Immersion 2 Date': pd.Timestamp, 
    'Tail Immersion 3 Collection': str, 
    'Tail Immersion 3 Date': pd.Timestamp,
    'Progressive Ratio 1 Date': pd.Timestamp, 
    'Progressive Ratio 2 Date': pd.Timestamp, 
    'LgA Pre-Treatment 1 Date': pd.Timestamp, 
    'LgA Pre-Treatment 2 Date': pd.Timestamp,
    'LgA Pre-Treatment 3 Date': pd.Timestamp, 
    'LgA Pre-Treatment 4 Date': pd.Timestamp, 
    'LgA Post-Treatment 1 Date': pd.Timestamp, 
    'LgA Post-Treatment 2 Date': pd.Timestamp, 
    'LgA Post-Treatment 3 Date': pd.Timestamp, 
    'LgA Pre-Treatment 4 Date': pd.Timestamp,
    'Treatment 1 Date': pd.Timestamp, 
    'Treatment 1 Group': str, 
    'Treatment 1 Start Time': str, # Currently stored as string. Ideally as time format
    'Treatment 2 Date': pd.Timestamp,
    'Treatment 2 Group': str,
    'Treatment 2 Start Time': str,
    'Treatment 3 Date': pd.Timestamp,
    'Treatment 3 Group': str,
    'Treatment 3 Start Time': str,
    'Age at Dissection': int, # int 
    'Dissection Date': pd.Timestamp, 
    'Dissection Group': str, 
    'Date of Death': pd.Timestamp, 
    # 'Days of Experiment': int
    # Save for separate
    # 'Days of Experiment', 'Reason for Removal from Study', 'Was Replaced', 'Replaced', 'Last Session'
}

oxycodone_characteristics_list = [
    'Rat', 'RFID', 'Cohort', 'Sex', 'D.O.B', 'Coat Color', 'Ear Punch', 'Shipping Box', 
    'Date of Wean', 'Date of Ship', 'Litter Number', 'Litter Size', 'Rack', 
    'Arrival Date', 'Age at Arrival', 'Handled Collection', 'Experiment Group', 'Drug Group', 
    'Surgery Date', 'Age at Surgery', 'Surgeon ', 'Surgery Assist', 'Date of Eye Bleed',
    'UV', 'Age at ShA', 'Short Access Start Date', 'Short Access End Date ',
    'Age at LgA', 'Long Access Start Date', 'Long Access End Date', 'Brevital Date', 'Brevital', 
    'Von Frey 1 Collection', 'Von Frey 1 Date', 'Von Frey 2 Collection', 'Von Frey 2 Date',
    'Tail Immersion 1 Collection', 'Tail Immersion 1 Date', 'Tail Immersion 2 Collection', 'Tail Immersion 2 Date', 'Tail Immersion 3 Collection', 'Tail Immersion 3 Date',
    'Progressive Ratio 1 Date', 'Progressive Ratio 2 Date', 'LgA Pre-Treatment 1 Date', 'LgA Pre-Treatment 2 Date',
    'LgA Pre-Treatment 3 Date', 'LgA Pre-Treatment 4 Date', 'LgA Post-Treatment 1 Date', 'LgA Post-Treatment 2 Date', 
    'LgA Post-Treatment 3 Date', 'LgA Pre-Treatment 4 Date',
    'Treatment 1 Date', 'Treatment 1 Group', 'Treatment 1 Start Time',
    'Treatment 2 Date', 'Treatment 2 Group', 'Treatment 2 Start Time', 'Treatment 3 Date', 'Treatment 3 Group', 'Treatment 3 Start Time',
    'Age at Dissection', 'Dissection Date', 'Dissection Group', 'Date of Death', 'Days of Experiment'
    # Save for separate
    # 'Days of Experiment', 'Reason for Removal from Study', 'Was Replaced', 'Replaced', 'Last Session'
]

oxycodone_measurements_list = [
    {'measurement_name': 'urine', 'col_name': 'Urine', 'counts': [i for i in range(1, 3)], 'col_suffix': ['Date', 'Collection']},
    {'measurement_name': 'weight', 'col_name': 'Weight', 'counts': [i for i in range(1, 13)], 'col_suffix': ['Value', 'Date']},
    {'measurement_name': 'feces', 'col_name': 'Feces', 'counts': [i for i in range(1, 4)], 'col_suffix': ['Date', 'Collection']},
]

exit_tab_list = [
    'exit day', 'last good session', 'exit code', 'complete', 'tissue collected', 'exit notes', 'replaced by'
]

# Union of characteristics from Cocaine and Oxycodone
final_characteristics_list = [
    'rfid', 'rat', 'cohort', 'experiment group', 'drug group', 'sex', 'arrival date', 'age at arrival', 'uv', 'brevital', 'brevital date', 'brevital collection', 'lga15 date',
    'lga16 date', 'lga17 date', 'lga18 date', 'lga19 date', 'lga20 date', 'age at lga', 'long access start date', 'long access end date', 'age at sha', 'short access start date',
    'short access end date', 'pre-shock date', 'shock (0.1ma) date', 'shock (0.2ma) date', 'shock (0.3ma) date',
    'female swab 1 collection', 'female swab 1 date', 'female swab 1 analysis', 'female swab 2 collection', 'female swab 2 date', 'female swab 2 analysis', 'female swab 3 collection', 'female swab 3 date', 'female swab 3 analysis',
    'irritability 1 collection', 'irritability 1 date', 'irritability 2 collection', 'irritability 2 date',  'von frey 1 collection', 'von frey 1 date', 'von frey 2 collection', 'von frey 2 date',
    'tail immersion 1 collection', 'tail immersion 1 date', 'tail immersion 2 collection', 'tail immersion 2 date', 'tail immersion 3 collection', 'tail immersion 3 date', 'lga pre-treatment 1 date', 'lga pre-treatment 2 date', 
    'lga pre-treatment 3 date', 'lga pre-treatment 4 date', 'lga post treatment 1 date', 'lga post treatment 2 date', 'lga post treatment 3 date', 'lga post treatment 4 date', 'progressive ratio 1 date', 'progressive ratio 2 date', 'progressive ratio 3 date', 
    'treatment 1 date', 'treatment 1 group', 'treatment 1 start time', 'treatment 2 date', 'treatment 2 group', 'treatment 2 start time', 'treatment 3 date', 'treatment 3 group', 'treatment 3 start time',
    'treatment 4 date', 'treatment 4 group', 'treatment 4 start time' , 'coat color', 'd.o.b', 'date of eye bleed', 'date of ship', 'date of wean', 'age at dissection', 'dissection group', 'dissection date', 'ear punch', 'group for pre-shock',
    'group for shock', 'handled collection', 'litter number', 'litter size', 'rack', 'recatheter surgeon', 'recatheter surgery date', 'shipping box', 'surgeon', 'surgery assist', 'surgery date',
    'age at surgery', 'date of death', 'days of experiment', 'exit day', 'last good session', 'exit code', 'complete', 'tissue collected', 'exit notes', 'replaced by'
]

characteristic_table_cols = [
    'rfid',
    'rat',
    'cohort',
    'experiment_group',
    'drug_group',
    'sex',
    'arrival_date',
    'age_at_arrival', 
    'uv',
    'brevital', 
    'brevital_date', 
    'brevital_technicians', 
    'lga_15_date',
    'lga_16_date',
    'lga_17_date',
    'lga_18_date', 
    'lga_19_date',
    'lga_20_date',
    'age_at_lga', 
    'long_access_start_date', 
    'long_access_end_date', 
    'age_at_sha', 
    'short_access_start_date',
    'short_access_end_date',
    'pre_shock_date',
    'shock_1_date',
    'shock_2_date',
    'shock_3_date',
    'female_swab_1_technicians',
    'female_swab_1_date',
    'female_swab_1_analysis',
    'female_swab_2_technicians',
    'female_swab_2_date',
    'female_swab_2_analysis',
    'female_swab_3_technicians',
    'female_swab_3_date',
    'female_swab_3_analysis',
    'irritability_1_technicians',
    'irritability_1_date',
    'irritability_2_technicians',
    'irritability_2_date',
    'von_frey_1_technicians',
    'von_frey_1_date',
    'von_frey_2_technicians',
    'von_frey_2_date',
    'tail_immersion_1_technicians',
    'tail_immersion_1_date',
    'tail_immersion_2_technicians',
    'tail_immersion_2_date',
    'tail_immersion_3_technicians',
    'tail_immersion_3_date',
    'lga_pre_treatment_1_date',
    'lga_pre_treatment_2_date',
    'lga_pre_treatment_3_date',
    'lga_pre_treatment_4_date',
    'lga_post_treatment_1_date',
    'lga_post_treatment_2_date',
    'lga_post_treatment_3_date',
    'lga_post_treatment_4_date',
    'progressive_ratio_1_date',
    'progressive_ratio_2_date',
    'progressive_ratio_3_date',
    'treatment_1_date',
    'treatment_1_group',
    'treatment_1_start_time',
    'treatment_2_date',
    'treatment_2_group',
    'treatment_2_start_time',
    'treatment_3_date',
    'treatment_3_group',
    'treatment_3_start_time',
    'treatment_4_date',
    'treatment_4_group',
    'treatment_4_start_time',
    'coat_color',
    'date_of_birth',
    'date_of_eye_bleed',
    'date_of_ship',
    'date_of_wean',
    'age_at_dissection',
    'dissection_group',
    'dissection_date',
    'ear_punch',
    'group_pre_shock',
    'group_shock',
    'handled_by',
    'litter_number',
    'litter_size', 
    'rack',
    'recatheter_surgeon',
    'recatheter_surgery_date',
    'shipping_box',
    'surgeon',
    'surgery_assist',
    'surgery_date',
    'age_at_surgery',
    'date_of_death',
    'days_of_experiment',
    'exit_day',
    'last_good_session',
    'exit_code',
    'complete',
    'tissue_collected',
    'exit_notes',
    'replaced_by'
]

measurement_table_cols = [
    'rfid', 'measurement_name', 'measurement_value', 'drug_group', 'cohort', 'measure_number', 'date_measured', 'technician'
]

SUBJECT_INGEST_MAPPING = {}

for k, v in zip(final_characteristics_list, characteristic_table_cols):
    SUBJECT_INGEST_MAPPING[k] = v


# COMMAND ----------

pd.DataFrame(columns=characteristic_table_cols)

# COMMAND ----------

df_subject = pd.DataFrame(columns=characteristic_table_cols)
df_subject
df_subject.to_csv(SUBJECT_OUTPUT_FILEPATH, index=False)

# COMMAND ----------

df_measurement = pd.DataFrame(columns=measurement_table_cols)
df_measurement
df_measurement.to_csv(MEASUREMENT_OUTPUT_FILEPATH, index=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Class to access connection to database (and other services in the processes)
# MAGIC Previously the pipeline.py file

# COMMAND ----------

# class Pipeline(object):

#     def __init__(self):
#         self.conn, self.cur = self.connect_db()

#     @staticmethod
#     def connect_db():        
#         try:
#             conn = psycopg2.connect(user=DATABASE_USERNAME,
#                                     password=DATABASE_PASSWORD,
#                                     host=DATABASE_HOST,
#                                     port=DATABASE_PORT,
#                                     database=DATABASE_NAME,
#                                     options=f'-c search_path={DATABASE_SCHEMA}'
#                                     )
#             cur = conn.cursor()
#         except Exception as error:
#             print(f'Cannot connect to DB due to "{error}" error')

#         return (conn, cur)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Class to create a new object to contain each row of rat information
# MAGIC Previously the subject.py file

# COMMAND ----------

class Subject:

    def __init__(self, subject_row, type):
        '''
            characteristics are only one value
            Measurements are repeated (numbered) values
        '''
        self.rfid = subject_row['rfid']
        # self.characteristics = defaultdict(lambda: None)

        # Used to pass into pd.Dataframe function to create df that can just be appended to file
        # Can't just use dict and need to use defaultdict as the measurement counts are not the same
        # across cocaine and oxycodone --> differing frequency
        self.characteristics = defaultdict(lambda: None)
        for col in characteristic_table_cols:
            self.characteristics[col] = []

        self.measurements = list()
        # Separate measurement list for the different drugs as their frequency of occurrence differs slightly
        self.measurement_cols = cocaine_measurements_list if type == 'cocaine' else oxycodone_measurements_list
        self.subject_row = subject_row
        self.type = type

        # Used later to create a dataframe out of the dict to append to csv file
        self.measure_df_mapping = {}
        for col in measurement_table_cols:
            self.measure_df_mapping[col] = []
        # super().__init__()
        # self.conn, self.cur = Pipeline.connect_db()

    def process_characteristics(self):
        '''
            Get all the information about the subject
            that isn't a measurement
        '''
        for characteristic in final_characteristics_list:
            characteristic_value = self.subject_row.get(characteristic, default=None)
            actual_col_name = SUBJECT_INGEST_MAPPING[characteristic]
            if 'date' in characteristic.lower() or 'exit day' in characteristic.lower():
                # self.characteristics[actual_col_name] = self.format_date(characteristic_value)
                self.characteristics[actual_col_name].append(self.format_date(characteristic_value))
            elif any(technician_col in characteristic.lower() for technician_col in ['collection']):
                # self.characteristics[actual_col_name] = characteristic_value
                self.characteristics[actual_col_name].append(characteristic_value)
            else:
                # self.characteristics[actual_col_name] = characteristic_value
                self.characteristics[actual_col_name].append(characteristic_value)


        # for k, v in self.characteristics.items():
        #     print(f'{k}' + ' --> ' + f'{v}')

    def process_measurements(self):
        '''
            Get all information that is repeated measured (for a possibly arbitrary number of times) - weights, feces, etc
        '''

        for measurement_dict in self.measurement_cols:
    
            counts = measurement_dict['counts']
            suffixes = measurement_dict['col_suffix']

            # Loop through each count of the measurement
            for count_num in counts:

                # Use default dict as we will need all access all values to insert into database
                insert_dict = defaultdict(lambda: None)
                col_name = measurement_dict['col_name']
                current_number = count_num

                insert_dict['rfid'] = self.subject_row.get('rfid')
                insert_dict['measurement_name'] = measurement_dict['measurement_name']
                insert_dict['measure_number'] = current_number
                insert_dict['drug_group'] = self.type
                insert_dict['cohort'] = self.subject_row.get('cohort')
                
                for suffix in suffixes:
                    # This is used to query for the value of a specific column referencing a measurement value for the current subject (row)
                    full_col_name = ' '.join([col_name, str(current_number), suffix]).strip()
                    full_col_name = full_col_name.lower()
                    if suffix == 'Value' or suffix == 'Analysis':
                        m_val =  self.subject_row.get(full_col_name, default=None)
                        insert_dict['measurement_value'] = int(m_val) if not math.isnan(m_val) else None
                    elif suffix == 'By' or suffix == 'Collection':
                        # Storing multiple technicians as csv for ease of reversal
                        insert_dict['technician'] = self.subject_row.get(full_col_name)
                    elif suffix == 'Date':
                        insert_dict['date_measured'] = self.format_date(self.subject_row.get(full_col_name))
                # 
                for col in measurement_table_cols:
                    self.measure_df_mapping[col].append(insert_dict[col])
                # self.measurements.append(insert_dict)
        # for k, v in self.measure_df_mapping.items():
        #     print(f'{k}' + ' --> ' + f'{v}')
        return 

    @staticmethod
    def format_date(date: datetime):
        '''
            Convert all date values into a consistent format. If the value is null, replace it with None
            for insertion into the database
        '''
        if pd.isnull(date):
            return None
        else:
            return datetime.strftime(date, "%m/%d/%Y %H:%M:%S")
    
    @staticmethod
    def format_multiple_values_into_array(comma_separated_string: str):
        '''
            Formats csv values into insertable format
        '''
        if not comma_separated_string or comma_separated_string.lower() == 'nan':
            return None

        css_formatted = ','.join([f'\"{value}\"' for value in comma_separated_string.split(',')])
        return f'{{{css_formatted}}}'

    def construct_characteristic_sql_string(self):
        values = ','.join(['%s'] * CHARACTERISTIC_TABLE_COLUMNS_COUNT)
        sql_string = f"""INSERT INTO {CHARACTERISTIC_TABLE_NAME} VALUES ({values}) ON CONFLICT (rfid) DO NOTHING;""" 
        sql_string_values = list([self.characteristics[key] for key in final_characteristics_list])
        for i in range(len(sql_string_values)):
            if sql_string_values[i] is not None and isinstance(sql_string_values[i], float):
                if math.isnan(sql_string_values[i]):
                    sql_string_values[i] = None
        return sql_string, sql_string_values

    def construct_measurement_sql_string(self, measurement_row):
        # values_placeholder = ','.join(['%s'] * len(measurement_table_cols))
        # sql_string = f"""INSERT INTO {MEASUREMENT_TABLE_NAME} (rfid, measurement_name, measurement_value, drug_group, cohort, measure_number, date_measured,
        #  technician) VALUES ({values_placeholder}) ON CONFLICT (rfid, measurement_name, measure_number) DO NOTHING"""
        sql_string_values = [measurement_row[col] for col in measurement_table_cols]
        # return sql_string, sql_string_values
        return sql_string_values

    def insert_characteristics(self):
        df = pd.DataFrame(self.characteristics)
        append_to = pd.read_csv(SUBJECT_OUTPUT_FILEPATH)
        append_to = pd.concat([append_to, df], axis=0)
        append_to.to_csv(SUBJECT_OUTPUT_FILEPATH, mode="w", index=False, header=True)
        print("Characteristic data Appended Successfully")

    def insert_measurements(self):
        # for k, arr in self.measure_df_mapping.items():
        #     print(f'Arr has length of {len(arr)} with the key {k} and the arr {arr}')
        df = pd.DataFrame(self.measure_df_mapping)
        append_to = pd.read_csv(MEASUREMENT_OUTPUT_FILEPATH)
        append_to = pd.concat([append_to, df], axis=0)
        append_to.to_csv(MEASUREMENT_OUTPUT_FILEPATH, mode="w", index=False, header=True)
        print("Measurement data Appended Successfully")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Class to process each cohort information sheet
# MAGIC Goes through each subject (row) in the cohort (excel file) and process its characteristics and measurements and ingests them 

# COMMAND ----------

class CohortProcess:

    def __init__(self, filepath, type):
        self.excel_filepath = filepath
        self.cohort_subjects = []
        self.subjects = []
        self.type = type
        if type == 'cocaine':
            self.df_sheets = pd.read_excel(self.excel_filepath, sheet_name = ['Timeline','Exit Tab'], converters = cocaine_excel_converters)
        else:
            self.df_sheets = pd.read_excel(self.excel_filepath, sheet_name = None, converters = oxycodone_excel_converters)
        self.df_timeline = self.get_df_excel_file(self.df_sheets['Timeline']) if 'Timeline' in self.df_sheets.keys() else self.get_df_excel_file(self.df_sheets['Information Sheet'])
        self.df_exit_tab = self.get_df_excel_file(self.df_sheets['Exit Tab'])
        self.df_exit_tab = self.df_exit_tab.drop(['rat', 'cohort'], axis=1)
        self.df_exit_tab = self.organize_exit_tabs(self.df_exit_tab)

        self.df_final = pd.merge(self.df_timeline, self.df_exit_tab, how='left', on='rfid')

    def organize_exit_tabs(self, df):
        col = df.columns

        out = []
        for rfid in df['rfid'].unique():
            if sum(df['rfid']==rfid)==1:
                continue
            db = df[df['rfid']==rfid]
            sampdf = {}
            for i in col:
                sampdf[i] = db.iloc[0][i]
            for n in range(1,len(db)):
                for i in col:
                    if type(sampdf[i]) == str and type(db.iloc[n][i]) == str and sampdf[i] != db.iloc[n][i]:
                        sampdf[i] = sampdf[i]+', '+db.iloc[1][i]
            out.append(pd.DataFrame(sampdf, index=[db.index[0]]))
            df = df.drop(db.index)
        if len(out) == 0:
            return df
        return df.append(pd.concat(out))
 
    def get_df_excel_file(self, df: pd.DataFrame):
        '''
            Function to modify passed in dataframe and convert all columns with 'date' in the column name 
            into datetime objects for easier processing into the database and other necessarily format handling.
            Converted all column names to be lowercase
        '''
        df['RFID'] = df['RFID'].astype(str)
        df.columns = (df.columns.str.replace(u'\xa0', u'')).str.strip()
        df.columns = df.columns.str.lower()
        list_date_cols = [col for col in df.columns if any(match in col.lower() for match in ['date', 'day'])]
        list_collection_cols = [col for col in df.columns if any(match in col.lower() for match in ['collection'])]
        df[list_date_cols] = df[list_date_cols].apply(pd.to_datetime, errors='coerce')
        df[list_collection_cols] = df[list_collection_cols].astype(str)
        return df

    def insert_subject(self, subject: Subject):
        subject.process_characteristics()
        subject.insert_characteristics()
        subject.process_measurements()
        subject.insert_measurements()

    def insert_cohort(self):
        '''
            Loop through all subjects of the cohort and insert them into the database
        '''
        
        for index, subject_row in self.df_final.iterrows():
            print(subject_row)
            subject = Subject(subject_row, self.type)
            self.insert_subject(subject)
            # subject.conn.close()

# COMMAND ----------

class rfid_df:
    def __init__(self,path):
        self.df = pd.DataFrame()
        self.path = path

    def add(self, path):
        temp = pd.read_excel(path)
        temp.columns = [i.lower() for i in temp.columns]
        temp = temp[['rat','rfid']]
        self.df = pd.concat([self.df,temp])

    def save(self):
        self.df = self.df.reset_index()[['rat','rfid']]
        self.df.columns = ['subject','rfid']
        self.df.to_csv(self.path)

RFID_OXY = rfid_df('')
RFID_COC = rfid_df('')

# COMMAND ----------

def main():
    for cocaine_cohort in COCAINE_COHORT_ALL:
        print(f'NAME OF THE COCAINE COHORT IS: {cocaine_cohort}')
        cohort = CohortProcess(cocaine_cohort, "cocaine")
        cohort.insert_cohort()
        RFID_COC.add(cocaine_cohort)
    for oxy_cohort in OXYCODONE_COHORT_ALL:
        print(f'NAME OF THE OXY COHORT IS: {oxy_cohort}')
        cohort = CohortProcess(oxy_cohort, "oxycodone")
        cohort.insert_cohort()
        RFID_OXY.add(oxy_cohort)
    RFID_OXY.save()
    RFID_COC.save()
main()
