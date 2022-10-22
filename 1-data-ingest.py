# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # Ingest Synthea Records to Delta
# MAGIC In this notebook we ingest synthetic patient records, generated using [synthea](https://github.com/synthetichealth/synthea/wiki), into deltalake's bronze layer.
# MAGIC The raw data is in csv format.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window
import re
import json

# COMMAND ----------

project_name='omop-cdm-100K'

# COMMAND ----------

# DBTITLE 1,read configs
with open(f'/tmp/{project_name}_configs.json','r') as f:
    paths = json.load(f)
    data_path = paths['data_path']
    base_path = paths['base_path']
    delta_path = paths['delta_path']
print(f'raw data is stored in {data_path}')

# COMMAND ----------

# DBTITLE 1,display raw synthea data
synthea_path=data_path
display(dbutils.fs.ls(synthea_path))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest CSV files as spark dataframes and write to delta bronze layer
# MAGIC Next we ingest all these files into spark dataframes, and write the resulting tables to delta (bronze layer)

# COMMAND ----------

# DBTITLE 1,read raw data and write to delta
from tqdm import tqdm
datasets=[k.name.strip('/') for k in dbutils.fs.ls(synthea_path)]
delta_bronze_path = f'{base_path}/{project_name}/delta/bronze/'
for dataset in tqdm(datasets):
    print(f'\nreading {dataset}\n')
    df=spark.read.csv(f'{synthea_path}/{dataset}',header=True,inferSchema=True)
    print(f'writing {dataset} to delta')
    df.write.mode("overwrite").save(f'{delta_bronze_path}/{dataset}')
    df.createOrReplaceTempView(dataset)

# COMMAND ----------

# DBTITLE 1,count of records
import pandas as pd
table_counts=[(tab,spark.read.load(f'{delta_bronze_path}/{tab}').count()) for tab in datasets]
display(pd.DataFrame(table_counts,columns=['dataset','n_records']).sort_values(by=['n_records'],ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC Copyright / License info of the notebook. Copyright Databricks, Inc. [2021].  The source in this notebook is provided subject to the [Databricks License](https://databricks.com/db-license-source).  All included or referenced third party libraries are subject to the licenses set forth below.
# MAGIC 
# MAGIC |Library Name|Library License|Library License URL|Library Source URL| 
# MAGIC | :-: | :-:| :-: | :-:|
# MAGIC |Smolder |Apache-2.0 License| https://github.com/databrickslabs/smolder | https://github.com/databrickslabs/smolder/blob/master/LICENSE|
# MAGIC |Synthea|Apache License 2.0|https://github.com/synthetichealth/synthea/blob/master/LICENSE| https://github.com/synthetichealth/synthea|
# MAGIC | OHDSI/CommonDataModel| Apache License 2.0 | https://github.com/OHDSI/CommonDataModel/blob/master/LICENSE | https://github.com/OHDSI/CommonDataModel |
# MAGIC | OHDSI/ETL-Synthea| Apache License 2.0 | https://github.com/OHDSI/ETL-Synthea/blob/master/LICENSE | https://github.com/OHDSI/ETL-Synthea |
# MAGIC |OHDSI/OMOP-Queries|||https://github.com/OHDSI/OMOP-Queries|
# MAGIC |The Book of OHDSI | Creative Commons Zero v1.0 Universal license.|https://ohdsi.github.io/TheBookOfOhdsi/index.html#license|https://ohdsi.github.io/TheBookOfOhdsi/|
