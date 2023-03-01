# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # OMOP 5.31 with databricks
# MAGIC In this solution accelerator, we will build a Common Data Model for observational research based on OMOP 5.31 CDM, on databricks lakehouse platform.
# MAGIC <div >
# MAGIC   <img src="https://drive.google.com/uc?export=view&id=1cTlUMLNyuCZFc3H04n9DxB3NxfY86Wkw", width=1500>
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ### Solution Overview
# MAGIC 
# MAGIC Observational databases are designed to primarily empower longitudinal studies and differ in both purpose and design from Electronic Medical Records (aimed at supporting clinical practice at the point of care), or claims data that are built for the insurance reimbursement processes. The Common Data Model is designed to address this problem:
# MAGIC 
# MAGIC >The Common Data Model (CDM) can accommodate both administrative claims and EHR, allowing users to generate evidence from a wide variety of sources. It would also support collaborative research across data sources both within and outside the United States, in addition to being manageable for data owners and useful for data users. 
# MAGIC 
# MAGIC One of the most widely used CDMs for observational research is The OMOP Common Data Model.
# MAGIC 
# MAGIC > The OMOP Common Data Model allows for the systematic analysis of disparate observational databases. The concept behind this approach is to transform data contained within those databases into a common format (data model) as well as a common representation (terminologies, vocabularies, coding schemes), and then perform systematic analyses using a library of standard analytic routines that have been written based on the common format. 
# MAGIC 
# MAGIC for more information visit https://www.ohdsi.org/data-standardization/the-common-data-model/
# MAGIC 
# MAGIC OMOP CDM has now been adopted by more than 30 countries with more than 600M unique patient records.
# MAGIC 
# MAGIC <img src="https://ohdsi.github.io/TheBookOfOhdsi/images/OhdsiCommunity/mapOfCollaborators.png" width = 400>
# MAGIC 
# MAGIC 
# MAGIC ### _Why lakehouse_?
# MAGIC The complexity of such healthcare data, data variety, volume and varied data quality, pose computational and organizational challenges. For example, performing cohort studies on longitudinal data requires complex ETL of the raw data to transform the data into a data model that is optimized for such studies and then to perform statistical analysis on the resulting cohorts. Each step of this process currently requires using different technologies for ETL, storage, governance and analysis of the data. Each additional technology adds a new layer of complexity to the process, which reduces efficiency and inhibits collaboration.
# MAGIC 
# MAGIC The lakehouse paradigm, addresses such complexities: It is an architecture that by design brings the best of the two worlds of data warehousing and data lakes together: Data lakes meet the top requirement for healthcare and life sciences organizations to store and manage different data modalities and allow advanced analytics methods to be performed directly on the data where it sits in the cloud. However, datalakes lack the ability for regulatory-grade audits and data versioning. In contrast, data warehouses, which allow for quick access to data and support simple prescriptive analytics (such as simple aggregate statistics on the data), do not support unstructured data nor perform advanced analytics and ML. The lakehouse architecture allows organizations to perform descriptive and predictive analytics on the vast amounts of healthcare data directly on cloud storage where data sits while ensuring regulatory-grade compliance and security.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Notebooks
# MAGIC <div >
# MAGIC   <img src="https://drive.google.com/uc?export=view&id=1uqzzhMl148otHL399FRO1G8Sa9l_bohP", width=500>
# MAGIC </div>
# MAGIC 
# MAGIC In this solution accelerator, we provide a few examples to show how the lakehouse paradigm can be leveraged to support OMOP CDM.
# MAGIC 
# MAGIC   1. `README`: This notebook
# MAGIC   2. `RUNME`: Automating the end-to-end workflow 
# MAGIC   3. `0-config`: configuration and setup
# MAGIC   3. `1-data-ingest`: To ingest synthetic records in csv format from cloud to delta bronze layer and create a synthea database
# MAGIC   4. `2_omop531_cdm_setup`: Definitions of OMOP 5.3.1 common data model for delta (DDL)
# MAGIC   5. `3-omop_vocab_setup`: create OMOP [vocabulary tables](https://www.ohdsi.org/analytic-tools/athena-standardized-vocabularies/)
# MAGIC   6. `4_omop531_etl_synthea`: Example ETL for transforming synthea resources into OMOP 5.3.1
# MAGIC   7. `5-analysis/CHF-cohort-building`: Example notebook to create a cohort of patients with Chronic Heart Failure and look at emergency room visit trends by gender and age
# MAGIC   8. `5-analysis/drug-analysis`: Examples for analysis of drug prescription trends
# MAGIC   9. `5-analysis/sample-omop_queries`: Sample SQL queries from OHDSI

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataset
# MAGIC To simulate health records, we used [Synthea](https://github.com/synthetichealth/synthea) to generate `~90K` synthetic patients, from across the US. You can also access sample raw `csv` files in `/databricks-datasets/ehr/rwe/csv` for 10K patients.
# MAGIC By running `../1-data-ingest` you create the bronze tables based on the pre-simulated synthetic records.

# COMMAND ----------

# MAGIC %md
# MAGIC # LICENSE 

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
