# Databricks notebook source
# MAGIC %md 
# MAGIC You may find this series of notebooks at https://github.com/databricks-industry-solutions/omop-cdm. For more information about this solution accelerator, visit https://www.databricks.com/blog/2021/07/19/unlocking-the-power-of-health-data-with-a-modern-data-lakehouse.html.

# COMMAND ----------

# MAGIC %md
# MAGIC # Congestive Heart Failure cohort study
# MAGIC <img src="https://upload.wikimedia.org/wikipedia/commons/f/fb/Blausen_0463_HeartAttack.png" width=300>
# MAGIC 
# MAGIC In this example we create a cohort of patients that have recently been diagnosed with Congestive Heart Failure. In this hypothetical study, we would like to calculate the rates of ER admissions between different age groups and genders, for this cohort. This example is based on an example study from [The Book of OHDSI](https://ohdsi.github.io/TheBookOfOhdsi/SqlAndR.html#designing-a-simple-study). The main purpose of this exercise is to show users how to build cohorts with standard SQL directly on delta tables.
# MAGIC 
# MAGIC >Heart failure, sometimes known as congestive heart failure, occurs when your heart muscle doesn't pump blood as well as it should. Certain conditions, such as narrowed arteries in your heart (coronary artery disease) or high blood pressure, gradually leave your heart too weak or stiff to fill and pump efficiently.
# MAGIC >
# MAGIC >Not all conditions that lead to heart failure can be reversed, but treatments can improve the signs and symptoms of heart failure and help you live longer. Lifestyle changes — such as exercising, reducing sodium in your diet, managing stress and losing weight — can improve your quality of life.
# MAGIC >
# MAGIC >One way to prevent heart failure is to prevent and control conditions that cause heart failure, such as coronary artery disease, high blood pressure, diabetes or obesity.
# MAGIC >
# MAGIC [www.mayoclinic.org](https://www.mayoclinic.org/diseases-conditions/heart-failure/symptoms-causes/syc-20373142#:~:text=Heart%20failure%2C%20sometimes%20known%20as,to%20fill%20and%20pump%20efficiently.)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Target Cohort
# MAGIC First we define the [target cohort](https://ohdsi.github.io/TheBookOfOhdsi/Cohorts.html), which is determined based on the following criteria:
# MAGIC 
# MAGIC Patients who are newly:
# MAGIC - diagnosed with chronic congestive heart failure (CCHF)
# MAGIC - persons with a condition occurrence record of CCHF or any descendants, indexed at the first diagnosis (cohort entry date)
# MAGIC - who have at least three years (1095 days) of prior observation before their first diagnosis
# MAGIC - and have no [Furosemide](https://en.wikipedia.org/wiki/Furosemide) exposure any time prior to first CCHF diagnosis

# COMMAND ----------

# MAGIC %sql
# MAGIC USE OMOP531

# COMMAND ----------

library(SparkR)
library(glue)

target_condition_concept_id<-4229440  #Chronic congestive heart failure (disorder)
target_drug_concept_id<-1719286       #10 ML Furosemide 10 MG/ML Injection

target_cohort_query<-glue("
with targetConditionSet AS
(
  SELECT person_id, min(condition_start_date) as condition_start_date
  FROM condition_occurrence
  WHERE condition_concept_id IN
    ( SELECT descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN ({target_condition_concept_id}) )
  GROUP BY person_id
),
targetDrugExposure as (
SELECT person_id, min(drug_exposure_start_date) as drug_exposure_start_date
  FROM drug_exposure
  WHERE drug_concept_id IN (SELECT descendant_concept_id FROM concept_ancestor WHERE ancestor_concept_id IN ({target_drug_concept_id}))
  GROUP BY person_id
)

SELECT 1 AS cohort_definition_id,
targetConditionSet.person_id AS subject_id,
targetConditionSet.condition_start_date AS cohort_start_date,
observation_period.observation_period_end_date AS cohort_end_date
from targetConditionSet
INNER JOIN observation_period
  ON targetConditionSet.person_id = observation_period.person_id
  AND targetConditionSet.condition_start_date >= date_add(observation_period.observation_period_start_date,1095)
  AND targetConditionSet.condition_start_date <= observation_period.observation_period_end_date
  LEFT JOIN targetDrugExposure
  ON targetConditionSet.person_id = targetDrugExposure.person_id
  AND targetConditionSet.condition_start_date > targetDrugExposure.drug_exposure_start_date
WHERE targetDrugExposure.person_id IS NULL
  ;
"
)

target_cohort_df<-sql(as.character(target_cohort_query))
createOrReplaceTempView(target_cohort_df,'targetCohort')
display(target_cohort_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Outcome cohort
# MAGIC Now, to calculate the incidence rate, we define the outcome cohort, which in this case, is the cohort of all patients that have visited ER.

# COMMAND ----------

outcome_concept_id<-9203  #Emergency Room Visit
outcome_cohort_query=glue("
  SELECT 2 AS cohort_definition_id,
  visit_occurrence.person_id AS subject_id,
  visit_occurrence.visit_start_date AS cohort_start_date,
  visit_occurrence.visit_end_date AS cohort_end_date
  FROM  visit_occurrence
  WHERE visit_occurrence.visit_concept_id IN ({outcome_concept_id})
  GROUP BY visit_occurrence.person_id, visit_occurrence.visit_start_date, 
  visit_occurrence.visit_end_date")
outcome_cohort_df<-sql(as.character(outcome_cohort_query))
createOrReplaceTempView(outcome_cohort_df,'outcomeCohort')
display(outcome_cohort_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###  3. Incidence Rate Calculation
# MAGIC Now that our cohorts are in place, we can compute the incidence rate, stratified by age and gender:

# COMMAND ----------

createOrReplaceTempView(union(target_cohort_df,outcome_cohort_df),'CHF_cohort')

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW INCIDENT_RATE AS (
# MAGIC WITH tar AS (
# MAGIC   SELECT concept_name AS gender,
# MAGIC     FLOOR((YEAR(cohort_start_date) - year_of_birth) / 10)*10 AS age,
# MAGIC     subject_id,
# MAGIC     cohort_start_date,
# MAGIC     CASE WHEN DATE_ADD(cohort_start_date,7) > observation_period_end_date
# MAGIC     THEN observation_period_end_date
# MAGIC     ELSE DATE_ADD(cohort_start_date,7)
# MAGIC     END AS cohort_end_date
# MAGIC   FROM CHF_cohort
# MAGIC   INNER JOIN observation_period
# MAGIC     ON subject_id = observation_period.person_id
# MAGIC       AND observation_period_start_date < cohort_start_date
# MAGIC       AND observation_period_end_date > cohort_start_date
# MAGIC   INNER JOIN person
# MAGIC     ON subject_id = person.person_id
# MAGIC   INNER JOIN concept
# MAGIC     ON gender_concept_id = concept_id
# MAGIC   WHERE cohort_definition_id = 1 -- Target
# MAGIC )
# MAGIC SELECT days.gender,
# MAGIC     days.age,
# MAGIC     days,
# MAGIC     CASE WHEN events IS NULL THEN 0 ELSE events END AS events
# MAGIC FROM (
# MAGIC   SELECT gender,
# MAGIC     age,
# MAGIC     SUM(DATEDIFF(cohort_end_date,cohort_start_date)) AS days
# MAGIC   FROM tar
# MAGIC   GROUP BY gender,
# MAGIC     age
# MAGIC ) days
# MAGIC LEFT JOIN (
# MAGIC   SELECT gender,
# MAGIC       age,
# MAGIC       COUNT(*) AS events
# MAGIC   FROM tar
# MAGIC   INNER JOIN CHF_cohort chf
# MAGIC     ON tar.subject_id = chf.subject_id
# MAGIC       AND tar.cohort_start_date <= chf.cohort_start_date
# MAGIC       AND tar.cohort_end_date >= chf.cohort_start_date
# MAGIC   WHERE cohort_definition_id = 2 -- Outcome
# MAGIC   GROUP BY gender,
# MAGIC     age
# MAGIC ) events
# MAGIC ON days.gender = events.gender
# MAGIC   AND days.age = events.age
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC select *, 1000*events/days/7 as ir
# MAGIC from INCIDENT_RATE

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Cohorts to Results Schema
# MAGIC To ensure reproducibility and accessibility of results to other researchers, or regulators it is important to ensure that cohorts are stored and accessible. This is done by writing resulting cohorts, as well as cohort definition (information regarding how the cohort is created including the `sql` code used to create the cohort) into the Results Schema:

# COMMAND ----------

# DBTITLE 1,write to cohort_definition
# MAGIC %r
# MAGIC targetCohort_Description<-"
# MAGIC patients who are newly diagnosed with chronic congestive heart failure (CCHF)
# MAGIC persons with a condition occurrence record of CCHF or any descendants, indexed at the first diagnosis
# MAGIC who have >1095 days of prior observation before their first diagnosis
# MAGIC and have no Furosemide exposure any time prior to first CCHF diagnosis
# MAGIC target_condition_concept_id=4229440  #Chronic congestive heart failure (disorder)
# MAGIC target_drug_concept_id=1719286       #10 ML Furosemide 10 MG/ML Injection
# MAGIC "
# MAGIC 
# MAGIC sql(as.character(glue("
# MAGIC   INSERT INTO OMOP531.cohort_definition
# MAGIC   select
# MAGIC   1, 'CHF_cohort', '{targetCohort_Description}', 1, '{target_cohort_query}',1, current_date()
# MAGIC ")))

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO OMOP531.cohort
# MAGIC   Select cohort_definition_id, 
# MAGIC          subject_id, 
# MAGIC          cohort_start_date, 
# MAGIC          cohort_end_date
# MAGIC   from CHF_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cohort

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
