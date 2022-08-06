# Databricks notebook source
# MAGIC %md This notebook sets up the companion cluster(s) to run the solution accelerator. It also creates the Workflow to create a Workflow DAG and illustrate the order of execution. Feel free to interactively run notebooks with the cluster or to run the Workflow to see how this solution accelerator executes. Happy exploring!
# MAGIC 
# MAGIC The pipelines, workflows and clusters created in this script are user-specific, so you can alter the workflow and cluster via UI without affecting other users. Running this script again after modification resets them.
# MAGIC 
# MAGIC **Note**: If the job execution fails, please confirm that you have set up other environment dependencies as specified in the accelerator notebooks. Accelerators sometimes require the user to set up additional cloud infra or data access, for instance. 

# COMMAND ----------

# DBTITLE 0,Install util packages
# MAGIC %pip install git+https://github.com/databricks-academy/dbacademy-rest git+https://github.com/databricks-academy/dbacademy-gems git+https://github.com/databricks-industry-solutions/notebook-solution-companion

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

job_json = {
        "timeout_seconds": 7200,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "HLS"
        },
        "tasks": [
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"0-README"
                },
                "task_key": "OMOP_01",
                "description": ""
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"00-setup",
                    "base_parameters": {"project":"omop-cdm-100K"}
                },
                "task_key": "OMOP_02",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_01"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"1-data-ingest",
                    "base_parameters": {"project":"omop-cdm-100K"}
                },
                "task_key": "OMOP_03",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_02"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"2-omop531-cdm-setup"
                },
                "task_key": "OMOP_04",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_03"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"3-omop-vocab-setup"
                },
                "task_key": "OMOP_05",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_04"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"4-omop531-etl-synthea"
                },
                "task_key": "OMOP_06",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_05"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"5-CHF-cohort-building"
                },
                "task_key": "OMOP_07",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_06"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"6-drug-analysis"
                },
                "task_key": "OMOP_08",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_07"
                    }
                ]
            },
            {
                "job_cluster_key": "OMOP_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"7-sample-omop-queries"
                },
                "task_key": "OMOP_09",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "OMOP_08"
                    }
                ]
            }
        ],
        "job_clusters": [
            {
                "job_cluster_key": "OMOP_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "num_workers": 8,
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_D3_v2", "GCP": "n1-highmem-4"},
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
NotebookSolutionCompanion().deploy_compute(job_json, run_job=run_job)

# COMMAND ----------



# COMMAND ----------


