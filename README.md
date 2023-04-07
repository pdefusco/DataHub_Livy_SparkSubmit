# Submitting a Spark Job to Data Hub via Livy

You can submit a Spark Job to DataHub from a remote machine. In this example we will use CML to trigger a Spark Job in DataHub.

### Requirements

To reproduce this project you will need:
* A CML Workspace in CDP Public Cloud (AWS and Azure ok)
* A CDP DataHub in the CDP Public Cloud Environment with Spark. This example is based on Spark 2 but it can be easily adapted to run with Spark 3.
* Basic familiarity with Spark and Python.

### Project Setup

Log into your CML Workspace and create a CML Project as a clone to this git Repository. Select a Runtime with Python 3.7 or above.

All files and dependencies will be automatically loaded into the CML Project Files.

### Steps to Reproduce

Launch a CML Session with Workbench Editor. A basic CML Resource Profile with minimum CPU/Mem will suffice.

```
Kernel: python
Version: any version available
Runtime Editor: Workbench
Runtime Edition: Standard
Enable Spark: Select any Add-On with Spark 2.4.x
Resource Profile: 1 vCPU / 2 GiB - No GPU required.
```

#### Create a directory in Cloud Storage

Open the CML Terminal and run the following command:

```hdfs dfs -mkdir s3a://<STORAGE_LOCATION>/datahub_livy_sparksubmit```

If you don't know your STORAGE_LOCATION parameter you can obtain it from the  



## Conclusion & Next Steps
