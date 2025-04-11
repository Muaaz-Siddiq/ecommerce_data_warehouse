# E-commerce DataWarehouse Project


## 📌 Objective

An e-commerce company aims to develop a **one-time data warehouse pipeline** to store all its data **incrementally from 2016 to 2018**, using a **single static CSV source**.


## 🚀 Features And Design Patterns

- Incremental Loading
- Star Schema
- Slowly Changing Dimension (SCD) Type 1 (UPSERT)
- One-time cleaning resource cost
- Quick transformation and loading in final stage



## 🛠️ Tech Stack

- Python
- Pyspark
- Apache Spark
- SQL
- Spark Sql
- Databricks


## 📦 Installation
- #### FOR LOCAL MACHINE:
1. git clone https://github.com/Muaaz-Siddiq/ecommerce_data_warehouse.git
2. Install dependencies
3. Install Apache Spark to use all the features of spark. For Reference on installation:https://youtu.be/8zWtEZ7ME08?si=KaJxgsisxnHOgPJ-

- #### FOR DataBricks:
1. Upload the Scripts to the databricks
2. Upload source file.


## Install dependencies
Dependencies can be installed through requirments.txt


## 🧪 Usage
1. Run db_and_tables.py
2. Run initial_ETL_and_cleaning.py
3. Run final_ETL.py (orchestration tools like airflow or cronjob can be used to schedule the incremental loading.)

NOTE: Project was developed on databricks, so you might required some extra setup to run it on local machine.


## 📘 Documentation
- High Level Documentation is present in the repo. under high_level_documentation directory


## 🧾 Dataset

- Source: Static CSV file
- Size: 15MB
- Records: 1+ million
- Format: CSV
- Nature: Messy, unchanging data (from 2016 to 2018)
- Data Set: dataset can be viewed and downloaded through https://www.kaggle.com/datasets/zusmani/pakistans-largest-ecommerce-dataset
