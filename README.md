# Description

This project analyses trip data from [New York's Taxi and Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). 

The primary component is a batch processing pipeline that takes the provided each month and creates an analytics-ready dataset in Google Cloud Platform. Example dashboards are shown below but creating the visualisation layer is not part of this repository and is instead left to the user.

# Architecture

The technologies used in the project are listed below. This pipeline could be simpler in a real life scenario, in particular the includsion of a spark cluster could be avoided, but it is included here for learning purposes.

The project has been created with DevOps principles in mind, requiring minimal config. The entire pipeline is dockerised and runs off the evironment variables in the .env file.

<p align="center">
  <img src="https://github.com/cjbraley/nyc_taxi_pipeline/raw/master/demo/architecture.png?raw=true" />
</p>


#### Infrastructure

The project uses GCP as cloud provider. Cloud infrastructure is managed as code using Terraform.

-   Docker
-   Google Cloud Platform:
-   Terraform:

#### Pipeline

The pipeline runs on a monthly basis and is orchestrated using Apache Airflow.

-   Docker
-   Airflow
-   Google Cloud Storage
-   Dataproc
-   Spark
-   BigQuery
-   DBT

The main function of Spark in this project is to align conflicting data types in the source files so that they can be read in a single external table by BigQuery, all other transforms could be handled by DBT. With data of this scale, creating a spark cluster for this purpose is unnecessary complex - it is included mainly for learning purposes.

1. The data is ingested to Google Cloud Storage
2. A Dataproc cluster started
3. PySpark runs on the cluster which cleans the incoming data
4. The cleaned data is stored in GCS
5. The Dataproc cluster shut down
6. Cleaned data is read by BigQuery
7. DBT runs on BigQuery and creates final tables used for analytics

#### Visualisation & Analysis

The visualisation in Looker Studio is indicative and not part of this repository.

-   Looker Studo

# Dashboard

To give an indication of the type of analysis that can be done with the data, a sample dashboard was created in Looker Studio. Looker Studio was used primarily for convenience, due to the project exisint in GCP, any other BI tool with a BiqQuery connecter could be used as a substitute.

The example dashboard consideres the following questions:

**1. To what degree was the taxi industry affected by the Covid 2020 pandemic?**

-   The trough of taxi trips occured in April 2020, shortly after the implementation of the first lockdowns
-   At this time, trips and fares were down ~94% on a yoy basis
-   Demand had still not recovered to pre-pandemic levels by the end of 2021

<p align="center">
  <img src="https://github.com/cjbraley/nyc_taxi_pipeline/raw/master/demo/dashboard1.png?raw=true" />
</p>

**2. How does the fare composition change based on the route of a trip?**

-   The vast majority of trips occur within Manhatten
-   The highest average fare occurs for trips from Queens to EWR (incl. Newark Airport)
-   The highest avg tip comes from routes to and from EWR. This seems to be related to a high proportion of airport trips (the difference in tips is not explained by trip distance).

<p align="center">
  <img src="https://github.com/cjbraley/nyc_taxi_pipeline/raw/master/demo/dashboard2.png?raw=true" />
</p>

## Setup

Requirements:

1. Docker
2. GCP account (important: the services used as part of the project will incur charges)

Instructions

1. Clone this repository
2. Create a new project in GCP
3. Enable the Cloud Dataproc API
   * Go to Dataproc from the menu
4. Create a service account
   * Go to IAM & Admin > Service Accounts
   * Choose "Create Service Account"
5. Give the service account necessary permissions
   * Do this as the account is created or go to IAM & Admin > Service Accounts
   * Grant the following permissions:
      * Service Account User
      * Storage Admin
      * Storage Object Admin
      * BigQuery Admin
      * Dataproc Administrator
6. Generate key file
   * Go to IAM & Admin > Service Accounts
   * Click on your service account
   * Go to the keys tab
   * Choose "ADD KEY" > "Create new key"
   * Choose the JSON option
7. Copy the key file to desired directory
   * Put the generated key file into a suitable directory
   * Project default is in the project root under .credentials/google/key.json
8. Set environment variables
   * Rename .env-example to .env
   * Update values to match your setup
9. Build infrastructure with Terraform
   * run "make infra-init"
   * run "make infra-run command=apply"
1. Init & run airflow
   * run "make init"
   * run "make up"
   * default port is 8080
1. Enable DAGs
   * Ingest > Transform > BQ Tables > DBT
   * When backfilling, set the CREATE_DESTROY_INFRA variable to false to avoid creating and destroying the cluster on every run
1. Optional: Connect to BigQuery and create visualisation in Looker Studio or similar