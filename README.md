# Data Engineering Project Documentation

## Objective

The Data Engineering Project is designed to showcase the skills and knowledge acquired over the past few weeks of our Northcoders Data Engineering Bootcamp. The project involves creating an application that Extracts, Transforms, and Loads (ETL) data from a prepared source into a data lake/warehouse hosted on AWS. The solution should be reliable, resilient, and preferably deployed and managed using code.

By the end of the project, the following goals will be achieved:

- Creation of an application that interacts with AWS and postgres database server.
- Remodelling of data into a data warehouse hosted on AWS.
- Demonstration of a well-monitored project with performance measurement capabilities.
- Deployment of at least some aspects of the project using scripting or automation.

The project should demonstrate knowledge in Python, SQL, database modelling, AWS, good operational practices, and Agile working.

## The Minimum Viable Product (MVP)

The main objective of the project is to create a data platform that extracts data from an operational database and archives it in a data lake. The data will then be remodelled into a predefined schema suitable for a data warehouse (OLAP). The following elements should be included in the Minimum Viable Product (MVP):

1. Two S3 buckets: One for ingested data and one for processed data, both structured and well-organized for easy access.
2. A Python application for continuous ingestion of all tables from the `totesys` database into the "ingestion" S3 bucket. The application must:
   - Operate automatically on a schedule.
   - Log progress to Cloudwatch.
   - Trigger email alerts in case of failures.
   - Follow good security practices (e.g., prevent SQL injection, maintain password security).
3. A Python application to remodel at least some of the data into a predefined schema suitable for the data warehouse. The data should be stored in `parquet` format in the "processed" S3 bucket. The application must:
   - Trigger automatically upon completion of an ingested data job.
   - Be adequately logged and monitored.
   - Populate the dimension and fact tables of a single "star" schema in the data warehouse.
4. A Python application to load data into the prepared data warehouse at defined intervals. The application should be adequately logged and monitored.
5. A Quicksight dashboard that allows users to view useful data in the data warehouse.

All Python code should be thoroughly tested, PEP8 compliant, and tested for security vulnerabilities with the `safety` and `bandit` packages. Test coverage should exceed 90%.

As much as possible, the project should be deployed automatically using infrastructure-as-code and CI/CD techniques, which can be implemented using `bash` scripts, Python code, or Terraform.

A change to the source database should reflect in the data warehouse within 30 minutes.

## The Data

The primary data source for the project is the `totesys` database, which simulates the back-end data of a commercial application. The ERD for the database is provided (<https://dbdiagram.io/d/6332fecf7b3d2034ffcaaa92>).

The data from `totesys` will need to be remodelled into three overlapping star schemas for this warehouse. The ERDs for these star schemas are available:

- ["Sales" schema](https://dbdiagram.io/d/637a423fc9abfc611173f637)
- ["Purchases" schema](https://dbdiagram.io/d/637b3e8bc9abfc61117419ee)
- ["Payments" schema](https://dbdiagram.io/d/637b41a5c9abfc6111741ae8)

The overall structure of the resulting data warehouse is shown (<https://dbdiagram.io/d/63a19c5399cb1f3b55a27eca>).

Tables to be ingested from `totesys` include:

- counterparty
- currency
- department
- design
- staff
- sales_order
- address
- payment
- purchase_order
- payment_type
- transaction

For the MVP, only the following tables need to be populated in the data warehouse:

- fact_sales_order
- dim_staff
- dim_location
- dim_design
- dim_date
- dim_currency
- dim_counterparty

The structure of the "processed" S3 data should reflect these tables, with possible changes in data types to conform to the warehouse data model.

## The Dashboard

To demonstrate the use of the data warehouse, we will create a [AWS Quicksight](https://aws.amazon.com/quicksight/) dashboard. The specific details of constructing the Quicksight dashboard will be guided by Northcoders tutors. We will supply the SQL queries used to retrieve the data for display.

## Possible Extensions

<!-- If time permits, the MVP can be enhanced. The focus for any enhancement should ensure that all tables in the data warehouse are being updated. Additional features could include implementing a _schema registry_ or _data catalogue_ to check the incoming data's structure and handle anomalies accordingly.

Extensions could involve:

1. Ingesting data from a file source, such as another S3 bucket with JSON files.
2. Ingesting data from an external API, for example, retrieving daily foreign exchange rates from `https://freeforexapi.com/Home/Api` using the `requests` library. -->

## Technical Details

The project's infrastructure should be hosted in a single AWS account. We will use one of the Northcoders accounts and provide team members with credentials to access it. Infrastructure scripts should be created to rebuild resources efficiently.

Required components for the project include:

1. A job scheduler using AWS EventBridge to run the ingestion job frequently (data should be visible in the warehouse within 30 minutes).
2. An "ingestion" S3 bucket acting as a landing zone for ingested data.
3. A Python application (preferably AWS Lambda) to check for changes in the database tables and ingest new or updated data into the "ingestion" S3 bucket. Status and error messages should be logged to Cloudwatch.
4. A Cloudwatch alert to trigger an email in case of major errors.
5. A "processed" S3 bucket for storing transformed data.
6. A Python application to transform data from the "ingestion" S3 bucket and place it in the "processed" S3 bucket. The data should be transformed to conform to the warehouse schema.
7. A Python application to periodically update the data warehouse from the data in the "processed" S3 bucket. Status and errors should be logged to Cloudwatch.
8. In the final week of the course, SQL queries will be required to perform complex queries on the data warehouse.

## Team Members

Harry Hainsworth-Staples,
Valentine Gakunga,
Harley Cole,
David Luke,
Holly Salthouse,
Amreet Singh Notay

## Conclusion

The Data Engineering Project provided a realistic simulation of a typical data engineering project. While completing a fully-functioning, "production-ready" solution within the given timeframe may be challenging, the focus was on tackling typical real-world problems and applying Python, data, and DevOps skills effectively. The emphasis was on delivering a high-quality Minimum Viable Product (MVP) rather than a complex but poorly-engineered platform.
