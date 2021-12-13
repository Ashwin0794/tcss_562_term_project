# tcss_562_term_project

1. Service 1: Lambda function which access the raw data from S3 and does the following transformations: adding columns and etc and stores the transformed file in S3.

2. Service 2a: Lambda function which access S3 for the transformed file and loads data intto Aurora serverless mysql database.
3. Service 2b: Lambda function which access S3 for the transformed file and loads data intto Aurora serverless posgres database.

5. Service 3a: Lambda function which accesses Aroura serveless mysql database to query sales data.
6. Service 3b: Lambda function which accesses Aroura serveless posgres database to query sales data.

For service 1, we used Pandas framework to tranform data instead of downloading the data from S3 to disk because Amazon provides only 500 mb of /tmp disk space for lambda functions. So used pandas framework utilizing the memory provided for Lambda which is 10 GB.

For services 2a and 2b created VPC endpoint to access S3 from the lambda function whihc hosted in the Aurora VPC.
