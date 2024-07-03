## Initial Set-Up

1. Copied file structure from de-terraform-tasks
2. Altered test-and-deploy to trigger only when push happens on the main branch
3. On GitHub, under Actions, selected test-and-deploy file
4. NOTE We need to set up secrets on GitHub/settings/secrets-and-variables

1. Create two S3 buckets, one for the data and one for the code.
2. Create a deployment package for the code and copy it to the code bucket.
3. Create an IAM policy that allows read access to S3 - this will need to allow the Lambda to read the code from the code bucket as well as read the data from the data bucket.
4. Create an IAM policy that allows write access to Cloudwatch Logs and also allows Lambda to create its own log files.
5. Create an IAM role for Lambda with the two policies attached.
6. Create the function, using the code in the code bucket and the IAM role.
7. Create a resource based permission to allow the S3 bucket to invoke the Lambda function
8. Finally, add the event notification to the S3 data bucket to invoke the function when an object is put in the bucket.


Date before ordering              Date after sorting

2022-11-03, 14:20:51:563000     2022-11-03, 14:20:51:563000
2022-11-03, 14:20:49:962000     2022-11-03, 14:20:49:962000
2022-11-03, 14:20:49:962000     2022-11-03, 14:20:49:962000
2022-11-03, 14:20:49:962000     2022-11-03, 14:20:49:962000
2023-08-03, 17:34:09:883000     2023-08-03, 17:34:09:883000
2023-08-07, 15:16:09:813000     2023-08-07, 18:50:09:650000
2022-11-03, 14:20:49:962000     2022-11-03, 14:20:49:962000
2023-08-07, 14:40:10:155000     2023-08-07, 17:51:10:130000
2023-08-07, 15:16:09:813000     2023-08-07, 18:50:09:650000
2022-11-03, 14:20:51:563000     2022-11-03, 14:20:51:563000
2023-08-07, 15:16:09:813000     2023-08-07, 18:50:09:650000
