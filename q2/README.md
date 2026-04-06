Public IP: 44.245.176.184

IAM Role ARN: arn:aws:iam::585369386434:role/ee547hw5

Deployment method: Docker

Issues Encountered and Solutions
1.Port 8080 Already in Use
Error: port is already allocated
Fix:
docker stop arxiv-api
docker rm arxiv-api

2.AWS Credentials Not Found 
Error: NoCredentialsError
Cause: Docker container did not receive AWS credentials
Fix: Pass credentials using environment variables or use IAM role in EC2