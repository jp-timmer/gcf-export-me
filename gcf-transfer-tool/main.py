#!/usr/bin/env python
import os
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import storage_transfer

load_dotenv() # Load environment variables from .env file

# Reused variables should go here
project_id = os.getenv('PROJECT_ID')
dest_bucket_name = os.getenv('DEST_BUCKET_NAME')

if not project_id or not dest_bucket_name:
  raise ValueError("One or more required environment variables are missing.")
  
# [ START create_bucket_if_not_exists ]
def create_bucket_if_not_exists():
  storage_client = storage.Client()

  buckets = storage_client.list_buckets(
     project=project_id,
  )
  
  for bucket in buckets:
    if bucket.name == dest_bucket_name:
      print(f"Bucket {dest_bucket_name} already exists, proceeding with transfer.")
      break
  else: 
    print(f"Bucket {dest_bucket_name} does not exist, creating new bucket")
    bucket = storage_client.bucket(bucket_name=dest_bucket_name)
    bucket.storage_class = "COLDLINE"
    new_bucket = storage_client.create_bucket(bucket, project=project_id, location="europe-west1")

    print(
      f"Bucket {new_bucket.name} created in {new_bucket.location} with storage class {new_bucket.storage_class}"
    )
    return new_bucket
# [ END create_bucket_if_not_exists ]

# [ START transfer_to_archive_storage ]
def transfer_to_archive_storage():
  create_bucket_if_not_exists()
  client = storage_transfer.StorageTransferServiceClient()

  transfer_job_name = os.getenv('TRANSFER_JOB_NAME')
  bucket_name = os.getenv('BUCKET_NAME')
  if not transfer_job_name or not bucket_name:
    raise ValueError("One or more required environment variables are missing.")

  # Check if the transfer job already exists
  filter_string = f'{{"projectId":"{project_id}", "jobNames":["transferJobs/{transfer_job_name}"]}}'

  list_transfer_jobs_request = storage_transfer.ListTransferJobsRequest(
          filter=filter_string,
  )
  existing_jobs = client.list_transfer_jobs(request=list_transfer_jobs_request)

  response = None

  for response in existing_jobs:
    response = response
    print(response)
  if response is not None:
    if f"transferJobs/{transfer_job_name}" in response.name:
      print("Existing transfer job found, starting transfer job.")
  else:
    print("No existing transfer job found, creating new transfer job.")
    create_transfer_job_request = storage_transfer.CreateTransferJobRequest( 
      {
        "transfer_job": {
          "name": f"transferJobs/{transfer_job_name}",
          "project_id": f"{project_id}",
          "status": storage_transfer.TransferJob.Status.ENABLED,
          "transfer_spec": {
            "gcs_data_source": {
              "bucket_name": f"{bucket_name}",
            },
            "gcs_data_sink": {
              "bucket_name": f"{dest_bucket_name}",
            },
          },
        }
      }
    )

    create_response = client.create_transfer_job(request=create_transfer_job_request)
    print(f"Created transfer job: {create_response.name}")

  # Start the transfer job
  run_transfer_job_request = storage_transfer.RunTransferJobRequest(
    {
      "job_name": f"transferJobs/{transfer_job_name}",
      "project_id": f"{project_id}",
    }
  )

  operation = client.run_transfer_job(request=run_transfer_job_request)
  
  print("Transfer job started...")
  # Handle the response
  response = operation.result()
  print("Transfer job complete.", response)

# [ END transfer_to_archive_storage ]
transfer_to_archive_storage()