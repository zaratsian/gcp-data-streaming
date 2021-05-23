FROM apache/beam_python3.7_sdk:2.25.0

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt

COPY . .

ENTRYPOINT [ "python3", "main.py", "--gcp_project", "dz-apps", "--region", "us-central1", "--job_name", "data-stream", "--gcp_staging_location", "gs://dz-apps-dataflow/staging", "--gcp_tmp_location", "gs://dz-apps-dataflow/tmp", "--batch_size", "10", "--pubsub_topic", "projects/dz-apps/topics/data-stream", "--runner", "DirectRunner" ]


