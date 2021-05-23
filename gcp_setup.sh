GCP_PROJECT_ID='dz-apps'

# Create GCP PubSub Topics
gcloud pubsub topics create data-stream

# Create GCP PubSub Subscriptions
gcloud pubsub subscriptions create data-stream-sub --topic data-stream

# Dataflow Setup
gsutil mkdir gs://$GCP_PROJECT_ID-dataflow

