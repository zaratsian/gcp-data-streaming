# Google Cloud - Data Streaming

**Dependenies:**

* Docker must be installed.

**Initial GCP Setup:**
```
cd ./gcp-data-streaming/

# NOTE: Be sure to update the gcp_setup.sh file to set the GCP_PROJECT_ID to your GCP project id.

./gcp_setup.sh
```

**To start the simulator:**
```
cd ./gcp-data-streaming/simulator/

./build.sh

./run.sh
```

**To start real-time Dataflow processing:**
```
cd ./gcp-data-streaming/dataflow/

./build.sh

./run.sh
```

