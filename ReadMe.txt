Template compile command:

mvn compile exec:java \
     -Dexec.mainClass=max.dataflow.csv.batch.CsvBatchPipelineRun \
     -Dexec.args="--runner=DataflowRunner \
                  --project=max-data-flow-scv-1 \
                  --region=europe-west1 \
                  --stagingLocation=gs://storage-max-data-flow-scv-1/batch/staging \
                  --templateLocation=gs://storage-max-data-flow-scv-1/batch/template \
                  --gcpTempLocation=gs://storage-max-data-flow-scv-1/batch/tmp \
                  --inputType1Folder=gs://storage-max-data-flow-scv-1/batch/input/csv-type-1 \
                  --inputType2Folder=gs://storage-max-data-flow-scv-1/batch/input/csv-type-2 \
                  --outputFolder=gs://storage-max-data-flow-scv-1/batch/output \
                  --errorsOutputFolder=gs://storage-max-data-flow-scv-1/batch/errors" \
     -P dataflow-runner

mvn compile exec:java -Dexec.mainClass=max.dataflow.csv.batch.CsvBatchPipelineRun -Dexec.args="--runner=DataflowRunner --project=max-data-flow-scv-1 --region=europe-west1 --stagingLocation=gs://storage-max-data-flow-scv-1/batch/staging --templateLocation=gs://storage-max-data-flow-scv-1/batch/template --gcpTempLocation=gs://storage-max-data-flow-scv-1/batch/tmp --inputType1Folder=gs://storage-max-data-flow-scv-1/batch/input/csv-type-1 --inputType2Folder=gs://storage-max-data-flow-scv-1/batch/input/csv-type-2 --outputFolder=gs://storage-max-data-flow-scv-1/batch/output --errorsOutputFolder=gs://storage-max-data-flow-scv-1/batch/errors" -P dataflow-runner