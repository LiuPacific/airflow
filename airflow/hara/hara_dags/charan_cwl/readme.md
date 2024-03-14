docker build -t charan-download-files .
docker run charan-download-files:latest https://data.cdc.gov/resource/yrur-wghw.csv
cwltool dataset_extraction.cwl inputs.yaml




