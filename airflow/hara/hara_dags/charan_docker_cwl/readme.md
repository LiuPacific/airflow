cwltool docker_main.cwl inputs.yaml


docker build -t download-files .
docker build -t dataset-metadata .
