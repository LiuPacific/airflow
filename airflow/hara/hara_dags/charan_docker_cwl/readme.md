cwltool docker_main.cwl inputs.yaml


docker build -t download-files .
docker build -t dataset-metadata .




ARM64


docker buildx create --use
docker buildx inspect --bootstrap

docker buildx build --platform linux/arm64 -t docker_extraction_arm:latest --load .
docker tag  docker_extraction_arm typingliu/docker_extraction_arm:v1
docker push typingliu/docker_extraction_arm:v1

docker buildx build --platform linux/arm64 -t docker_metadata_arm:latest --load .
docker tag  docker_extraction_arm typingliu/docker_metadata_arm:v1
docker push typingliu/docker_metadata_arm:v1


