cwlVersion: v1.2
class: CommandLineTool
hints:
  NetworkAccess:
    networkAccess: true
  DockerRequirement:
    dockerPull: jslpdcharan/docker_geojson_extraction_amd:latest
inputs:
  url:
    type: string
    inputBinding:
      position: 1
outputs:
  output_csv:
    type: File
    outputBinding:
      glob: geometry_shp_file.geojson

# cwltool GeoFileReader.cwl inputs.yaml
