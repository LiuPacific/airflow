cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    # dockerPull: jslpdcharan/dataset-metadata:latest
    dockerPull: typingliu/dataset-metadata:latest
  NetworkAccess:
    networkAccess: true

inputs:
  input_csv:
    type: File
    inputBinding:
      position: 1

outputs: []
