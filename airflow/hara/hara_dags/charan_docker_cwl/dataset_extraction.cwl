cwlVersion: v1.2
class: CommandLineTool

hints:
  DockerRequirement:
    # dockerPull: jslpdcharan/download-files:latest
    dockerPull: download-files:latest
  NetworkAccess:
    networkAccess: true

inputs:
  url:
    type: string
    inputBinding:
      position: 1

outputs:
  output_csv:
    type: File
    outputBinding:
      glob: "*.csv"
