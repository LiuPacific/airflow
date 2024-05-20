cwlVersion: v1.2
class: CommandLineTool

# baseCommand: ["sh", "-c"]
# baseCommand:

# arguments:
#   - "echo 'nameserver 8.8.8.8' > /etc/resolv.conf && python3 /app/ExtractData.py"

hints:
  DockerRequirement:
    dockerPull: charan-download-files:latest
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
