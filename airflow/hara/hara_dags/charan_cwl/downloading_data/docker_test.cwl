id: ComprehensiveWorkflow
class: Workflow
hints:
  ResourceRequirement:
    ramMin: 2048
    coresMin: 2
label: Comprehensive Workflow with Extended GUI Information
steps:
  CsvReader_0:
    in:
      url: CsvReader_0_url
    out:
    - output_csv
    run: CsvReader.cwl
inputs:
  CsvReader_0_url: string

outputs: []
cwlVersion: v1.2
"$namespaces":
  author: team
  website_name: workflow-engine