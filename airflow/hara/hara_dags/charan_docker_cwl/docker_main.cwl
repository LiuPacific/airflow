id: ComprehensiveWorkflow
class: Workflow
hints:
  ResourceRequirement:
    ramMin: 2048
    coresMin: 2
label: Comprehensive Workflow with Extended GUI Information
steps:
  dataset_extraction_1:
    in:
      url: dataset_extraction_1_url
    out:
    - output_csv
    run: dataset_extraction.cwl
  dataset_information_0:
    in:
      input_csv: dataset_extraction_1/output_csv
    out: []
    run: dataset_information.cwl
inputs:
  dataset_extraction_1_url: string
outputs: []
cwlVersion: v1.2
"$namespaces":
  author: team
  website_name: workflow-engine
