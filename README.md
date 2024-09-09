# sarek_plus_germline_dry
sarek_plus_germline_dry


```
Curl

curl -X 'PUT' \
  'http://localhost:8000/pipelines/' \
  -H 'accept: application/json' \
  -H 'Authorization: Bearer  $token'
  -H 'Content-Type: application/json' \
  -d '{
  "name": "ananda_plus_bag",
  "analysis_type": "germline",

      "data": {
        "repo": "bag-cnag/sarek_plus_germline_dry",
        "step": "mapping",
        "steps": [
          {
            "step": "File availability and integrity",
            "step_type": "process",
            "description": ""
          },
          {
            "step": "Sequencing data quality check",
            "step_type": "qc",
            "description": ""
          },
          {
            "step": "Mapping to the reference genome",
            "step_type": "process",
            "description": ""
          },
          {
            "step": "Mapping quality check",
            "step_type": "qc",
            "description": ""
          },
          {
            "step": "Variant calling (SNV, CNV/SV) + Biomarkers/Pharmacogenomics",
            "step_type": "process",
            "description": ""
          },
          {
            "step": "Variant calling quality check",
            "step_type": "qc",
            "description": ""
          },
          {
            "step": "Annotations",
            "step_type": "process",
            "description": ""
          },
          {
            "step": "Annotations Quality Check",
            "step_type": "qc",
            "description": ""
          },
          {
            "step": "Upload to Elastic",
            "step_type": "process",
            "description": ""
          },
          {
            "step": "Upload to Elastic Quality Check",
            "step_type": "qc",
            "description": ""
          },
          {
            "step": "Pipeline completion check",
            "step_type": "process",
            "description": ""
          }
        ],
        "tools": "cnvkit,manta,haplotypecaller,strelka,expansionhunter,stripy,fullmetrics",
        "pipelines": "sarek,pcgx,annotatesvs,gatk_mt,qualitycontrols"
      }
    
}'
```
