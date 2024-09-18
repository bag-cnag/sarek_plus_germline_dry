import requests
#import jwt
import os

from keycloak import KeycloakOpenID
import pytest
def pytest_namespace():
    return {'plan_id': 0 ,"dar_id": None ,  "analysis_type_id":0, "pipeline_id":-1}
api_host="https://playground.gpapdev.cnag.eu/analysis_service/"

username='test'#os.environ['username']
password= os.environ['password']


keycloak_openid = KeycloakOpenID(server_url="https://sso.gpapdev.cnag.eu/",
                    client_id="genomed4all",
                    realm_name="playground",
                    client_secret_key="ac1ae8c6-d525-472e-ab59-7631399cdc29",
                    verify=False)

import requests,json
token = keycloak_openid.token(username,password )

headers={"Content-Type": "application/json","Authorization":f"Bearer {token['access_token']}", "host":"playground.gpapdev.cnag.eu"}

#dataload
#first step generate a csv file out of  something,pipeline,query,etc and then link the data id to the training plan




def test_get_analysis_type():
    resp=requests.get(api_host+"/analysis_type/", headers=headers)
    for analysis in resp.json()['data']:
        if analysis['name']=='germline':
           
           pytest.analysis_type_id= analysis['id']
           assert True

def test_get_resource():
    resp=requests.get(api_host+"/resources/", headers=headers)
    for resource in resp.json()['data']:
        if resource['name']=='dry':
           
           pytest.resource_id= resource['id']
           assert True

def test_put_pipeline():
    pipeline_def={
      "analysis_type": "germline",
      "analysis_type_id": pytest.analysis_type_id,
      "name": "dry_germline_plus",
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
            "step": "Upload to Elastic Quality",
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
        "release": "v0.1",
        "pipelines": "sarek,pcgx,annotatesvs,gatk_mt,qualitycontrols",
        "output_format":  {
    "snvs": ["{wd}/results/variant_calling/haplotypecaller/{experiment}/{experiment}.haplotypecaller.filtered.vcf.gz"],
    "cnvs": ["{wd}/results/annotsv/cnvkit/{experiment}/{experiment}.tsv"],
    "svs": ["{wd}/results/annotsv/manta/{experiment}/{experiment}.tsv"],
    "pharmacogenomics": ["{wd}/results/pharmacogenomics/{experiment}/results_gathered_alleles.tsv"],
    "multiqc": ["{wd}/results/multiqc/multiqc_report.html"],
    "cram": ["{wd}/results/preprocessing/recalibrated/{experiment}/{experiment}.recal.cram"],
    "qc_checks": ["{wd}/annotations.json",
                  "{wd}/elastic.json",
                  "{wd}/sequencing_data_quality_check.json",
                  "{wd}/mapping_qc.json",
                  "{wd}/variant.json",
                  "{wd}/workflow_complete.json"]
           }
      },
    }
    #only run whn needed we can not delete it via API
    pytest.pipeline_id = -1
    resp=requests.get(api_host+"/pipelines/", headers=headers)
    for pipeline in resp.json():
        if pipeline['name']=='dry_germline_plus':                   
           pytest.pipeline_id = pipeline['id']

    if  pytest.pipeline_id == -1:
            
            resp=requests.put(api_host+"/pipelines/",json=pipeline_def, headers=headers)
            pytest.pipeline_id = resp.json()['id']
    assert (resp.status_code == 200 or  resp.status_code == 201)


def test_create_dar():
    dar_data1 = {"analysis_type":"germline",
    "analysis_type_id":pytest.analysis_type_id,
 "description":"","clinical_referrer":"",
 "clinical_referrer_contact":"","hospital_name":"",
 "priority":"medium","deadline":"2024-09-04",
 "resource_id":pytest.resource_id,"pipeline_id":pytest.pipeline_id,
 "tumor_experiment_id":"",
 "control_experiment_id":"HG002"}

    resp=requests.put(api_host+"/dars/",json=dar_data1, headers=headers)
    pytest.dar_id=resp.json()['dar_id']
    print(pytest.dar_id)
    assert resp.status_code == 201

def test_run_task():
    resp=requests.post(f"{api_host}/dars/run/{pytest.dar_id}",json={"resource_id":pytest.resource_id}, headers=headers)

    status_array=resp.json()['data'][0]
    if len(status_array[1])==7:
        pytest.task_id=status_array[1]
    elif len(status_array[2])==7:
        pytest.task_id=status_array[2]
        
    else :
        pytest.task_id=status_array[0]
    print(pytest.task_id)
    assert resp.status_code == 200

def test_run_execution():
    import time
    while True:
        
        
        time.sleep(3)
        
        resp= requests.get(f"{api_host}/tasks/{pytest.task_id}", headers=headers)
        
        status=resp.json()['data'][0]['status']
        if status=='completed':
            assert 1==1
            break
        if status=='failed':
            assert 1==0
            break
        

def test_check_status_dar():
    resp=requests.get(f"{api_host}/dars/{pytest.dar_id}", headers=headers)
    assert resp.json()['status']=='review'

    assert resp.status_code == 200 

def test_delete_dar():
    resp=requests.delete(f"{api_host}/dars/{pytest.dar_id}", headers=headers)
    assert resp.status_code == 200 
