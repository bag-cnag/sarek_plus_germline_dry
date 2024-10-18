import re
import json
import argparse
import sys
import os
import csv


# USAGE SECTION ---------------------------------------------------------------
def parseOpts():
    # Parse opts
    parser = argparse.ArgumentParser()
    parser.add_argument("--task_id", type=str)
    parser.add_argument("--analysis_type", type=str)
    parser.add_argument("--working_dir", type=str)
    parser.add_argument("--experiment_list", type=str)
    parser.add_argument("--config_dir", type=str)

    return vars(parser.parse_args())


# --------------- CODE TO GENERATE CONFIGS ------------------

def prepareConfigs(task_id, analysis_type, wd, experiment, config_dir):
    """
    Function to prepare the configs by finding the files in their corresponding directory and creating the needed configs.
    It starts from a default config and adds the paths corresponding to this execution.
    """
    # Get the default config
    config_path = os.path.join(config_dir, "default_config.json")
    with open(config_path, 'r') as f:
        config = json.load(f)

    # General paths
    destination_path = os.path.join(wd, f"results/annotation/{analysis_type}/{experiment}/")
    source_path = os.path.join(destination_path, "8.0.0/annotated_gnomadws/")
    config["process"]["destination_path"] = destination_path
    config["process"]["source_path"] = source_path

    # Define SNV: 
    if analysis_type == 'germline':
        # Pipeline: germline
        snv_config = copy.deepcopy(config)
        snv_path = [f"{wd}/results/variant_calling/haplotypecaller/{experiment}/{experiment}.haplotypecaller.filtered.vcf.gz"]
        snv_index = f"cnag_snv_{task_id}"
        dense_path = os.path.join(destination_path, "denseMatrix")
        sparse_path = os.path.join(destination_path, "sparseMatrix/0.0.0")
        snv_config["process"]["experiments_list"] = snv_path
        snv_config["resources"]["elasticsearch"]["index_name"] = snv_index
        snv_config["applications"]["combine"]["dense_matrix_path"] = dense_path
        snv_config["applications"]["combine"]["sparse_matrix_path"] = sparse_path
        
    elif analysis_type == 'tumor_only':
        # Pipeline: tumor_only
        snv_config = copy.deepcopy(config)
        mutect_path = f"{wd}/results/variant_calling/mutect2/{experiment}/{experiment}.mutect2.filtered.vcf.gz"
        exps = {'mutect': mutect_path}
        es_index = f"cnag_to_{task_id}"
        es_biomarkers = f"cnag_biom_{task_id}"
        snv_config["process"]["somatic_without_pre"] = exps
        snv_config["resources"]["elasticsearch"]["index_name"] = es_index
        snv_config["resources"]["elasticsearch"]["biomarkers_index"] = es_biomarkers
        
    elif analysis_type == 'tumor_normal':
        # Pipeline: tumor_normal
        snv_config = copy.deepcopy(config)
        mutect_path = f"{wd}/results/variant_calling/mutect2/{experiment}/{experiment}.mutect2.filtered.vcf.gz"
        strelka_snv = f"{wd}/results/variant_calling/strelka/{experiment}/{experiment}.strelka.somatic_snvs.vcf.gz"
        strelka_indels = f"{wd}/results/variant_calling/strelka/{experiment}/{experiment}.strelka.somatic_indels.vcf.gz"
        exps = {'mutect': mutect_path}
        es_index = f"cnag_tn_{task_id}"
        es_biomarkers = f"cnag_biom_{task_id}"
        snv_config["process"]["somatic_without_pre"] = exps
        snv_config["resources"]["elasticsearch"]["index_name"] = es_index
        snv_config["resources"]["elasticsearch"]["biomarkers_index"] = es_biomarkers

    with open(os.path.join(config_dir, "snv_config.json"), 'w') as f:
        json.dump(snv_config, f, indent=4)
    
    # Define CNV and SV: (juntos?)
    cnv_config = copy.deepcopy(config)
    cnv_path = f"{wd}/results/annotsv/cnvkit/{experiment}/{experiment}.tsv"
    sv_path = f"{wd}/results/annotsv/manta/{experiment}/{experiment}.tsv"
    exps = [[experiment, cnv_path, "CNVKIT"],
            [experiment, sv_path, "MANTA"]]
    cnv_index = f"cnag_cnv_{task_id}"
    cnv_config["process"]["experiments_list"] = exps
    cnv_config["resources"]["elasticsearch"]["index_name"] = cnv_index

    with open(os.path.join(config_dir, "cnv_config.json"), 'w') as f:
        json.dump(cnv_config, f, indent=4)
    
    # Define Pharmacogenomics:
    pgx_config = copy.deepcopy(config)
    pgx_source_path = f"{wd}/results/pharmacogenomics/{experiment}/results_gathered_alleles.tsv"
    experiment_list = [experiment]
    index_name_pgx = f"cnag_pgx_{task_id}"
    pgx_config["process"]["pgx_source_path"] = pgx_source_path
    pgx_config["process"]["experiments_list"] = experiment_list
    pgx_config["resources"]["elasticsearch"]["index_name_pgx"] = index_name_pgx

    with open(os.path.join(config_dir, "pgx_config.json"), 'w') as f:
        json.dump(pgx_config, f, indent=4)


# --------------- MAIN -----------------

def main(args):
    print("Preparing configs for Amanda+")

    task_id = args['task_id']
    analysis_type = args['analysis_type']
    working_dir = args['working_dir']
    experiment_list = args['experiment_list']
    config_dir = args['config_dir']

    if len(experiment_list) == 1:
        experiment = experiment_list[0]
    elif len(experiment_list) == 2:
        experiment = f"{experiment_list[0]}_vs_{experiment_list[1]}"
    else:
        raise Exception(f"Wrong experiment_list: {experiment_list}")

    prepareConfigs(task_id, analysis_type, wd, experiment, config_dir)

if __name__ == "__main__":
    args = parseOpts()
    main(args)
