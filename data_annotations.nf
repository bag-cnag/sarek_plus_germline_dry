#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// CNV --------------------------------------------------------

process importCNV {
  cpus 16
  memory '128 GB'
  tag "Chromosome: $chrom"
  input:
    val chrom
  output:
    val chrom
  script:
    """
    /apps/spark-3.*/bin/spark-submit  --driver-memory 128g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg "$params.path"/main.py --config "$params.path"/cnv_config.json --assembly $params.assembly --pipeline CNV --step 1
    """
}

process exportCNV {
  cpus 1
  memory '6 GB'
  executor 'local'
  maxForks = 1
  tag "Chromosome: $chrom"
  input:
    val chrom
  output:
    val chrom
  script:
    """
    /apps/spark-3.*/bin/spark-submit --master local[4] --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg "$params.path"/main.py --config "$params.path"/cnv_config.json --assembly $params.assembly --pipeline CNV --step 2
    #/apps/spark-3.*/bin/spark-submit --master local[4] --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg "$params.path"/main.py --config "$params.path"/cnv_config.json --assembly $params.assembly --pipeline CNV --step 3
    """
}


// SNV --------------------------------------------------------


process importVCF {
  cpus 20
  memory '32 GB'
  queue 'scratch'

  tag "Import Chromosome: $chrom"
  input:
    val chrom
  output:
    tuple val(chrom), env(sparse_path)
  script:
    """
    /apps/spark-3.*/bin/spark-submit  --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg  "$params.path"/main.py --config $params.path/snv_config.json --pipeline SNV --assembly $params.assembly --step 0 --chrom $chrom --sparse_path '' 
    source .command.int_env
    """
}

process annotateVCF {
  cpus 5
  memory '64 GB'

  tag "Annot Chromosome: $chrom, $sparse_path"
  input:
    tuple val(chrom), val(sparse_path)
    //tuple chrom SPARSE_PATH
  output:
    tuple val(chrom), val(sparse_path)
  script:
    """
    printf $sparse_path
    /apps/spark-3.*/bin/spark-submit  --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg  "$params.path"/main.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 1 --chrom $chrom --sparse_path $sparse_path  
    """
}

process pushSNV {
  maxForks = 1
  cpus 10
  memory '16 GB'
  //clusterOptions = '-w cnd19'
  errorStrategy 'terminate'
  

  tag "Push Chromosome: $chrom"
  input:
    tuple val(chrom), val(sparse_path)
  output:
    tuple val(chrom), val(sparse_path)

  script:
    """
    nice -n 20 /apps/spark-3.*/bin/spark-submit --master local[4] --driver-memory 16g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg  "$params.path"/main.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 3 --chrom $chrom --sparse_path $sparse_path  
    """
}

process checkES {
  executor 'local'
  maxForks = 1
  debug true
  cpus 4
  memory '6 GB'
  errorStrategy 'finish'

  tag "Push Chromosome: $chrom, $sparse_path"
  input:
    tuple val(chrom), val(sparse_path)
  output:
    tuple val(chrom), val(sparse_path)
  script:
    """
    nice -n 20 /apps/spark-3.*/bin/spark-submit --master local[4] --driver-memory 32g --py-files $params.path/vcfLoader-0.1-py3.11.egg  $params.path/main.py --config $params.path/snv_config.json --pipeline SNV --assembly $params.assembly --step 5 --chrom $chrom --sparse_path $sparse_path
    """
}

process updateDM {
  executor 'local'
  maxForks = 1
  debug true
  cpus 4
  memory '6 GB'
  errorStrategy 'terminate'
 
  tag "Push Chromosome: $chrom, $sparse_path"
  input:
    tuple val(chrom), val(sparse_path)
  output:
    stdout
  script:
    """
    nice -n 20 /apps/spark-3.*/bin/spark-submit  --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg  "$params.path"/main.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 4 --chrom $chrom --sparse_path $sparse_path  
    """
}

// SOMATIC --------------------------------------------------------

process preprocessSOMATIC {
  maxForks = 1
  debug true
  cpus 20
  memory '32 GB'
  errorStrategy 'terminate'
 
  tag "Preprocessing SOMATIC Data"
  input:
    val(empty)
  output:
    val(empty)
  script:
    """
    #/apps/spark-3.*/bin/spark-submit  --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg "$params.path"/main.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 0
    python3 $params.path/preprocess_files.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 0
    """
}

process generateChannels {
  cpus 1
  memory '1 GB'
  tag "Define channels"
  input:
    val(empty)
  output:
    stdout
    """
        python3 $params.path/preprocess_files.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 1
    """
}

process annotaONCO {
  cpus 20
  memory '32 GB'

  tag "Annot OncoClassify Chromosome: $chrom, $sparse_path"
  input:
    tuple val(chrom), val(sparse_path)
    //tuple chrom SPARSE_PATH
  output:
    tuple val(chrom), val(sparse_path)
  script:
    """
    printf $sparse_path
    /apps/spark-3.*/bin/spark-submit  --driver-memory 32g --py-files "$params.path"/vcfLoader-0.1-py3.11.egg  "$params.path"/main.py --config $params.path/snv_config.json --pipeline SOMATIC --assembly $params.assembly --step 2 --chrom $chrom --sparse_path $sparse_path  
    """
}

// Pharmacogenomics --------------------------------------------------------


process loadPGX {
  cpus 16
  memory '32 GB'
  tag "Load Pharmacogenomics"
  input:
    val ready
  output:
  path 'pgx_output.ht'
  script:
    """
    nice -n 20 /apps/spark-3.*/bin/spark-submit \
      --driver-memory 32g \
      --py-files /home/groups/dat/lkraatz/pharmacogx/vcfLoader-0.1-py3.11.egg \
      /home/groups/dat/lkraatz/pharmacogx/repo1/omicsloader/python/drivers/pharmacogx.py \
      --config $params.path/snv_config.json \
      --load
    """
}

process pushPGX {
  executor 'local'
  cpus 16
  memory '32 GB'
  tag "Push Pharmacogenomics"
  input:
  path result_file
  output:
  path 'pgx_output.ht'
  script:
    """
    nice -n 20 /apps/spark-3.*/bin/spark-submit \
      --driver-memory 32g \
      --py-files /home/groups/dat/lkraatz/pharmacogx/vcfLoader-0.1-py3.11.egg \
      /home/groups/dat/lkraatz/pharmacogx/repo1/omicsloader/python/drivers/pharmacogx.py \
      --config $params.path/snv_config.json \
      --push \
      --data $result_file
    """
}

process updateDMPGX {
  executor 'local'
  cpus 16
  memory '32 GB'
  tag "Update DM Pharmacogenomics"
  input:
  path result_file
  output:
    stdout
  script:
    """
    nice -n 20 /apps/spark-3.*/bin/spark-submit \
      --driver-memory 32g \
      --py-files /home/groups/dat/lkraatz/pharmacogx/vcfLoader-0.1-py3.11.egg \
      /home/groups/dat/lkraatz/pharmacogx/repo1/omicsloader/python/drivers/pharmacogx.py \
      --config $params.path/snv_config.json \
      --update_dm \
      --index $result_file
    """
}

// Upload s3 --------------------------------------------------------

process uploadS3 {
  executor 'local'
  cpus 1
  memory '16 GB'
  tag "Upload s3"
  input:
    val chrom
  output:
    stdout
  script:
    """
    source ~/.bash_profile
    #source /home/groups/dat/lkraatz/virtual-environments/cram1/bin/activate
    pip install boto3
    nice -n 20 python $params.path/drivers/s3.py --py-files $params.path/vcfLoader-0.1-py3.11.egg --config $params.path/snv_config.json --transfer_files $params.path/crams_transfer.tsv
    """
}

process uploadS3_gvcfs {
  executor 'local'
  queue 'scratch'
  cpus 1
  memory '16 GB'
  tag "Upload s3"
  input:
    val chrom
  output:
    stdout
  script:
    """
    source ~/.bash_profile
    #source /home/groups/dat/lkraatz/virtual-environments/cram1/bin/activate
    pip install boto3
    nice -n 20 python $params.path/drivers/s3.py --py-files $params.path/vcfLoader-0.1-py3.11.egg --config $params.path/snv_config.json --transfer_files $params.path/gvcf_transfer.tsv
    """
}


// PREPARE CONFIG ----------------------------------------

process prepareConfig {
  cpus 1
  memory '6 GB'
  tag "Prepare Config"
  input:
    val chrom
  output:
    stdout
  script:
    """
    source ~/.bash_profile
    nice -n 20 python configPreparation.py --task_id $params.task_id --analysis_type $params.analysis_type --working_dir $params.working_dir --experiment_list $params.experiment_list --config_dir $params.config_dir
    """
}

// PREPROCESS GERMLINE

process preprocessGERMLINE {
  maxForks = 1
  debug true
  cpus 20
  memory '32 GB'
  errorStrategy 'terminate'
 
  tag "Preprocessing GERMLINE Data"
  input:
    val(empty)
  output:
    val(empty)
  script:
    """
    python3 $params.path/preprocess_files.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 0
    """
}

process generateChannelsGERMLINE {
  cpus 1
  memory '1 GB'
  tag "Define channels"
  input:
    val(empty)
  output:
    stdout
    """
        python3 $params.path/preprocess_files.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 1
    """
}


// --------------------------------------------------------

workflow {

    if (params.analysis_type == 'germline') {
        prepareConfig()

        // SNV
        preprocessGERMLINE(prepareConfig.out)
        python_output = generateChannelsGERMLINE(preprocessGERMLINE.out) // HACER EL MISMO PERO PARA GERMLINE
        channels = python_output.map{line -> line.trim().split("\n")}.flatten()
        channels | importVCF | annotateVCF | pushSNV

        // CNV
        prepareConfig.out | importCNV | exportCNV

        // PGX
        prepareConfig.out | loadPGX | pushPGX //| updateDMPGX
    }

    if (params.analysis_type == 'tumor_only' || params.analysis_type == 'tumor_normal') {
        prepareConfig()

        // SNV
        preprocessSOMATIC(prepareConfig.out)
        python_output = generateChannels(preprocessSOMATIC.out)
        channels = python_output.map{line -> line.trim().split("\n")}.flatten()
        channels | annotateVCF | annotaONCO | pushSNV

        // CNV
        prepareConfig.out | importCNV | exportCNV
    }

}