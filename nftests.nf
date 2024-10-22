#!/usr/bin/env nextflow
nextflow.enable.dsl=2

// CNV --------------------------------------------------------

process importCNV {
  tag "Chromosome: $chrom"
  input:
    val chrom
  output:
    val chrom
  script:
    """
    echo "import CNV"
    """
}

process exportCNV {
  executor 'local'
  maxForks = 1
  tag "Chromosome: $chrom"
  input:
    val chrom
  output:
    val chrom
  script:
    """
    echo "export CNV"
    """
}


// SNV --------------------------------------------------------


process importVCF {
  queue 'scratch'

  tag "Import Chromosome: $chrom"
  input:
    val chrom
  output:
    tuple val(chrom), env(sparse_path)
  script:
    """
    echo "import VCF $chrom"
    sparse_path="./${chrom}.sparse"
    touch sparse_path
    echo "Sparse file path: $sparse_path"
    """
}

process annotateVCF {

  tag "Annot Chromosome: $chrom, $sparse_path"
  input:
    tuple val(chrom), val(sparse_path)
    //tuple chrom SPARSE_PATH
  output:
    tuple val(chrom), val(sparse_path)
  script:
    """
    echo "annotate VCF $chrom"
    """
}

process pushSNV {
  //clusterOptions = '-w cnd19'
  errorStrategy 'terminate'
  

  tag "Push Chromosome: $chrom"
  input:
    tuple val(chrom), val(sparse_path)
  output:
    tuple val(chrom), val(sparse_path)

  script:
    """
    echo "push SNV $chrom"
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
  errorStrategy 'terminate'
 
  tag "Preprocessing SOMATIC Data"
  input:
    val(empty)
  output:
    val(empty)
  script:
    """
    echo "proprocess SOMATIC"
    """
}

process generateChannels {
  tag "Define channels"
  input:
    val(empty)
  output:
    stdout
    """
        #python3 $params.path/preprocess_somatic.py --config $params.path/snv_config.json --pipeline $params.pipeline --assembly $params.assembly --step 1
        python3 /home/groups/dat/jdieguez/AMANDA/nftests/mock_generateChannels.py
    """
}

process annotaONCO {
  tag "Annot OncoClassify Chromosome: $chrom, $sparse_path"
  input:
    tuple val(chrom), val(sparse_path)
    //tuple chrom SPARSE_PATH
  output:
    tuple val(chrom), val(sparse_path)
  script:
    """
    echo "annotateONCO $chrom"
    """
}

// Pharmacogenomics --------------------------------------------------------


process loadPGX {
  tag "Load Pharmacogenomics"
  input:
    val ready
  output:
    path 'pgx_output.ht'
  script:
    """
    echo "loadPGX"
    touch pgx_output.ht
    """
}

process pushPGX {
  memory '1 GB'
  tag "Push Pharmacogenomics"
  input:
  path result_file
  output:
  path 'pgx_output.ht'
  script:
    """
    echo "pushPGX"
    """
}

process updateDMPGX {
  memory '1 GB'
  tag "Update DM Pharmacogenomics"
  input:
  path result_file
  output:
    stdout
  script:
    """
    echo "updateDMPGX"
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
  tag "Prepare Config"
  input:
  output:
    val true
  script:
    """
    echo "prepare configs"
    #source ~/.bash_profile
    #nice -n 20 python configPreparation.py --task_id $params.task_id --analysis_type $params.analysis_type --working_dir $params.working_dir --experiment_list $params.experiment_list --config_dir $params.config_dir
    """
}

// PREPROCESS GERMLINE

process preprocessGERMLINE {
  errorStrategy 'terminate'
 
  tag "Preprocessing GERMLINE Data"
  input:
    val(empty)
  output:
    val(empty)
  script:
    """
    echo "proprocess GERMLINE"
    """
}

process generateChannelsGERMLINE {
  tag "Define channels"
  input:
    val ready
  output:
    stdout
    """
        python3 /home/groups/dat/jdieguez/AMANDA/nftests/mock_generateChannels.py
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
