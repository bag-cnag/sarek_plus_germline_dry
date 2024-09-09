// Define the first process



include { annotations } from './lot3_annotation'
include { to_elastic } from './lot3_to_elastic'

include {makePostComplete} from './lot3_functions'
include {create_file} from './lot3_functions'

process check_files {
    input:
    val name

    output:
      val message 
    exec:    
    
        println "QC 1";  
        message="QC_2"
        makePostComplete(params.qc_endpoint,"sequencing_data_quality_check.json")
}
process mapping {
    input:
    val message

    output:
      val message2 
    exec:

    println message
    message2 = "QC_3"
//Crams
    create_file(params.outdir+"/results/preprocessing/recalibrated/"+params.control_experiment_id+"/"+params.control_experiment_id+".recal.cram")   
    makePostComplete(params.qc_endpoint,"mapping_qc.json")
}

process variant_calling {
    input:
    val message2

    output:
      val message3 
    exec:

    println message2
    message3 = "dd"
    create_file(params.outdir+"/exp.vcf")
    //SNVs
    create_file(params.outdir+"/results/variant_calling/haplotypecaller/"+params.control_experiment_id+"/"+params.control_experiment_id+".haplotypecaller.filtered.vcf.gz")
     //CNVs
    create_file(params.outdir+"/results/annotsv/cnvkit/"+params.control_experiment_id+"/"+params.control_experiment_id+".tsv")
     //SVs
    create_file(params.outdir+"/results/annotsv/manta/"+params.control_experiment_id+"/"+params.control_experiment_id+".tsv")
     //Pharmacogenomics
    create_file(params.outdir+"/results/pharmacogenomics/"+params.control_experiment_id+"/results_gathered_alleles.tsv")
    //Multiqc
            create_file(params.outdir+"/results/multiqc/multiqc_report.html")
          



    makePostComplete(params.qc_endpoint,"variant.json")
}
// Define the workflow
workflow {
    // Call the first process
    check_files(name:"Hello") | mapping | variant_calling |  annotations | to_elastic | view { it.trim() }
 }

workflow.onComplete {
        makePostComplete(params.qc_endpoint,"workflow_complete.json")
    }
