# --------------- CODE TO GENERATE CHANNELS -----------------

def is_gvcf_not_empty(file_path):
    with gzip.open(file_path, 'rt') as f:
        for line in f:
            if not line.startswith("#"):
                return True  # If there's a non-header line, the file is not empty
    return False  # If no non-header lines found, it's empty

def is_chrom_not_empty(files, chrom):
    for tool in files:
        file_path = files[tool]
        file_path = file_path.replace("chromosome", str(chrom))
        if is_gvcf_not_empty(file_path):
            return True
    return False

def generateChannelsSOMATIC(config_path):
    with open(config_path, "r") as f:
        config = json.load(f)
    files = config['process']['somatic']
    for chrom in [*range(1, 23)] + ['X', 'Y', 'M']:
        if is_chrom_not_empty(files, chrom):
            if chrom == 'M': print(23)
            elif chrom == 'X': print(24)
            elif chrom == 'Y': print(25)
            else: print(chrom)

def generateChannelsGERMLINE(config_path):
    with open(config_path, "r") as f:
        config = json.load(f)
    experiments = config_file["process"]["experiments_list"]
    if config_file["process"]["assembly"] == "37":
        chromosomes = [f"{i}" for i in range(1, 23)] + ["MT", "X", "Y"]
    if config_file["process"]["assembly"] == "38":
        chromosomes = [f"chr{i}" for i in range(1, 23)] + ["chrM", "chrX", "chrY"]
    failed = []
    for chrom in range(1, 26):
        for experiment in experiments:
            chromosome = chromosomes[chrom]
            experiment_path = experiment.replace("allchr", "chrchromosome").replace("chrchromosome", f"{chromosome}")
            if not os.path.exists(experiment_path):
                failed.append((experiment, experiment_path))
        print(chrom)
    if failed:
        raise Exception(f"Experiments not found and where: {failed}.")

# MAIN SECTION ---------------------------------------------------------------

if __name__ == "__main__":
    args = parseOpts()
    if args['step'] == 0:
        # WARNING: at the moment this only works if each tool has only 1 file
        # if the releases are done one experiment at a time, this souldn't be an issue
        preprocessSOMATIC(args['config'])
    if args['step'] == 1:
        # Function to generate the channels. It will skip any chromosomes with empty table.
        # IMPORTANT! DO NOT print anything alse during this process 
        if analysis_type == 'tumor_only' or analysis_type == 'tumor_normal':
            generateChannelsSOMATIC(args['config'])