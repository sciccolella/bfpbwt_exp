import os
import re

# --- Configuration ---
DATA_DIR = "phase3"
# Executables
BCF_EXE = "bf-pbwt-clean/2bfpbwt-bcf"
BM_EXE = "bf-pbwt-clean/2bfpbwt-bm"
V2B_EXE = "haploblocks/vcf2bm/vcf2bm"
PBWT_EXE = "pbwt/pbwt"
PAR_PBWT_EXE = "parallel_pbwt/app"
SYLLABLE_EXE = "exp/Syllable-PBWT/server"

# Arguments
BCF_ARGS = [
    "lin",
    "ars",
    "prs",
    "spr",
]
BM_ARGS = [
    "blis",
    "blim",
    "bars",
    "barm",
    "prs",
    "bpr",
    "bprs",
    "bprm",
    "spr",
    "sprs",
    "sprm",
]

# --- Helper Logic ---


def get_sample_id(filename):
    """Extracts 'chrXX' or 'wgs' from the filename."""
    match = re.search(r"chr([A-Za-z0-9]+)|wgs", filename)
    if match:
        return match.group(0)
    return filename


# Map of {short_sample_id: full_path_to_original_bcf}
# We use the BCF files as the "source of truth" for which samples exist
BCF_MAP = {
    get_sample_id(f): os.path.join(DATA_DIR, f)
    for f in os.listdir(DATA_DIR)
    if f.endswith(".bcf")
}

SAMPLES = BCF_MAP.keys()

# --- Rules ---


rule all:
    input:
        expand("results/1kp3.{sample}.bcf.{arg}.time", sample=SAMPLES, arg=BCF_ARGS),
        expand("results/1kp3.{sample}.bcf.{arg}", sample=SAMPLES, arg=BCF_ARGS),
        expand("results/1kp3.{sample}.bm.{arg}.time", sample=SAMPLES, arg=BM_ARGS),
        expand("results/1kp3.{sample}.bm.{arg}", sample=SAMPLES, arg=BM_ARGS),
        expand("results/1kp3.{sample}.pbwt.time", sample=SAMPLES),
        expand("results/1kp3.{sample}.parpbwt.time", sample=SAMPLES),
        expand("results/1kp3.{sample}.syl.time", sample=SAMPLES),
        "results/all.csv",


rule extractVerbose:
    input:
        time="results/1kp3.{sample}.{sub}.{tool}.time",
    output:
        "results/1kp3.{sample}.{sub}.{tool}.time.csv",
    wildcard_constraints:
        sub="bcf|bm",
    shell:
        """
        python time_to_csv.py {wildcards.sub}-{wildcards.tool} {wildcards.sample} \
        < {input.time} > {output}
        """


rule extractVerboseM:
    input:
        time="results/1kp3.{sample}.{tool}.time",
    output:
        "results/1kp3.{sample}.{tool}.time.csv",
    wildcard_constraints:
        tool="pbwt|parpbwt|syl",
    shell:
        """
        python time_to_csv.py {wildcards.tool} {wildcards.sample} \
        < {input.time} > {output}
        """


rule stack_times:
    input:
        expand(
            "results/1kp3.{sample}.{tool}.time.csv",
            sample=SAMPLES,
            tool=[
                "bcf.ars",
                "bcf.lin",
                "bcf.prs",
                "bcf.spr",
                "bm.barm",
                "bm.bars",
                "bm.blim",
                "bm.blis",
                "bm.bpr",
                "bm.bprs",
                "bm.bprm",
                "bm.prs",
                "bm.spr",
                "bm.sprs",
                "bm.sprm",
                "parpbwt",
                "pbwt",
                "syl",
            ],
        ),
    output:
        "results/all.csv",
    shell:
        """
        csvstack {input} > {output}
        """


# 1. Convert BCF to VCF (Intermediate)
rule bcf_to_vcf:
    input:
        bcf=lambda wildcards: BCF_MAP[wildcards.sample],
    output:
        vcf=temp(os.path.join(DATA_DIR, "{sample}.vcf")),
    shell:
        "bcftools view {input.bcf} > {output.vcf}"


# 2. Convert VCF to BM
rule vcf_to_bm:
    input:
        vcf=os.path.join(DATA_DIR, "{sample}.vcf"),
    output:
        bm=os.path.join(DATA_DIR, "{sample}.bm"),
    shell:
        "{V2B_EXE} -v {input.vcf} -o {output.bm}"


# 3. Run BCF Executable
rule run_bf_pbwt_bcf:
    input:
        bcf=lambda wildcards: BCF_MAP[wildcards.sample],
    output:
        out="results/1kp3.{sample}.bcf.{arg}",
        time="results/1kp3.{sample}.bcf.{arg}.time",
    benchmark:
        "benchmarks/1kp3.{sample}.bcf.{arg}.benchmark.txt"
    wildcard_constraints:
        arg="[a-zA-Z0-9]+",
    threads: lambda wildcards: 8 if wildcards.arg in ["bpr", "spr", "prs"] else 1
    shell:
        "OMP_NUM_THREADS={threads} \\time -vo {output.time} {BCF_EXE} {wildcards.arg} {input.bcf} &> {output.out}"


ruleorder: run_bf_pbwt_bm_ramlimit > run_bf_pbwt_bm


# 4. Run BM Executable
rule run_bf_pbwt_bm:
    input:
        bm=os.path.join(DATA_DIR, "{sample}.bm"),
    output:
        out="results/1kp3.{sample}.bm.{arg}",
        time="results/1kp3.{sample}.bm.{arg}.time",
    benchmark:
        "benchmarks/1kp3.{sample}.bm.{arg}.benchmark.txt"
    wildcard_constraints:
        arg="[a-zA-Z0-9]+",
    threads:
        lambda wildcards: (
            8
            if wildcards.arg in ["bpr", "bprs", "bprm", "spr", "prs", "sprs", "sprm"]
            else 1
        )
    shell:
        "OMP_NUM_THREADS={threads} \\time -vo {output.time} {BM_EXE} {wildcards.arg} {input.bm} &> {output.out}"


# 5. Run durbin PBWT
rule run_durbin_pbwt:
    input:
        bcf=lambda wildcards: BCF_MAP[wildcards.sample],
    output:
        out="results/1kp3.{sample}.pbwt",
        time="results/1kp3.{sample}.pbwt.time",
    benchmark:
        "benchmarks/1kp3.{sample}.pbwt.benchmark.txt"
    shell:
        "\\time -vo {output.time} {PBWT_EXE} -readVcfGT {input.bcf} -maxWithin &> {output.out}"


# 6. Run Parallel PBWT
rule run_parallel_pbwt:
    input:
        bcf=lambda wildcards: BCF_MAP[wildcards.sample],
    output:
        out="results/1kp3.{sample}.parpbwt",
        time="results/1kp3.{sample}.parpbwt.time",
    benchmark:
        "benchmarks/1kp3.{sample}.parpbwt.benchmark.txt"
    threads: 8
    params:
        tmp_txt="/tmp/{sample}.torm.txt",
    shell:
        """
        \\time -vo {output.time} {PAR_PBWT_EXE} -f {input.bcf} -o {params.tmp_txt} -t {threads} > {output.out}
        
        # Cleanup undesired files created by the app
        rm -f {params.tmp_txt}*
        """


rule run_syllable:
    input:
        vcf=os.path.join(DATA_DIR, "{sample}.vcf"),
    output:
        out="results/1kp3.{sample}.syl",
        time="results/1kp3.{sample}.syl.time",
    benchmark:
        "benchmarks/1kp3.{sample}.syl.benchmark.txt"
    shell:
        "\\time -vo {output.time} {SYLLABLE_EXE} -f fifo -i {input.vcf} &> {output.out}"
