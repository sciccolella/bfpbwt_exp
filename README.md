Experiment replication for manuscript


## Requirements:
- `bcftools`
- `snakemake`
- [csvkit](https://csvkit.readthedocs.io/en/latest/)
- [haploblock](https://gitlab.com/bacazaux/haploblocks) for vcf to bm conversion
- [pbwt](https://gitlab.com/bacazaux/haploblocks)
- [parallel_pbwt](https://github.com/rwk-unil/parallel_pbwt)
- [Syllable-PBWT](https://github.com/ZhiGroup/Syllable-PBWT/tree/master)

## Download and prepare files
```bash
# download VCF files
mkdir -p ./phase3/ && rsync -av --include="*.vcf.gz" --exclude="*" rsync://ftp.1000genomes.ebi.ac.uk/vol1/ftp/release/20130502/ ./phase3/

# Covert to BCF and keep only biallelic SNPs
find ./phase3 -name "*.vcf.gz" | parallel --plus -j6 'bcftools view -m2 -M2 -v snps -Ob -o {..}.bcf --threads 4 {}'
```

## Patching other tools
All the patch for the tools are available in [tool_patches/](./tool_patches) and can be applied to the
corresponding repository with `git apply <patch_file>`.

## Running:
```bash
snakemake -c CORES -p
```
