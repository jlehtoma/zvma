from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider

HTTP = HTTPRemoteProvider()

## List datasets ---------------------------------------------------------------

# Base URL for the data
SYKE_BASE_URL = "http://wwwd3.ymparisto.fi/d3/gis_data/spesific"
# Define the two high level datasets ("regional" and "national")
AMA = "zonation_alueellinen"
VMA = "zonation_valtakunnallinen"
# Define full URLs to zip to be downloaded
SYKE_ZIPS = expand("{URL}/{dataset}.zip", URL=SYKE_BASE_URL, dataset=[AMA, VMA])

# Enumerate individual datasets within the zip files
AMA_DATASETS = ["MetZa2018_AMA01",
                "MetZa2018_AMA02",
                "MetZa2018_AMA03",
                "MetZa2018_AMA04",
                "MetZa2018_AMA05",
                "MetZa2018_AMA06"]
VMA_DATASETS = ["MetZa2018_VMA01",
                "MetZa2018_VMA02",
                "MetZa2018_VMA03",
                "MetZa2018_VMA04",
                "MetZa2018_VMA05",
                "MetZa2018_VMA06"]

# Define the various file extensions for the GeoTIFFs
TIFF_COMPONENTS = ["tfw", "tif", "tif.aux.xml", "tif.ovr", "tif.xml"]

## Rules -----------------------------------------------------------------------

rule all:
    input: expand("data/org/{dataset}.{ext}", dataset=[AMA_DATASETS, VMA_DATASETS], ext=TIFF_COMPONENTS)

# Rule to download the zip extract the data from from the zip archive.
rule get_data:
    input:
        ama=HTTP.remote(SYKE_ZIPS[0], keep_local=False),
        vma=HTTP.remote(SYKE_ZIPS[1], keep_local=False)
    output:
        org="data/org",
        # Define the ouputs expanded from the zip files. NOTE that this is
        # slightly quirky as the outputs are now hardcoded (as opposed to
        # read directly from the zip files).
        ama=temp(expand("data/org/{dataset}.{ext}",
                        ama=AMA,
                        dataset=AMA_DATASETS,
                        ext=TIFF_COMPONENTS)),
        vma=temp(expand("data/org/{dataset}.{ext}",
                        vma=VMA,
                        dataset=VMA_DATASETS,
                        ext=TIFF_COMPONENTS))
    log:
        "log/getdata.log"
    run:
        # Inflate the zip files 
        shell("unzip -o {input.ama} -d {output.org} >& {log}")
        shell("unzip -o {input.vma} -d {output.org} >& {log}")
