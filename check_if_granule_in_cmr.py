import requests
import sys

def check_cmr(granule):
    product_shortname ="".join( granule.split(".")[0:2])
    concept_ids = {"HLSL30": "C2021957657-LPCLOUD",
                   "HLSS30": "C2021957295-LPCLOUD"
                }
    concept_id = concept_ids[product_shortname]
    url = f"https://cmr.earthdata.nasa.gov/search/granules?collection_concept_id={concept_id}&readable_granule_name={granule}&options[readable_granule_name][pattern]=true"
    resp = requests.get(url).text
    print(resp)
    hits = int(resp.split("<hits>")[1].split("</hits>")[0])
    if hits > 0:
        print(f"{granule} is available in CMR.")
        return True

if len(sys.argv) == 1:
    print("Please provide an input granule name (e.g. HLS.L30.T43QGG.2021228T051400)")
    exit()

status = check_cmr(sys.argv[1])

