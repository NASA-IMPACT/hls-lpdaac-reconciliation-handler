import boto3
import datetime
import json
import requests
import sys

def check_in_s3(bucket, prefix):
    objs = s3Client.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return objs

def find_source_file(filename):
    file_comps = filename.split(".")
    dt = datetime.datetime.strptime(file_comps[3],"%Y%jT%H%M%S")
    utm_info = file_comps[2]
    utm_zone = utm_info[1:3]
    lat_band = utm_info[3]
    square = utm_info[4:]
    prefix = f"{utm_zone}/{lat_band}/{square}/{dt:%Y/%-m/%-d}/"
    objs = check_in_s3("s2-archive",prefix)
    return objs.get("Contents", [])

def check_cmr(product_shortname, granule):
    concept_ids = {"HLSL30": "C2021957657-LPCLOUD",
                   "HLSS30": "C2021957295-LPCLOUD"
                }
    concept_id = concept_ids[product_shortname]
    url = f"https://cmr.earthdata.nasa.gov/search/granules.xml?collection_concept_id={concept_id}&readable_granule_name={granule}"
    resp = requests.get(url).text
    hits = int(resp.split("<hits>")[1].split("</hits>")[0])
    if hits > 0:
        return True
    return False

report_name= sys.argv[1]

bucket = "lp-prod-reconciliation"
report_key = f"reports/{report_name}"

forward_bucket = "hls-global-v2-forward"
historical_bucket = "hls-global-v2-historical"

s2_archive_bucket ="s2-archive"
hls_s30_trigger_bucket= "hls-prod-sentinel-input-files"
hls_s30_historical_trigger_bucket= "hls-prod-sentinel-input-files-historical"
print(report_key)
data_bucket = historical_bucket if "historical" in report_key else forward_bucket
trigger_bucket = hls_s30_historical_trigger_bucket if "historical" in report_key else hls_s30_trigger_bucket
s3Resource = boto3.resource("s3")
s3Client = boto3.client("s3")
obj = s3Resource.Object(bucket, report_key)

json_object = json.loads(obj.get()["Body"].read().decode("utf-8"))
if isinstance(json_object,list):
    json_object = json_object

triggered_granules = set()
for prod_dict in json_object:
    for key in prod_dict:
        product_shortname = key.split("_")[0]
        nfiles = 18 if product_shortname == "HLSL30" else 21
        print(f"{int(len(prod_dict[key]['report'])/nfiles)} granules missing from {key}")
        for file in prod_dict[key]["report"]:
            file_objs = file.split(".")
            granule = ".".join(file_objs[:6]).strip("_stac")
            if granule not in triggered_granules:
                data_path = f"{file_objs[1]}/data/{file_objs[3][:7]}/{granule}/{granule}.json"
                manifest = check_in_s3(data_bucket, data_path)
                if manifest["KeyCount"] == 0:
                    data_path = f"{file_objs[1]}/data/{file_objs[3][:7]}/{granule}/twin/{granule}.json"
                    manifest = check_in_s3(data_bucket, data_path)
                print(f"{granule} has a status of {prod_dict[key]['report'][file]['status']}.")
                in_cmr = check_cmr(product_shortname, granule)
                if in_cmr and prod_dict[key]['report'][file]['status'] not in ["queued"]:
                    triggered_granules.add(granule)
                elif manifest.get("Contents", False):
                    '''
                    copySource = {"Bucket": data_bucket, "Key": data_path}
                    s3Client.copy_object(Bucket=data_bucket, Key=data_path,
                            CopySource=copySource,
                            MetadataDirective="REPLACE"
                            )
                    '''
                elif product_shortname == "HLSS30":
                    '''
                    files = find_source_file(granule)
                    for file in files:
                        archive_key = file["Key"]
                        s2_filename = archive_key.split("/")[-1]
                        copySource = {"Bucket": s2_archive_bucket,"Key": archive_key}
                        s3Client.copy_object(Bucket=trigger_bucket, Key=s2_filename,
                                CopySource=copySource
                                )
                    '''
                elif product_shortname == "HLSL30" in key:
                    print("This is not finished")
                triggered_granules.add(granule)
                
