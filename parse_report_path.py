import datetime
import subprocess
import time
data = []
with open("reports.txt","r") as f:
    for line in f.readlines():
        data.append(line.split(" "))

ref_date = datetime.datetime(2022,1,24,0,0,0)

for row in data:
    if "HLS" in row[-1] and "2.0" in row[-1]:
        name = row[-1].strip("\n")
        if datetime.datetime.strptime(row[0], "%Y-%m-%d") >= ref_date:
            subprocess.run(["python3","handler.py",name])
            print(f"Completed report: {name}. Current Time: {datetime.datetime.now()}. Waiting 60 seconds to send next report.")
            time.sleep(60)
