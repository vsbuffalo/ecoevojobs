import os
import time
import requests
from collections import defaultdict
from dotenv import load_dotenv
import json
import polars as pl
from tqdm import tqdm
import math

# Base URL for the Department of Education's College Scorecard API
base_url = "https://api.data.gov/ed/collegescorecard/v1/schools"

load_dotenv()
api_key = os.getenv("DATA_GOV_KEY")

# Do the initial query to get the pagination data
params = {"api_key": api_key}
response = requests.get(base_url, params=params)
metadata = response.json()["metadata"]

total_schools = metadata["total"]
per_page = metadata["per_page"]
total_pages = math.ceil(total_schools / per_page)

instiutions = defaultdict(list)
carnegie_classes = defaultdict(list)
for page in tqdm(range(total_pages)):
    params = {"api_key": api_key, "page": page}
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        data = response.json()
        for result in data["results"]:
            name = result["school"]["name"]
            instiutions[name].append(result)
            score = result["school"]["carnegie_basic"]
            city = result["school"]["city"]
            state = result["school"]["state"]
            carnegie_classes[name].append(
                {"carnegie_basic": score, "city": city, "state": state}
            )
    else:
        raise ValueError(f"Error on page {page}: {response.status_code}")
    time.sleep(0.1)


# write the total JSON dataset (around 1.5Gb)
with open("college_scorecard.json", "w") as f:
    json.dump(instiutions, f)


# clean up the carnegie basic data into dataframe
rows = []
for institution, entries in carnegie_classes.items():
    for i, entry in enumerate(entries):
        campus = i if len(entries) > 1 else None
        city, state = entry["city"], entry["state"]
        carnegie_basic = entry["carnegie_basic"]
        rows.append(
            {
                "institution": institution,
                "city": city,
                "state": state,
                "carnegie_basic": carnegie_basic,
                "campus": campus,
            }
        )
pl.DataFrame(rows).write_csv("college_scorecard_carnegie_basic.csv")
