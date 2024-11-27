import requests
import csv
from headers import headers

def get_countries():
    url = "https://api-football-v1.p.rapidapi.com/v3/countries"

    response = requests.get(url, headers=headers)
    countries = response.json()["response"]

    with open('countries.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "code", "flag"])

        for country in countries:
            name = country["name"]
            code = country["code"]
            flag = country["flag"]
            writer.writerow([name, code, flag])