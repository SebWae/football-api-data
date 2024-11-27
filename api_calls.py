import requests
import csv

def get_countries():
    url = "https://api-football-v1.p.rapidapi.com/v3/countries"

    headers = {
	"x-rapidapi-key": "99dabacffdmshe2f65f615aad2f1p14aaa5jsnb020480e6c10",
	"x-rapidapi-host": "api-football-v1.p.rapidapi.com"
    }

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