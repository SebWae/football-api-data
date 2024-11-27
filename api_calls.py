import requests
import csv
from headers import headers

def get_countries():
    url = "https://api-football-v1.p.rapidapi.com/v3/countries"

    response = requests.get(url, headers=headers)
    countries = response.json()["response"]

    with open('data/countries.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["name", "code", "flag"])

        for country in countries:
            name = country["name"]
            code = country["code"]
            flag = country["flag"]
            writer.writerow([name, code, flag])


def get_leagues(country):
    url = "https://api-football-v1.p.rapidapi.com/v3/leagues"

    querystring = {"country": country}

    response = requests.get(url, headers=headers, params=querystring)
    leagues = response.json()["response"]

    with open('data/leagues.csv', 'a', newline='') as file:
        writer = csv.writer(file)

        for league in leagues:
            id = league["league"]["id"]
            name = league["league"]["name"]
            type = league["league"]["type"]
            country = league["country"]["name"]
            first_season = league["seasons"][0]["year"]
            writer.writerow([id, name, type, country, first_season])