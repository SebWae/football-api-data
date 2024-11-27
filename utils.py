import csv

def register_team(id, name, logo):
    with open('data/teams.csv', 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([id, name, logo])