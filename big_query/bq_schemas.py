from google.cloud import bigquery

# dictionary to find Google BQ schemas for data file names
bq_schemas = {
    "fixtures_all": [
        bigquery.SchemaField("fixture_id", "INTEGER"),
        bigquery.SchemaField("referee", "STRING"),
        bigquery.SchemaField("timezone", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("kick_off", "TIME"),
        bigquery.SchemaField("venue_id", "INTEGER"),
        bigquery.SchemaField("venue_name", "STRING"),
        bigquery.SchemaField("city", "STRING"),
        bigquery.SchemaField("elapsed_time", "INTEGER"),
        bigquery.SchemaField("extra_time", "INTEGER"),
        bigquery.SchemaField("league_id", "INTEGER"),
        bigquery.SchemaField("league_name", "STRING"),
        bigquery.SchemaField("season", "INTEGER"),
        bigquery.SchemaField("stage", "STRING"),
        bigquery.SchemaField("home_team_id", "INTEGER"),
        bigquery.SchemaField("home_team_name", "STRING"),
        bigquery.SchemaField("away_team_id", "INTEGER"),
        bigquery.SchemaField("away_team_name", "STRING"),
        bigquery.SchemaField("home_ft", "INTEGER"),
        bigquery.SchemaField("away_ft", "INTEGER"),
        bigquery.SchemaField("home_ht", "INTEGER"),
        bigquery.SchemaField("away_ht", "INTEGER"),
        bigquery.SchemaField("home_et", "INTEGER"),
        bigquery.SchemaField("away_et", "INTEGER"),
        bigquery.SchemaField("home_pen", "INTEGER"),
        bigquery.SchemaField("away_pen", "INTEGER")
    ],

    "leagues": [
        bigquery.SchemaField("id", "INTEGER"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("country", "STRING"),
        bigquery.SchemaField("first_season", "INTEGER")
        ],
    
    "teams": [
        bigquery.SchemaField("team_id", "INTEGER"),
        bigquery.SchemaField("team_name", "STRING"),
        bigquery.SchemaField("team_logo", "STRING")
    ]
}