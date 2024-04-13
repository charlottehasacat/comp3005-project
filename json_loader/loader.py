from json import dumps, load
from pathlib import Path
from psycopg import Cursor, connect

# Keys
AWAY_SCORE = "away_score"
AWAY_TEAM = "away_team"
AWAY_TEAM_ID = "away_team_id"
AWAY_TEAM_NAME = "away_team_name"
COMPETITION = "competition"
COMPETITION_ID = "competition_id"
COMPETITION_NAME = "competition_name"
COUNTRY = "country"
COUNTRY_ID = "country_id"
COUNTRY_NAME = "country_name"
EVENTS = "events"
HOME_SCORE = "home_score"
HOME_TEAM = "home_team"
HOME_TEAM_ID = "home_team_id"
HOME_TEAM_NAME = "home_team_name"
ID = "id"
JERSEY_NUMBER = "jersey_number"
KICK_OFF = "kick_off"
LINEUP = "lineup"
MATCH_DATE = "match_date"
MATCH_ID = "match_id"
MATCH_STATUS = "match_status"
MATCH_WEEK = "match_week"
NAME = "name"
PLAYER = "player"
PLAYER_ID = "player_id"
PLAYER_NAME = "player_name"
PLAYER_NICKNAME = "player_nickname"
SEASON = "season"
SEASON_ID = "season_id"
SEASON_NAME = "season_name"
TEAM = "team"
TEAM_ID = "team_id"
TEAMS = "teams"
TYPE = "type"

# Main
def main():
	global data_path
	global matches_folder
	global lineups_folder
	global events_folder

	# Paths
	data_path = Path("data")
	competitions_file = data_path.joinpath("competitions.json")
	matches_folder = data_path.joinpath("matches")
	lineups_folder = data_path.joinpath("lineups")
	events_folder = data_path.joinpath("events")

	selected_competitions = ["La Liga", "Premier League"]

	# Read data from files
	competitions = [c
		for c in load_json_file(competitions_file) \
		if c[COMPETITION_NAME] in selected_competitions
	]
	matches = [m
		for id in set(c[COMPETITION_ID] for c in competitions)
		for m in load_matches(id)
	]
	lineups = (load_match_lineups(m) for m in matches)
	events = (load_match_events(m) for m in matches)

	# Insert into database
	db_name = "project_database"
	db_username = 'postgres'
	db_password = '1234'
	db_host = 'localhost'
	db_port = '5432'
	db_url = f"postgresql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}"
	with connect(db_url, autocommit=True) as connection:
		with connection.cursor() as cursor:
			# Insert competitions
			inserted = [insert_competition(cursor, c) for c in competitions]
			print(f"Inserted {len(inserted)} competitions")
			# Insert matches
			inserted = [insert_match(cursor, m) for m in matches]
			print(f"Inserted {len(inserted)} matches")
			# TODO: count the number of individual lineups and individual events
			# Insert lineups
			inserted = [insert_lineups(cursor, l) for l in lineups]
			print(f"Inserted lineups for {len(inserted)} matches")
			# Insert events
			inserted = [insert_events(cursor, e) for e in events]
			print(f"Inserted events for {len(inserted)} matches")

def load_json_file(path: Path):
	with open(path, "r") as file:
		return load(file)

def load_matches(competition_id: int):
	for child in matches_folder.iterdir():
		if child.is_dir() and child.stem == str(competition_id):
			return [m
				for match_file in child.iterdir()
				if match_file.is_file() and match_file.suffix == '.json'
				for m in load_json_file(match_file)]
	return []

def load_match_lineups(match: dict):
	result = {
		MATCH_ID: match[MATCH_ID],
		TEAMS: []
	}
	for file in lineups_folder.iterdir():
		if file.is_file() and file.suffix == '.json' and file.stem == str(match[MATCH_ID]):
			result[TEAMS] = load_json_file(file)
	return result

def load_match_events(match: dict):
	result = {
		MATCH_ID: match[MATCH_ID],
		EVENTS: []
	}
	for file in events_folder.iterdir():
		if file.is_file() and file.suffix == '.json' and file.stem == str(match[MATCH_ID]):
			result[EVENTS] = load_json_file(file)
	return result

def select_country(cursor: Cursor, name: str):
	return cursor \
		.execute("""
			SELECT * FROM countries
			WHERE name = %s
		""", (name,)) \
		.fetchone()

def insert_country(cursor: Cursor, name: str):
	inserted = select_country(cursor, name)
	if inserted:
		return inserted
	cursor.execute(f"""
		INSERT INTO countries (name) VALUES (%s)
		""", (name,))
	return select_country(cursor, name)

def select_competition(cursor: Cursor, competition: dict):
	return cursor \
		.execute("""
			SELECT * FROM competitions
			WHERE competition_id = %s AND season_id = %s
		""", (competition[COMPETITION_ID], competition[SEASON_ID])) \
		.fetchone()

def insert_competition(cursor: Cursor, competition: dict):
	inserted = select_competition(cursor, competition)
	if inserted:
		return inserted
	country_name = competition[COUNTRY_NAME]
	country_id = insert_country(cursor, country_name)[0]
	cursor.execute(f"""
		INSERT INTO competitions (
			competition_id,
			season_id,
			competition_name,
			season_name,
			country_id
		) VALUES (
			%s, %s, %s, %s, %s
		)
		""", (
		competition[COMPETITION_ID],
		competition[SEASON_ID],
		competition[COMPETITION_NAME],
		competition[SEASON_NAME],
		country_id
	))
	return select_competition(cursor, competition)

def select_team(cursor: Cursor, name: str):
	return cursor \
		.execute("SELECT * FROM teams WHERE name = %s", (name,)) \
		.fetchone()

def insert_team(cursor: Cursor, team: dict):
	team_name = team.get(HOME_TEAM_NAME) or team.get(AWAY_TEAM_NAME)
	team_id = team.get(HOME_TEAM_ID) or team.get(AWAY_TEAM_ID)
	inserted = select_team(cursor, team_name)
	country_id = insert_country(cursor, team[COUNTRY][NAME])[0]
	if inserted:
		return inserted
	cursor.execute(f"""
		INSERT INTO teams (team_id, name, country_id) VALUES (%s, %s, %s)
		""", (team_id, team_name, country_id))
	return select_team(cursor, team_name)

def select_match(cursor: Cursor, match_id: int):
	return cursor \
		.execute(f"SELECT * FROM matches WHERE match_id = {match_id}") \
		.fetchone()

def insert_match(cursor: Cursor, match: dict):
	inserted = select_match(cursor, match[MATCH_ID])
	if inserted:
		return inserted
	home_team_id = insert_team(cursor, match[HOME_TEAM])[0]
	away_team_id = insert_team(cursor, match[AWAY_TEAM])[0]
	cursor.execute(f"""
		INSERT INTO matches (
			match_id, match_date, kick_off, match_status, match_week,
			competition_id, season_id,
			home_score, home_team_id, away_score, away_team_id
		) VALUES (
			%s, %s, %s, %s, %s,
			%s, %s,
			%s, %s, %s, %s
		)
		""", (
		match[MATCH_ID], match[MATCH_DATE], match[KICK_OFF], match[MATCH_STATUS], match[MATCH_WEEK],
		match[COMPETITION][COMPETITION_ID], match[SEASON][SEASON_ID],
		match[HOME_SCORE], home_team_id, match[AWAY_SCORE], away_team_id
	))
	return select_match(cursor, match[MATCH_ID])

def select_player(cursor: Cursor, player_id: int):
	return cursor \
		.execute("SELECT * FROM players WHERE player_id = %s", (player_id,)) \
		.fetchone()

def insert_player(cursor: Cursor, player: dict, team_id: int):
	player_id = player[PLAYER_ID]
	inserted = select_player(cursor, player_id)
	if inserted:
		return inserted
	player_name = player[PLAYER_NAME]
	country: dict = player.get(COUNTRY)
	if country:
		country_name = country.get(COUNTRY_NAME) or country.get(NAME)
		country_id = insert_country(cursor, country_name)[0]
		cursor.execute(f"""
			INSERT INTO players (
				player_id, name, nickname, jersey_number, country_id, team_id
			) VALUES (
				%s, %s, %s, %s, %s, %s
			)
			""", (
			player_id, player_name, player[PLAYER_NICKNAME],
			player[JERSEY_NUMBER], country_id, team_id)
		)
	else:
		cursor.execute(f"""
			INSERT INTO players (
				player_id, name, nickname, jersey_number, team_id
			) VALUES (
				%s, %s, %s, %s, %s
			)
			""", (
			player_id, player_name, player[PLAYER_NICKNAME],
			player[JERSEY_NUMBER], team_id)
		)
	return select_player(cursor, player_id)

def select_lineups(cursor: Cursor, lineups: dict):
	result = []
	for match_id, team_id, player_id, _ in for_each_lineup(lineups):
		found = cursor \
			.execute("""
				SELECT * FROM lineups
				WHERE
					match_id = %s AND
					team_id = %s AND
					player_id = %s
				""", (match_id, team_id, player_id)) \
			.fetchone()
		if found:
			result.append(found)
	return result

def insert_lineups(cursor: Cursor, lineups: dict):
	inserted = select_lineups(cursor, lineups)
	if inserted:
		return inserted
	for match_id, team_id, player_id, player in for_each_lineup(lineups):
		insert_player(cursor, player, team_id)
		cursor.execute("""
			INSERT INTO lineups (
				match_id, team_id, player_id
			) VALUES (
				%s, %s, %s
			)
		""", (match_id, team_id, player_id))
	return select_lineups(cursor, lineups)

def for_each_lineup(lineups: dict):
	match_id = lineups[MATCH_ID]
	for team in lineups[TEAMS]:
		team_id = team[TEAM_ID]
		for player in team[LINEUP]:
			player_id = player[PLAYER_ID]
			yield match_id, team_id, player_id, player

def select_event(cursor: Cursor, uuid: str):
	return cursor \
		.execute("SELECT * FROM events WHERE uuid = %s", (uuid,)) \
		.fetchone()

def insert_events(cursor: Cursor, events: dict):
	match_id = events[MATCH_ID]
	inserted_events = []
	for event in events[EVENTS]:
		uuid = event[ID]
		inserted = select_event(cursor, uuid)
		if inserted:
			continue
		event_type = event[TYPE][NAME].lower()
		event_details = dumps(event.get(event_type)) if event_type in event else None
		team_id = event[TEAM][ID] if TEAM in event else None
		player_id = event[PLAYER][ID] if PLAYER in event else None
		cursor.execute("""
			INSERT INTO events (
				uuid, match_id, event_type, team_id, player_id, event_details
			) VALUES (
				%s, %s, %s, %s, %s, %s
			)
			RETURNING *
			""",(
				uuid, match_id, event_type, team_id, player_id, event_details
			))
		inserted_events.append(cursor.fetchone())
	return insert_events

if __name__ == "__main__":
	main()
