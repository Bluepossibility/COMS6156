import json
import mysql.connector
import csv
import sys
from mysql.connector import Error
from nba_api.stats.endpoints import commonplayerinfo
import time

# basic info
db_user = "root"
db_password = "dbuserdbuser"
connection, cursor = None, None
games_declaration = """
                        CREATE TABLE IF NOT EXISTS GAMES(
                            GAME_DATE_EST varchar(64) not NULL,
                            GAME_ID varchar(64) not NULL,
                            GAME_STATUS_TEXT varchar(64),
                            HOME_TEAM_ID varchar(64),
                            VISITOR_TEAM_ID varchar(64),
                            SEASON varchar(64),
                            TEAM_ID_home varchar(64),
                            PTS_home varchar(64),
                            FG_PCT_home varchar(64),
                            FT_PCT_home varchar(64),
                            FG3_PCT_home varchar(64),
                            AST_home varchar(64),
                            REB_home varchar(64),
                            TEAM_ID_away varchar(64),
                            PTS_away varchar(64),
                            FG_PCT_away varchar(64),
                            FT_PCT_away varchar(64),
                            FG3_PCT_away varchar(64),
                            AST_away varchar(64),
                            REB_away varchar(64),
                            HOME_TEAM_WINS varchar(64))
                    """
game_details_declaration = """
                                CREATE TABLE IF NOT EXISTS GAME_DETAILS
                                (
                                    GAME_ID varchar(64) not NULL,
                                    TEAM_ID varchar(64) not NULL,
                                    TEAM_ABBREVIATION varchar(64) not NULL,
                                    TEAM_CITY varchar(64),
                                    PLAYER_ID varchar(64) not NULL,
                                    PLAYER_NAME varchar(64) not NULL,
                                    NICKNAME varchar(64),
                                    START_POSITION varchar(64),
                                    COMMENT varchar(256),
                                    MIN varchar(8),
                                    FGM varchar (8),
                                    FGA varchar(8),
                                    FG_PCT varchar(64),
                                    FG3M varchar(8),
                                    FG3A varchar(8),
                                    FG3_PCT varchar(64),
                                    FTM varchar(8),
                                    FTA varchar(8),
                                    FT_PCT varchar(64),
                                    OREB varchar(8),
                                    DREB varchar(8),
                                    REB varchar(8),
                                    AST varchar(8),
                                    STL varchar(8),
                                    BLK varchar(8),
                                    TOs varchar(8),
                                    PF varchar(8),
                                    PTS varchar(8),
                                    PLUS_MINUS varchar(8));
                            """

player_basics_declaration = """
                                CREATE TABLE IF NOT EXISTS PLAYER_BASICS
                                (
                                    PLAYER_ID varchar(64),
                                    FIRST_NAME varchar(64),
                                    LAST_NAME varchar(64),
                                    BIRTHDATE varchar(128),
                                    SCHOOL varchar(64),
                                    COUNTRY varchar(64),
                                    HEIGHT varchar(64),
                                    WEIGHT varchar(64),
                                    POSITION varchar(64));
                            """

games_insert_query = """
                        INSERT INTO NBA.GAMES (
                            GAME_DATE_EST,
                            GAME_ID,
                            GAME_STATUS_TEXT,
                            HOME_TEAM_ID,
                            VISITOR_TEAM_ID,
                            SEASON,
                            TEAM_ID_home,
                            PTS_home,
                            FG_PCT_home,
                            FT_PCT_home,
                            FG3_PCT_home,
                            AST_home,
                            REB_home,
                            TEAM_ID_away,
                            PTS_away,
                            FG_PCT_away,
                            FT_PCT_away,
                            FG3_PCT_away,
                            AST_away,
                            REB_away,
                            HOME_TEAM_WINS)
                                VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
                    """

game_details_insert_query = """
                INSERT INTO NBA.GAME_DETAILS (
                    GAME_ID,
                    TEAM_ID,
                    TEAM_ABBREVIATION,
                    TEAM_CITY,
                    PLAYER_ID,
                    PLAYER_NAME,
                    NICKNAME,
                    START_POSITION,
                    COMMENT,
                    MIN,
                    FGM,
                    FGA,
                    FG_PCT,
                    FG3M,
                    FG3A,
                    FG3_PCT,
                    FTM,
                    FTA,
                    FT_PCT,
                    OREB,
                    DREB,
                    REB,
                    AST,
                    STL,
                    BLK,
                    TOs,
                    PF,
                    PTS,
                    PLUS_MINUS) 
                        VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')
            """
player_basics_insert_query = """
                        INSERT INTO NBA.PLAYER_BASICS (
                            PLAYER_ID,
                            FIRST_NAME,
                            LAST_NAME,
                            BIRTHDATE,
                            SCHOOL,
                            COUNTRY,
                            HEIGHT,
                            WEIGHT,
                            POSITION)
                                VALUES ('{}','{}','{}','{}','{}','{}','{}','{}','{}')
                    """

if __name__ == "__main__":
    try:
        # connect to local database
        connection = mysql.connector.connect(
            host='localhost',
            user="root",
            password="dbuserdbuser")
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        cursor.execute("CREATE DATABASE IF NOT EXISTS NBA")
        cursor.execute("USE NBA")
        # ------------------------------------ games ----------------------------------------------------
        games = []
        # Open the file for reading and then wrap with a CSV reader class.
        with open('./games.csv') as in_text_file:
            csv_file = csv.DictReader(in_text_file)
            for r in csv_file:
                games.append(r)

        cursor.execute("DROP TABLE IF EXISTS NBA.GAMES")
        cursor.execute(games_declaration)
        print("GAMES insert progress: ")
        for idx, r in enumerate(games):
            values = list(r.values())
            query = games_insert_query.format(
                *[value.replace("'", " ") if value else 'None' for value in values])
            # print(query)
            cursor.execute(query)
            sys.stdout.write('\r')
            sys.stdout.write(
                "{0} in total, now {1:.2f} %".format(len(games), 100 * idx / len(games)))
            sys.stdout.flush()

        # --------------------------------- game details ------------------------------------------------
        game_details = []
        # Open the file for reading and then wrap with a CSV reader class.
        with open('./games_details.csv') as in_text_file:
            csv_file = csv.DictReader(in_text_file)
            for r in csv_file:
                game_details.append(r)

        cursor.execute("DROP TABLE IF EXISTS NBA.GAME_DETAILS")
        cursor.execute(game_details_declaration)

        print("\nGAMES_DETAILS insert progress: ")
        for idx, r in enumerate(game_details):
            values = list(r.values())
            query = game_details_insert_query.format(*[value.replace("'", " ") if value else 'None' for value in values])
            cursor.execute(query)
            sys.stdout.write('\r')
            sys.stdout.write("{0} in total, now {1:.2f} %".format(len(game_details), 100 * idx / len(game_details)))
            sys.stdout.flush()

        # ---------------------------------------- players ----------------------------------------------
        cursor.execute("DROP TABLE IF EXISTS NBA.PLAYER_BASICS")
        cursor.execute(player_basics_declaration)
        print("\nPLAYER_BASICS insert progress")
        with open('./player_id.csv') as f:
            num_players = sum(1 for line in f)
        with open('./player_id.csv') as in_text_file:
            csv_file = csv.DictReader(in_text_file)
            idx = 1
            for r in csv_file:
                player_id = r["PLAYER_ID"]
                s.add(player_id)
                # Player info on www.nba.com
                try:
                    player_info = commonplayerinfo.CommonPlayerInfo(player_id=player_id)
                except json.decoder.JSONDecodeError:
                    continue
                else:
                    # dictionary
                    headers = player_info.get_dict()["resultSets"][0]["headers"]
                    # print(headers)
                    content = player_info.get_dict()["resultSets"][0]["rowSet"][0]
                    # print(values)
                    player_info_dict = dict()
                    for k, v in zip(headers, content):
                        player_info_dict[k] = v
                    values = [player_info_dict["PERSON_ID"],
                              player_info_dict["FIRST_NAME"],
                              player_info_dict["LAST_NAME"],
                              player_info_dict["BIRTHDATE"],
                              player_info_dict["SCHOOL"],
                              player_info_dict["COUNTRY"],
                              player_info_dict["HEIGHT"],
                              player_info_dict["WEIGHT"],
                              player_info_dict["POSITION"]]
                    query = player_basics_insert_query.format(
                        *[str(value).replace("'", " ") if value else 'None' for value in values])
                    cursor.execute(query)
                    # nba api restrictions
                    # see https://github.com/swar/nba_api/issues/176
                    time.sleep(0.600)
                finally:
                    sys.stdout.write('\r')
                    sys.stdout.write("{0} in total, now {1:.2f} %".format(num_players, 100 * idx / num_players))
                    sys.stdout.flush()
                    idx += 1

        # close connection
        if connection and connection.is_connected():
            connection.commit()
            cursor.close()
            connection.close()
            print("\nMySQL connection is closed")


