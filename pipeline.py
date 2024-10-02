import json
import pandas as pd
#from geopy import Nominatim
import numpy as np
import requests
from bs4 import BeautifulSoup
import psycopg2


url="https://en.wikipedia.org/wiki/List_of_Premier_League_clubs"

def get_wiki_page(url):
    print("getting wiki page",url)
    try:
        response= requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error occured: {e}")


def get_wiki_data(html):
    soup= BeautifulSoup(html,'html.parser')
    table = soup.find_all("table", {"class": "wikitable sortable"})[0]
    
    return table


def extract_wikipedia_data(**kwargs):
    url=kwargs['url']
    html=get_wiki_page(url)

    if not html:
        return "Failed to retrieve Wikipedia page."
    

    table=get_wiki_data(html)

    clubs=[]
    for row in table.find_all('tr')[1:]:
        columns=row.find_all('td')
        if columns:
            club_name = columns[0].text.strip()
            location = columns[1].text.strip()
            Total_seasons = columns[2].text.strip()
            Total_spells = columns[3].text.strip()
            Longest_spell = columns[4].text.strip()
            recent_promotion = columns[5].text.strip()
            recent_relegation = columns[6].text.strip()
            total_seasons_absent = columns[7].text.strip()
            seasons = columns[8].text.strip()
            current_spell = columns[9].text.strip()
            recent_finish = columns[10].text.strip()
            highest_finish = columns[11].text.strip()
            top_scorer= columns[12].text.strip()
            clubs.append({
                'club' : club_name,
                'location' : location,
                'total seasons' : Total_seasons,
                'total spells' : Total_spells,
                'longest spells' : Longest_spell,
                'recent promotion' : recent_promotion,
                'recent relegation' : recent_relegation,
                'total seasons absent' : total_seasons_absent,
                'seasons' : seasons,
                'current spell' : current_spell,
                'recent finish' : recent_finish,
                'highest finish' : highest_finish,
                'top scorer':top_scorer 
            })
    json_rows= json.dumps(clubs)
    kwargs['ti'].xcom_push(key='rows',value=json_rows)



    return "OK"

'''
def get_lang_log(country, city):
    geolocator= Nominatim(user_agent='geoapiExercises')
    location = geolocator.geocode(f'{city}')

    if location:
        return location.latitude, location.longitude

    return None

'''


def transform_wikipedia_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data=json.loads(data)

    team_df=pd.DataFrame(data)

    #team_df['location'] = team_df.apply(lambda x : get_lang_log(x['location']))
    team_df['location'] = team_df['location'].str.split('(').str[0]
    team_df['recent relegation'] = team_df['recent relegation'].apply(lambda x : np.nan if x ==  'Never relegated' else x)


    kwargs['ti'].xcom_push(key='rows', value=team_df.to_json())

    return "OK"



def write_wikipedia_data(**kwargs):
    # Pull data from Airflow's XCom (e.g., the result of a previous task)
    data = kwargs['ti'].xcom_pull(key='rows', task_ids='transform_wikipedia_data')
    
    # Convert the JSON string to a pandas DataFrame
    data = json.loads(data)
    df = pd.DataFrame(data)
    
    # Database connection parameters
    conn = psycopg2.connect(
        host="postgres",  # PostgreSQL service name from Docker Compose
        database="airflow",  # The name of the database
        user="airflow",  # The username
        password="airflow"  # The password
    )
    
    # Insert DataFrame rows into PostgreSQL
    cursor = conn.cursor()
    
    for index, row in df.iterrows():
        cursor.execute("""
            INSERT INTO football (club, location, total_seasons, total_spells, longest_spells, recent_promotion, recent_relegation, total_seasons_absent, seasons, current_spell, recent_finish, highest_finish, top_scorer) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (row['club'], row['location'], row['total seasons'], row['total spells'], 
              row['longest spells'], row['recent promotion'], row['recent relegation'], 
              row['total seasons absent'], row['seasons'], row['current spell'], 
              row['recent finish'], row['highest finish'], row['top scorer']))
    
    # Commit the transaction and close the connection
    conn.commit()
    cursor.close()
    conn.close()






