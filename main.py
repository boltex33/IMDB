import requests
from bs4 import BeautifulSoup
import numpy as np
import mysql.connector
import os

db_user = os.environ.get('DB_USER')
db_pass = os.environ.get('DB_PASS')
db_name = os.environ.get('DB_NAME')

movie_name = []
year = []
time = []
rating = []
metascore = []
votes = []
gross = []
nr_pages = 2  # max is 20
pages = np.arange(1, nr_pages * 50, 50)
start_id = 1
pages_nr = 1
clear_data = 0

for page in pages:
    page = requests.get("https://www.imdb.com/search/title/?groups=top_1000&start=" + str(page) + "&ref_=adv_nxt")
    print(f"Page {pages_nr} done")
    pages_nr += 1
    soup = BeautifulSoup(page.content, 'lxml')
    movie_data = soup.find_all('div', attrs={'class': 'lister-item mode-advanced'})
    for store in movie_data:
        name = store.h3.a.text
        movie_name.append(name)

        year_of_release = store.h3.find('span', class_='lister-item-year text-muted unbold') \
            .text.replace('(', '').replace(')', '')
        year.append(year_of_release)

        runtime = store.p.find('span', class_='runtime').text.replace('min', '')
        time.append(runtime)

        rate = store.find('div', class_='inline-block ratings-imdb-rating').text.replace('\n', '')
        rating.append(rate)

        meta = store.find('span', class_='metascore'). \
            text.replace(' ', '') if store.find('span', class_='metascore') else '-'
        metascore.append(meta)

        values = store.find_all('span', attrs={'name': 'nv'})

        vote = values[0].text
        votes.append(vote)

        grosses = values[1].text if len(values) > 1 else '-'
        gross.append(grosses)

        # saving data to mysql database
        db = mysql.connector.connect(user=db_user, password=db_pass, database=db_name)

        delete_current_data = ("delete from movies_list where id")

        add_news = ("INSERT INTO movies_list"
                    "(id,name_movie,release_year,runtime,rating,metascor,votes,gross)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")

        data_news = (start_id, name, year_of_release, runtime, rate, meta, vote, grosses)

        cursor = db.cursor()

        if pages_nr == 2 and clear_data == 0:
            cursor.execute(delete_current_data)
            cursor.execute(add_news, data_news)
            clear_data = 1
            db.commit()
            cursor.close()
            db.close()
        else:
            cursor.execute(add_news, data_news)
            db.commit()
            cursor.close()
            db.close()

        start_id += 1

print("Done")
