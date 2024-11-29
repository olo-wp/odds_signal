
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from Data_class import odds
from config import fortuna_leagues

def fortuna_scraping():
    data = []

    for league in fortuna_leagues:
        #time.sleep(1)
        url = "https://www.efortuna.pl/zaklady-bukmacherskie/pilka-nozna/{league}".format(league=league)
        response = requests.get(url)

        soup = BeautifulSoup(response.text, "html.parser")

        main_table = soup.find_all("tr", class_="")

        for row in main_table:
            item = {}
            n = row.find("span", class_="market-name")
            if n is not None:
                item['game'] = n.text.strip()
                kursy = row.findAll("span", class_="odds-value")
                if kursy is not None:
                    item['home'] = kursy[0].text.strip()
                    item['draw'] = kursy[1].text.strip()
                    item['away'] = kursy[2].text.strip()
                datetime_text = row.find("span", class_="event-datetime").text.strip()
                date_part, time_part = datetime_text.split()
                current_year = datetime.now().year
                full_datetime_str = f"{date_part}.{current_year} {time_part}"
                full_datetime = datetime.strptime(full_datetime_str, "%d.%m..%Y %H:%M")
                full_datetime = full_datetime.timestamp()
                data.append(odds(game=item['game'], home=item['home'], draw=item['draw'], away=item['away'],
                                 date=full_datetime))
    #for v in data:
    #    print_dom_variables(v)
    df = pd.DataFrame(data)
    df['home'] = df['home'].astype(float)
    df['draw'] = df['draw'].astype(float)
    df['away'] = df['away'].astype(float)
    df['game'] = df['game'].str.normalize('NFKD').str.encode('ascii',errors='ignore').str.decode('utf-8')
    return df

#df.to_excel('games.xlsx')
