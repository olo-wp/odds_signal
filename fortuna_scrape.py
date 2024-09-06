import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
from Data_class import odds, print_dom_variables

data = []

leagues= ["ekstraklasa-polska","1-anglia","1-niemcy","1-hiszpania"]

for league in leagues:
    url = "https://www.efortuna.pl/zaklady-bukmacherskie/pilka-nozna/{league}".format(league=league)
    response = requests.get(url)
    #print(response.status_code)

    soup = BeautifulSoup(response.text, "html.parser")

    main_table = soup.find_all("tr",class_="")

    for row in main_table:
        item = {}
        n = row.find("span", class_="market-name")
        if n is not None:
            item['game'] = n.text.strip()
            kursy = row.findAll("span",class_="odds-value")
            if kursy is not None:
                item['home'] = kursy[0].text.strip()
                item['draw'] = kursy[1].text.strip()
                item['away'] = kursy[2].text.strip()
            datetime_text = row.find("span",class_="event-datetime").text.strip()
            date_part, time_part = datetime_text.split()
            current_year = datetime.now().year
            full_datetime_str = f"{date_part}.{current_year} {time_part}"
            full_datetime = datetime.strptime(full_datetime_str, "%d.%m.%Y %H:%M")

for v in data:
    print_dom_variables(v)
#df = pd.DataFrame(data)
#df.to_excel('games.xlsx')

'''
dlugi 
komentarz
tego typu
'''


# Wyciągnięcie tekstu z elementu 'span'
#datetime_text = datetime_element.text.strip()
# Podział tekstu na datę i godzinę
#date_part, time_part = datetime_text.split()
# Uzyskanie bieżącego roku
#current_year = datetime.now().year
# Skonstruowanie pełnej daty z bieżącym rokiem
#full_datetime_str = f"{date_part}.{current_year} {time_part}"
# Parsowanie do obiektu datetime, aby upewnić się, że format jest prawidłowy
#full_datetime = datetime.strptime(full_datetime_str, "%d.%m.%Y %H:%M")