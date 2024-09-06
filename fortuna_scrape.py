import requests
from bs4 import BeautifulSoup
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
            data.append((odds(game= item['game'], home= item['home'], draw=item['draw'],away=item['away'], date=None)))
            date = row.find("span",class_="event_datetime")
            print(date)


#for v in data:
 #   print_dom_variables(v)
#df = pd.DataFrame(data)
#df.to_excel('games.xlsx')
