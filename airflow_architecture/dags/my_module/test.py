from fortuna_scrape import fortuna_scraping
from data_stream import stream_data

df = fortuna_scraping()
stream_data(df)