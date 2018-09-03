mysql -e "truncate table viiautoboutiquedb.car"
python scrape_mb.py &
python scrape_auto_west_bmw.py used &
python scrape_auto_west_bmw.py demo &
python scrape_the_bmw_store.py &
