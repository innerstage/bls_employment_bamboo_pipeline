from urllib.request import urlopen
from bs4 import BeautifulSoup
from table_parser import table_to_csv


url = "https://www.bls.gov/sae/additional-resources/list-of-published-state-and-metropolitan-area-series/home.htm"
html = urlopen(url) # Insert your URL to extract
bsObj = BeautifulSoup(html.read(), "lxml")

links = []
for link in bsObj.find_all("a"):
    if "additional-resources" in link.get("href") and "home" not in link.get("href"):
        links.append("https://www.bls.gov" + link.get("href"))

for state in links:

    html_st = urlopen(state)
    state_name = state[state.rfind("/"):].replace("htm","csv")
    df = table_to_csv(html_st, "data_temp/"+state_name)

    print(df.head())