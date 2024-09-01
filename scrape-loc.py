import pandas as pd
from bs4 import BeautifulSoup
import requests

# Grab the wikipedia page for MLS stadiums
url = 'https://en.wikipedia.org/wiki/List_of_Major_League_Soccer_stadiums'

# Get the html for scraping
response = requests.get(url)
html = response.text

# Let beautifulsoup do its magic
soup = BeautifulSoup(html, 'html.parser')
table = soup.find('table', {'class': 'wikitable sortable'})

# Create a dataframe to store the data
df = pd.DataFrame(columns=['stadium', 'team', 'location'])

# Construct the df from the table that we pulled from the html content
for row in table.find_all('tr')[1:]:
    cells = row.find_all(['td', 'th'])
    stadium = cells[1].text.strip()
    team = cells[2].text.strip()
    location = cells[3].text.strip()
    
    print(stadium, team, location)
    df = pd.concat([df, pd.DataFrame({'stadium': [stadium], 'team': [team], 'location': [location]})])

df.to_csv('./locations.csv', index=False)