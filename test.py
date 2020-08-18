


import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime

url = "https://store.steampowered.com/stats/Steam-Game-and-Player-Statistics"

res= requests.get(url)

tablerow=[["game", "current players", "peak today"]]

soup = BeautifulSoup(res.content,features="html.parser")
for item in soup.find_all("tr",{"class" : "player_count_row"}):
    colums = []
    for tableData in item.find_all("td"):
        colums.append(tableData.get_text().strip())
    tablerow.append([colums[3], colums[0], colums[1]])

now = datetime.now()
filename="steamstat_{0}-{1}-{2}-{3}h{4}m.csv".format(now.year, now.month, now.day, now.hour, now.minute)
with open(filename ,'w',newline='') as csvfile:
    statwriter = csv.writer(csvfile)
    for row in tablerow:
        statwriter.writerow(row)




