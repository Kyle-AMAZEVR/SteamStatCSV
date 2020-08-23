import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import time
from pymongo import MongoClient
import collections
import sys

class WindowsInhibitor:
    '''Prevent OS sleep/hibernate in windows; code from:
    https://github.com/h3llrais3r/Deluge-PreventSuspendPlus/blob/master/preventsuspendplus/core.py
    API documentation:
    https://msdn.microsoft.com/en-us/library/windows/desktop/aa373208(v=vs.85).aspx'''
    ES_CONTINUOUS = 0x80000000
    ES_SYSTEM_REQUIRED = 0x00000001

    def __init__(self):
        pass

    def inhibit(self):
        import ctypes
        print("Preventing Windows from going to sleep")
        ctypes.windll.kernel32.SetThreadExecutionState(
            WindowsInhibitor.ES_CONTINUOUS | \
            WindowsInhibitor.ES_SYSTEM_REQUIRED)

    def uninhibit(self):
        import ctypes
        print("Allowing Windows to go to sleep")
        ctypes.windll.kernel32.SetThreadExecutionState(
            WindowsInhibitor.ES_CONTINUOUS)

client=MongoClient('localhost',27017)
db=client["SteamStatDB"]
realtimeCollection=db["SteamStat"]
dailypeakCollection=db["DailyPeak"]
inhibitor = WindowsInhibitor()
inhibitor.inhibit()

def SaveDailyPeakToDB():
    todaystart = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    todayend = datetime.now().replace(hour=23, minute=59, second=59)

    # 상위 50개만 추출
    # 10개만 추출하고 싶다면 10으로 바꾸면됨
    topcount = 50
    print("Save Daily Peak to DataBase Start")
    url = "https://store.steampowered.com/stats/Steam-Game-and-Player-Statistics"
    res = requests.get(url)
    tablerow = {}

    soup = BeautifulSoup(res.content, features="html.parser")
    rank = 1
    for item in soup.find_all("tr", {"class": "player_count_row"}):
        colums = [tableData.get_text().strip() for tableData in item.find_all("td")]
        tablerow[colums[3]] = {"rank": rank, "peaktoday": colums[1]}
        rank = rank + 1
        if rank > topcount:
            break

    recordData = {"recordtime": todaystart, "gamerank": tablerow}

    item = dailypeakCollection.find_one({"recordtime" : { "$lt" : todayend, "$gte" : todaystart }})
    if item is None:
        print(dailypeakCollection.insert_one(recordData).inserted_id)
        print("Save Daily Peak to DataBase End")
    else:
        dailypeakCollection.update_one({"recordtime" : { "$lt" : todayend, "$gte" : todaystart }}, {"$set" : {"gamerank" : tablerow}})
        print("Daily Peak Data Update")


def SaveRealtimeDataToDB():
    # 상위 20개만 추출
    # 10개만 추출하고 싶다면 10으로 바꾸면됨
    topcount=20
    print("Save Realtime Stat to DataBase Start")
    url = "https://store.steampowered.com/stats/Steam-Game-and-Player-Statistics"
    res= requests.get(url)
    tablerow = {}

    soup = BeautifulSoup(res.content,features="html.parser")
    rank=1
    for item in soup.find_all("tr",{"class" : "player_count_row"}):
        colums = [tableData.get_text().strip() for tableData in item.find_all("td")]
        tablerow[colums[3]] = {"rank" : rank, "currentplayers" : colums[0], "peaktoday" : colums[1]}
        rank = rank+1
        if rank > topcount:
            break

    now = datetime.now().replace(second=0, microsecond=0)
    recordData = {"recordtime" : now, "gamerank" : tablerow}
    print(realtimeCollection.insert_one(recordData).inserted_id)
    print("Save Realtime Stat to DataBase End")


def RealtimeCurrentPlayersToCSV():
    DataBaseToCSV("currentplayers", "data/realtimecurrentplayers.csv", realtimeCollection)

def RealtimePeakPlayersToCSV():
    DataBaseToCSV("peaktoday", "data/realtimepeakplayers.csv", realtimeCollection)

def DailyPeakPlayersToCSV():
    DataBaseToCSV("peaktoday", "data/dailypeakplayers.csv", dailypeakCollection)


def DataBaseToCSV(column, csvfilename, targetCollection):
    gamelist=set()
    timeline=[]
    datadic={}
    timelineRank=[]

    # find game list
    for item in targetCollection.find():
        timeline.append(item["recordtime"])
        timelineRank.append(item["gamerank"])
        for gamename in item["gamerank"].keys():
            if gamename not in gamelist:
                gamelist.add(gamename)
                datadic[gamename] = {}

    # set data of timeline
    for index in range(0, len(timeline)):
        for gamename in gamelist:
            if gamename in timelineRank[index]:
                datadic[gamename][index] = timelineRank[index][gamename][column]
            else:
                datadic[gamename][index] = 0

    tablerow=[]
    header = ["time"] + list(datadic.keys())
    tablerow.append(header)
    for index in range(0, len(timeline)):
        row = []
        row.append(timeline[index])
        for gamename in datadic.keys():
            row.append(datadic[gamename][index])
        tablerow.append(row)

    with open(csvfilename, 'w', newline='') as csvfile:
        statwriter = csv.writer(csvfile)
        for row in tablerow:
            statwriter.writerow(row)

def CollectSteamStat():
    # 상위 20개만 추출
    # 10개만 추출하고 싶다면 10으로 바꾸면됨
    topcount=20

    print("Collect stat Start")
    url = "https://store.steampowered.com/stats/Steam-Game-and-Player-Statistics"
    res= requests.get(url)
    tablerow=[["game", "current players", "peak today"]]

    soup = BeautifulSoup(res.content,features="html.parser")
    for item in soup.find_all("tr",{"class" : "player_count_row"}):
        colums = [tableData.get_text().strip() for tableData in item.find_all("td")]
        tablerow.append([colums[3], colums[0], colums[1]])

    now = datetime.now()
    filename="data/realtime/steamstat_{0}-{1}-{2}-{3}h{4}m.csv".format(now.year, now.month, now.day, now.hour, now.minute)
    with open(filename ,'w',newline='') as csvfile:
        statwriter = csv.writer(csvfile)
        for row in tablerow[:topcount+1]:
            statwriter.writerow(row)
    print("Collect stat End")


scheduler = BackgroundScheduler()
scheduler.add_job(func=SaveRealtimeDataToDB, trigger="cron", minute="*/5", id="1")
scheduler.add_job(func=SaveDailyPeakToDB, trigger="cron", hour="*/5", id="2")
scheduler.add_job(func=CollectSteamStat, trigger="cron", minute="*/5", id="3")
scheduler.start()

while True:
    print("Running...", datetime.now())
    time.sleep(60)