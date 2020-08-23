
import requests
from bs4 import BeautifulSoup
import csv
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
import time
from pymongo import MongoClient
import collections

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


RealtimePeakPlayersToCSV()
RealtimeCurrentPlayersToCSV()
DailyPeakPlayersToCSV()