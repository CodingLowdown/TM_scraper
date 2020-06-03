#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  2 09:55:04 2020

@author: nicholaslowe
"""

import requests
import pandas as pd
import os
from bs4 import BeautifulSoup as bs
import json
import locale
import numpy as np
import re
import time
import urllib.request as request
import glob

already_scraped_links=[]

df7 = pd.read_csv(os.getcwd() +'/linksDone.csv')

for ijkl in df7['links'].to_list():
    already_scraped_links.append(ijkl)
    

s=requests.session()

header = {'Accept-Encoding': 'utf-8',
                  'Accept-Language': 'en-US,en;q=0.5',
                  'Connection': 'keep-alive',
                  'Content-Length': '0',
                  'Host': 'www.transfermarkt.us',
                  'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 ' \
           '(KHTML, like Gecko) Chrome/51.0.2704.79 Safari/537.36',
                  'X-Instagram-AJAX': '1',
                  'X-Requested-With': 'XMLHttpRequest'}

locale.setlocale(locale.LC_ALL, 'en_US')
BASE='https://www.transfermarkt.us'
os.chdir('/Users/nicholaslowe/Desktop/code.nosync/TransferMarkt')

SeasonList=[2018]
URL_IN='https://www.transfermarkt.us/canadian-premier-league-spring-season/startseite/wettbewerb/CAN1/?saison_id='


def final_master(s,seasonYear):
    print(seasonYear)
    df=get_team_list(s,URL_IN,seasonYear)
    get_players_links(s,df)
    

def get_team_list(s,URL_IN,seasonYear):
    ## Define first url here of the league you want
    BASE_URL=URL_IN+str(seasonYear)
    res1=s.get(BASE_URL,
               headers=header)
    
    soup=bs(res1.text,'html.parser')
    body=soup.find('body')
    
    parsed_table =body.find_all('table')[3].find('tbody')
    
    href_link=[]
    for i in parsed_table.find_all('tr'):
        href_link.append(i.find_all('td')[0].find('a').get('href'))
    
    dfs = pd.read_html(res1.text)
    df = dfs[3]
    
    df = df[df['Club.1'].notnull()]
    
    df['href'] = href_link

    return df


def get_players_links(s,df):
    for j in df['href']:
        url=BASE+j
        res1=s.get(url,
               headers=header)
        soup=bs(res1.text,'html.parser')
        body=soup.find('body')
        
        parsed_table =body.find_all('table')[1].find('tbody')
        regex = re.compile('.*spielprofil_tooltip*')
        href_link=[]
        i=0
        while i < len(parsed_table.find_all('tr')):
            item=parsed_table.find_all('tr')[i]
            try:
                href_link.append(item.find_all('td')[1].find('a', {"class": regex}).get('href'))
            except:
                print('NA')
            i+=2
     
        
        dfs = pd.read_html(res1.text)
        df2 = dfs[1]
        df2 = df2[df2['#'].notnull()]
        df2['href'] = href_link
        for k in df2['href']:
            if k in already_scraped_links:
                print('already Scraped')
                continue
            urlp=BASE+k
            master_team_run(s,urlp)
            already_scraped_links.append(k)
            print(k)
            time.sleep(10)
        




def master_team_run(s,url):
    try:
        df1,res1,PlayerName=get_player_data(s,url)
        df2=get_transfer_value(s,res1,PlayerName)
        results=pd.merge(df1,df2, how='outer',on='PlayerName')
        results.to_excel(os.getcwd()+'/results/'+PlayerName+'.xlsx')
    except:
        print('missing Data for: '+str(url))






def get_player_data(s,urlp):
    res1=s.get(urlp,
               headers=header)

    soup=bs(res1.text,'html.parser')
    body=soup.find('body')

    ScrapedName=body.find('div',{"class":"dataName"}).text.replace('\n','').replace('#','')
    PlayerName = ''.join(i for i in ScrapedName if not i.isdigit()).lstrip()
    
    y=body.find('div',{"class":"nebenpositionen"})
    Otherposlist=[]
    try:
        for br in y.findAll('br'):
            Otherposlist.append(br.nextSibling.rstrip())
    except:
        print('Missing Data')
    dfs = pd.read_html(res1.text)

    Playerdf= dfs[0]
    

    PlayerBirthDat=Playerdf.loc[Playerdf.iloc[:,0] == 'Date of birth:'][1].iloc[0]
    PlayerCitzenship=Playerdf.loc[Playerdf.iloc[:,0] == 'Citizenship:'][1].iloc[0]
    PlayerPosition=Playerdf.loc[Playerdf.iloc[:,0] == 'Position:'][1].iloc[0]
    PlayerBirthPlace=Playerdf.loc[Playerdf.iloc[:,0] == 'Place of birth:'][1].iloc[0]
    
    
    
    
    
    
    
    df1=pd.DataFrame({
    "PlayerName" :[PlayerName],
    "PlayerHomeCOuntryName" :[PlayerName],
    "PlayerPosition" :[PlayerPosition],
    "Otherposlist" : [Otherposlist],
    "PlayerCitzenship" :[PlayerCitzenship],
    "PlayerBirthPlace" : [PlayerBirthPlace],
    "PlayerBirthDate" : [PlayerBirthDat]
    
    
    })
    df1['Otherposlist'] = pd.DataFrame([str(line).strip('[').strip(']') for line in df1['Otherposlist']])
    df1['Otherposlist'] = pd.DataFrame([str(line).replace("'","") for line in df1['Otherposlist']])
    return df1,res1,PlayerName

def get_transfer_value(s,res1,PlayerName):
    soup=bs(res1.text,'html.parser')
    scripts_tag=str(soup.find_all('script',{"type": "text/javascript"})[-1].string)
    
    #scripts_tag=scripts_tag.split('series')[1]
    
    content = scripts_tag[scripts_tag.find('{'): 1+scripts_tag.rfind('}')]
    
    try:
        content = content.split("var chart = new Highcharts.Chart(")[1].replace(');\n}','')
        content=content.replace("'",'"')
            
        fixed = content.encode('latin1').decode('unicode_escape')
        jsonread='{"series"'+fixed.split('"series"')[1].split(',"legend"')[0]+"}"
        jsonMVData=json.loads(jsonread)
        PlayerNameList=[]
        DateMV=[]
        MV=[]
        ClubMV=[]
        for i in jsonMVData['series'][0]['data']:
            PlayerNameList.append(PlayerName)
            DateMV.append(i['datum_mw'])
            if 'Th.' in i['mw']:
                 MV.append(i['mw'].replace('Th.',',000'))
            elif 'm' in i['mw']:
                MVres=locale.format("%d", float(i['mw'][1:].replace('m',''))*10**6, grouping=True)
                MV.append(str(i['mw'][0]+str(MVres)))
            else:
                MV.append(i['mw'])
            
            ClubMV.append(i['verein'])
        
        
        df2=pd.DataFrame({ 
            "PlayerName" : PlayerNameList,
            "DateMV" :DateMV,
            "MV" :MV,
            "ClubMV" :ClubMV
            
            })
    except:
        df2=pd.DataFrame({ 
            "PlayerName" : [PlayerName],
            "DateMV" :["NA"],
            "MV" :["0"],
            "ClubMV" :["NA"]
            })

    return df2


def run_output_convert_excel(s,SeasonList):
    for seasonYear in SeasonList:
        final_master(s,seasonYear)
        print(seasonYear)
    
    linksDone=pd.DataFrame({
    "links": already_scraped_links
    })

    linksDone.to_csv('linksDone.csv', mode='a', header=False)
    all_files = glob.glob(os.getcwd() + "/results/*.xlsx")

    li = []
    
    for filename in all_files:
        companyname=filename.split('results/')[1].split('.xlsx')[0]
        df6 = pd.read_excel(filename, index_col=None, header=0)
        li.append(df6)
    
    
    ResultsDaata = pd.concat(li, axis=0, ignore_index=True)
    idx = 0
    new_col = ResultsDaata['PlayerName'].fillna('') +ResultsDaata['DateMV'].fillna('').to_list()
    ResultsDaata.insert(loc=idx, column='Lookup', value=new_col)
    
    ResultsDaata.to_excel('Costa Rica Leage Players.xlsx')






run_output_convert_excel(s,SeasonList)











