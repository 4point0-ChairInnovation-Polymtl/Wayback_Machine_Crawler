#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2022-06-27

@name : Wayback Machine Crawler

@version: 2.0

@author: Davide Pulizzotto, GitHub: puli83, E-mail : davide.pulizzotto@gmail.com

@credits: Mikaël Heroux-Vaillancourt (mikael.heroux-vaillancourt@polymtl.ca)

@license: MIT License Copyright (c) 2022 Davide Pulizzotto
"""

import os
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
import time
from datetime import datetime
from fake_useragent import UserAgent
ua = UserAgent()


# CONFIGS FOR FILE READING
#%% Divide file in several
def divideWebsiteFileInput(dirpath, filename, n_chunks = 3):
    ### developping ###
    filenameOnly, file_extension = os.path.splitext(filename)
    #print(filenameOnly, file_extension )

    path_temp = os.path.join(dirpath,"TEMP")
    if not os.path.exists(path_temp):
        print('Creating TEMP directory to store files for parallelisation {!r}'.format(path_temp))
        os.makedirs(os.path.realpath(path_temp))

    doc_size = 0
    with open(os.path.join(dirpath, filename)) as sites:
        for idx,url in enumerate(sites):
            doc_size = doc_size + 1
            # print(idx)

    if n_chunks == 1:
        limitforFile = doc_size
    else:
        limitforFile = int(doc_size / n_chunks)

    with open(os.path.join(dirpath, filename)) as sites:
        counter = 0
        index = 0
        n_file = 0
        file = open(os.path.join(path_temp, filenameOnly + str(counter) + file_extension), 'w')
        n_file = n_file +1
        for idx,url in enumerate(sites):
             url = url.strip("\n")
             url = url.strip()
             if index < limitforFile:
                 file.write(url+'\n')
                 index = index + 1
             elif index == limitforFile:
                 file.close()
                 # print("close file", str(counter))
                 counter = counter + 1
                 n_file = n_file +1
                 # open new wile with different counter
                 file = open(os.path.join(path_temp, filenameOnly + str(counter) + file_extension), 'w')
                 file.write(url + '\n')
                 index = 0

        ## end
        file.close()
        return n_file

#%% import file
def importWebsitesFiles(fileIndex, directoryfileimport, filename):
    # location = os.path.realpath(os.path.join(os.path.dirname(__file__), os.pardir))
    # print(f'gg {os.pardir}')
    # print(location)
    # os.getcwd()
    # location = os.path.join(location, directoryfileimport, "TEMP")
    location = os.path.join(directoryfileimport, "TEMP")
    filenameOnly, file_extension = os.path.splitext(filename)
    # print("location : "+location)
    # print("os.getcwd()" + os.getcwd())
    sitesFile = os.path.realpath(os.path.join(location, filenameOnly + str(fileIndex) + file_extension))

    # print("\nFile imported to procces \t {!r}\n".format(sitesFile))

    return sitesFile

# CONFIGS FOR PUTPUT IN FILE
#%% get folder output (NOT USED)
def getFolder_output():
    folder = ""
    return folder

#%% Get  Proxy
def get_proxies(n_proxy = 50, url = 'https://free-proxy-list.net/'):
    """

    Parameters
    ----------
    n_proxy : int, optional
        Number of proxyies to get. The default is 50.
    url : str, optional
        Database for proxyies with a table. The default is 'https://free-proxy-list.net/'.

    Returns
    -------
    proxies : list
        A list of proxies.

    """
    if url == "https://free-proxy-list.net/":

        response = requests.get(url)
        proxies = []
        soup = BeautifulSoup(response.content, features="lxml")
        table_item = soup.find("tbody")
        row_items = table_item.findAll("tr")
        row_ = row_items[0]
        for row_ in row_items[:n_proxy]:
            proxy = ":".join([x.text for x in row_.findAll("td")[:2]])
            proxies.append(proxy)

        return proxies
    # elif url == 'https://spys.one/en/free-proxy-list/':
    #     from selenium import webdriver
    #     from selenium.webdriver.common.action_chains import ActionChains
    #     from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
    #     from pyvirtualdisplay import Display

    #     response = requests.get(url)
    #     proxies = []
    #     soup = BeautifulSoup(response.content, features="lxml")
    #     table_item = soup.find("tbody")
    #     row_items = table_item.findAll("tr")
    #     row_ = row_items[0]
    #     for row_ in row_items[:n_proxy]:
    #         proxy = ":".join([x.text for x in row_.findAll("td")[:2]])
    #         proxies.append(proxy)

def test_proxies(proxies_temp, url = 'http://example.com/', timeout = 10):
    """
    Parameters
    ----------
    proxies_temp : list
        List of proxyies from get_proxies().
    url : str, optional
        DESCRIPTION. The default is 'http://example.com/'.
    timeout : int, optional
        timeout argument ofr request.get(). The default is 10.

    Returns
    -------
    checked_proxy : str
        This get first working proxy for an url.
    proxies_temp : list
        List of remaining proxyies to test

    """
    headers = {'User-Agent': ua.random}#headers = {"Accept
    checked_proxy = ""
    for i in tqdm(range(len(proxies_temp)), desc = "Processing until first working proxy..."):
        proxy = proxies_temp.pop(0)
        try:
            # print("test")
            req_resp =  requests.get(url, headers = headers, proxies = { "http": proxy, "https": proxy}, timeout = timeout)
            if req_resp.status_code == 200:
                checked_proxy = proxy
                break
        except:
            # print('no')
            continue
    return checked_proxy, proxies_temp, req_resp




def retry_acces(url, number_retries = 3, proxy = None, timeout_request = 5, time_sleep = 0):
    import requests
    import time
    def acces_url(url, proxies = None, timeout_request = 5):
        from fake_useragent import UserAgent
        ua = UserAgent()
        try:
#            with requests.get('http://httpbin.org/get', stream=True) as r:
#                response_req = r
            headers = {'User-Agent': ua.random}
            response_req = requests.get(url, headers = headers, proxies = proxies, timeout = timeout_request)

        except Exception as e:
            response_req = "Error_detected: " + str(e)
#            print("Error_detected: acces URL %s : %s" %(url, str(e)))
        return response_req

    acces_opened = False
    response_req = 'Initialisez response_req'
    number_retries_temp = 0
    while number_retries_temp != number_retries and acces_opened == False:

        response_req = acces_url(url, proxies = proxy, timeout_request = timeout_request)
        # response_req = requests.get(url_quering_API, headers =  {'User-Agent': ua.random}, timeout = 5)
        if isinstance(response_req, requests.models.Response) and response_req.status_code == 200:

            acces_opened = True
            return response_req, number_retries_temp

        number_retries_temp = number_retries_temp + 1
        time.sleep(time_sleep)

    return response_req, number_retries_temp


def text_from_soup(soup):
    """
    new funciton: Davide 2022-02-23

    Parameters
    ----------
    body : TYPE
        DESCRIPTION.

    Returns
    -------
    title_text : TYPE
        DESCRIPTION.
    clean_text : TYPE
        DESCRIPTION.

    """
    import re

    title_text = ""
    if soup.find("title"):
        title_text = soup.find("title").text
    reg_tags = re.compile("(^h[0-9]{1,2}$|^p$)")
    texts = soup.findAll(reg_tags)
    # deprecated
    # texts = soup.findAll('p',text=True)
    # texts = soup.findAll('p')
    # visible_texts = filter(tag_visible, texts)
    visible_texts = texts
    clean_text = ""
    for t in visible_texts:

        t = t.text.strip() ### modified by Davide 2021-11-25
        if len(t) > 0:
            text = ""
            for line in t.splitlines():
                line = line.strip()
                if len(line) > 0:
                    text = text + ' '+ line  ### modified by Davide 2021-11-25
            clean_text = clean_text + '\n\n\n'+ text ### modified by Davide 2021-11-25 !!! important à verifier l'efficacité
    return title_text, clean_text

def insert_Errors_into_mongo(host_cible, year_query, original_query_url, last_processsed_url, Depth, collection, erro_mess, number_retries_for_link):
    from datetime import datetime
    query_update = {'$and':[{"website": host_cible},{'year_query' : int(year_query)},{'original_query_URL_wayback_machine': original_query_url},  {'URL_wayback_machine_processed': last_processsed_url}]}

    value_update = {'$set':{'Depth' : Depth, 'nretries_link': number_retries_for_link, "error_message": erro_mess, 'lastModified': datetime.now()}}
    ### ADDED DAVIDE 2022-06-20
    collection.update_one(query_update , value_update, upsert = True)



def insert_into_mongo(requests_response, host_cible, year_query, original_query_url, last_processsed_url, collection, coll_err, number_retries_for_link, Depth):
    import re
    from datetime import datetime, timezone
    # Download page from request
    def download_page(requests_response):
        if isinstance(requests_response, requests.models.Response):
            if 'Content-Type' in requests_response.headers and re.search("text|hmtl|xml", requests_response.headers['Content-Type']):
                return requests_response.content#.decode('utf-8')
            elif 'Content-Type' in requests_response.headers and not re.search("text|hmtl|xml", requests_response.headers['Content-Type']):
#                print("Error_detected: in access url %s: NO valid Content-Type"%(requests_response.url))
                return "Error_detected: in access url : {str(requests_response.url)} \t status code: {str(requests_response.status_code)}"
            else:
                return requests_response.content#.decode('utf-8')
        else:
#            print("Error_detected: in access url : %s" %( requests_response))
            return "Error_detected: in access url : " + str(requests_response)

    # Get soup for BeautifulSoup
    def get_soup(page): #,  tag=None, attributes={}
        return BeautifulSoup(page, features="lxml") #, self.parser, parse_only=strainer)

    #####
    page = download_page(requests_response)
    soup = get_soup(page)
    #####
    timestamp_format = '%Y%m%d%H%M%S'
    title_text = "No title"
    text = "No text"
    ###
    title_text, text = text_from_soup(soup)
    ###
    if text == "":
        error_message = 'No text found'
        insert_Errors_into_mongo(host_cible = host_cible, year_query = year_query, original_query_url = original_query_url, last_processsed_url = last_processsed_url, Depth = Depth, collection = coll_err, erro_mess = error_message, number_retries_for_link = number_retries_for_link)
        return

    try:
        timetext = re.search("[0-9]{14}", last_processsed_url).group()
        date_time_date = datetime.strptime(timetext, timestamp_format)
        date_time_date = date_time_date.replace(tzinfo=timezone.utc)
    except:
        timetext = ""
        date_time_date = datetime.min
        date_time_date = date_time_date.replace(tzinfo=timezone.utc)
    try:
        downloaded_link = re.search("(" + host_cible +")"+"(.+|.?)[^/$]", last_processsed_url).group()
    except:
        downloaded_link  = last_processsed_url

    redirected = False

    if original_query_url != last_processsed_url:
        redirected = True


    year_query = int(year_query)

    query_update = {'$and':[{"website": host_cible}, {"downloaded_link": downloaded_link},{'year_query' : year_query},{'original_query_URL_wayback_machine': original_query_url},  {'URL_wayback_machine_processed': last_processsed_url}]}

    value_update = {'$set':{"Title_page" : title_text, "TextContent": text, "snapshot_date_string": timetext, "snapshot_date":date_time_date,'year_obtained' : int(date_time_date.strftime("%Y")), 'Redirected': redirected, 'Depth': Depth, 'nretries_link': number_retries_for_link , 'lastModified': datetime.now()}}
    ### ADDED DAVIDE 2022-06-20
    collection.update_one(query_update , value_update, upsert = True)



if __name__ == '__main__':

    proxies = get_proxies()
    # proxy_checked, proxies = test_proxies(proxies, url = 'http://web.archive.org/web/20180706120726/http://fazendadatoca.com.br:80/')

    # len(proxies)
    # requests.get(url, headers = headers, proxies = { "http": proxy_checked, "https": proxy_checked}, timeout = 5).status_code



    # proxies_checked = test_proxies(proxies, url = 'http://web.archive.org/web/20180706120726/http://fazendadatoca.com.br:80/')

    # requests.get(url, headers = headers, proxies = { "http": proxies_checked, "https": proxies_checked}, timeout = 5).status_code

    # requests.get(url, headers = headers,  timeout = 5).status_code

