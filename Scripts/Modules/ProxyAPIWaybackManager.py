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
import re
import requests
import tldextract
import random
from datetime  import datetime
from tqdm import tqdm
import logging
import time
from fake_useragent import UserAgent
ua = UserAgent()
import json
from Scripts.Modules.utilities_WB import test_proxies, get_proxies, importWebsitesFiles, retry_acces
from Scripts.Modules.Scraper_wayback_machine_4point0_two import Scraper_wayback_machine_4point0 as scr4

root_query_wayback_machine = "http://archive.org/wayback/available?url="


class ProxyAPIWaybackManager:
#     def acces_url(self, url, proxies = None, timeout_request = 5):
#         try:
# #            with requests.get('http://httpbin.org/get', stream=True) as r:
# #                response_req = r
#             headers = {'User-Agent': ua.random}
#             response_req = requests.get(url, headers = headers, proxies = proxies, timeout = timeout_request)

#         except Exception as e:
#             response_req = "Error_detected: " + str(e)
# #            print("Error_detected: acces URL %s : %s" %(url, str(e)))
#         return response_req

#     def retry_acces(self, url, number_retries = 3, proxy = None, timeout_request = 5):
#         acces_opened = False
#         response_req = 'Initialisez response_req'
#         number_retries_temp = 0
#         while number_retries_temp != number_retries and acces_opened == False:

#             response_req = self.acces_url(url, proxies = proxy, timeout_request = timeout_request)
#             # response_req = requests.get(url_quering_API, headers =  {'User-Agent': ua.random}, timeout = 5)
#             if isinstance(response_req, requests.models.Response) and response_req.status_code == 200:

#                 acces_opened = True
#                 return response_req, number_retries_temp

#             number_retries_temp = number_retries_temp + 1

#         return response_req, number_retries_temp

    def insert_Errors_into_mongo(self, host_cible, processing_url, year_query,  collection, erro_mess):

        # title_page, text_page = self.extract_content(soup)
        query_update = {'$and':[{"website": host_cible}, {'year_query' : int(year_query)}]}

        value_update = {'$set':{'original_query_URL_wayback_machine': processing_url, 'Depth': 0, "error_message": erro_mess, 'website_not_scrapped' : True, 'lastModified': datetime.now()}}
        ### ADDED DAVIDE 2022-06-20
        collection.update_one(query_update , value_update, upsert = True)

        ### DEPRECATED: DAVBIDE 2022-06-20: insert each link downloaded without overriding
        # collection.insert_one({"website": host_cible, 'year_query' : int(year_query),  'original_query_URL_wayback_machine': processing_url, 'Depth': 0, "error_message": erro_mess, 'website_not_scrapped' : True})

    #%%  Initialize function
    def __init__(self, fileIndex, dirimport, filename, *a, **kw):
        """
        This class manages proxies rotating from a free list exrtacted here: , and execute query to WaybackMachine to get closet url from a year target. This class has been developped to work in parellel, called by another function from multiprocessing package.

        Parameters
        ----------
        fileIndex : int
            Index number of files to process in parallel.
        dirimport : str
            Directory to import.
        filename : int
            Name of file to import.
        *a : TYPE
            DESCRIPTION.
        **kw : TYPE
            DESCRIPTION.

        Returns
        -------
        None.

        """
        # self.time_start = datetime.now()
        # # initialize proxies
        # proxies = get_proxies(n_proxy = 300)
        # random.shuffle(proxies)
        #
        n_retries_parameter = kw.get('settings')['N_retries_for_url']
        timeout_request = kw.get('settings')['timeout_request']
        limit_size = kw.get('settings')['limit_size']
        depth_limit = kw.get('settings')['depth_limit']
        year_range = kw.get('settings')['year_range']
        time_sleep = kw.get('settings')['time_sleep']


        sites_file = importWebsitesFiles(fileIndex, dirimport, filename)
        with open(sites_file) as sites:
            # Verifier le buffer des urls et la liste d'allowed domains
            # urls = []
            # allowedDomains = []
            for row in tqdm(sites, desc = "Processing urls in file..."):
                row = row.strip("\n")
                row = row.strip()
                url, year = row.split(', ')
                ## debugging
                # url, year = ('travelnordegg.com','2012')
                ##
                ### following lines under evaluation (probably deprecated : Davide 2022-06-17)
                # ext = tldextract.extract(url)
                # domain = ext.domain + "." + ext.suffix
                domain = url

                ## 01: execute query on API of WaybackMachine
                year_api = ''.join((str(year), "0701"))
                url_quering_API = ''.join((root_query_wayback_machine, url, "&timestamp=", year_api))
                #
                #%% Get closest avialiable link from API waybackmachine
                acces_opened = False
                dict_temp_quering_wayback = None
                response_req, number_retries = retry_acces(url_quering_API, number_retries = n_retries_parameter, proxy = None, timeout_request = timeout_request)
                # print(acces_opened)
                if isinstance(response_req,requests.models.Response):
                    if re.search("\{", response_req.text):
                        dict_temp_quering_wayback = None
                        dict_temp_quering_wayback = json.loads(response_req.text)
                        if 'archived_snapshots' in dict_temp_quering_wayback.keys():
                            if 'closest' in dict_temp_quering_wayback['archived_snapshots'].keys():
                                if 'url' in dict_temp_quering_wayback['archived_snapshots']['closest'].keys():
                                    acces_opened = True
                                else:
                                    ## testion:
                                    print('noooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo')
                                    # logging.info("File name logging : {n_log}\tIndex of website : {ind}".format(n_log = prefix_name_file_log, ind = str(index_)))
                                    # logging.info("Output response %s\tNumber of retries %i"%(response_req.text, number_retries))


                #%% Check resopinse after several retries
                if not isinstance(response_req,requests.models.Response):
                    erro_mess = "Url not found in wayback machine {url}\t Url for query : {query_}".format(url = url, query_ = url_quering_API)
                    logging.info(erro_mess)
                    print(erro_mess)

                    self.insert_Errors_into_mongo(host_cible = domain, year_query = year, processing_url = url_quering_API, collection = kw.get('coll_error'), erro_mess = erro_mess)

                    continue

                if acces_opened == False:
                    erro_mess = f"ID company not processed because empty or erroneous response from Wayback machine AP. Error for the following url: {url}; WaybackMachine query : {url_quering_API}; Response API Wayback Machine :{response_req.text}"
                    logging.info(erro_mess)
                    self.insert_Errors_into_mongo(host_cible = domain, year_query = year, processing_url =url_quering_API, collection = kw.get('coll_error'), erro_mess = erro_mess)

                    continue

                #%% START SCRAPING PROCESS

                logging.info(f"WAYBACK MACHINE ANSWER for {url}: {response_req.text}")

                #%% get closet url
                # url peut ne peut etre presenrt meme si wayback machine donne une reponse 200
                self.url_for_scraping = dict_temp_quering_wayback['archived_snapshots']['closest']['url']

                #%% Get proxies if more then 10 minutes elapsed
                #%% Developping: Make a pause each 10 minutes?
                # t_elapsed = datetime.now() - self.time_start
                # if t_elapsed.seconds > 600:
                #     proxies = get_proxies(n_proxy = 300)
                #     random.shuffle(proxies)
                #     self.time_start = datetime.now()
                #%% get one working proxy
                # proxy_checked, proxies, req_resp = test_proxies(proxies, url = url_for_scraping) #doing a test before force to requering agaion the same url when scr4 is initialized (next line of coede)
                # if not proxy_checked: # if not proxy checked, get proxies again
                #     proxies = get_proxies(n_proxy = 300)
                #     random.shuffle(proxies)
                #     self.time_start = datetime.now()
                #     proxy_checked, proxies, req_resp = test_proxies(proxies, url = url_for_scraping)
                # print(url_for_scraping)
                acces_opened = False
                erro_mess = 'Initialized message error'
                self.req_resp, number_retries = retry_acces(self.url_for_scraping, number_retries = n_retries_parameter, proxy = None, timeout_request = timeout_request)

                if isinstance(self.req_resp,requests.models.Response) and self.req_resp.status_code == 200 and self.req_resp.url == 'https://web.archive.org/429.html':
                    time.sleep(10)
                    self.req_resp, number_retries = retry_acces(self.url_for_scraping, number_retries = n_retries_parameter, proxy = None, timeout_request = timeout_request)

                if isinstance(self.req_resp,requests.models.Response) and self.req_resp.status_code == 200 and self.req_resp.url != 'https://web.archive.org/429.html':
                    acces_opened = True
                elif isinstance(self.req_resp,requests.models.Response) and self.req_resp.status_code == 200 and self.req_resp.url == 'https://web.archive.org/429.html':
                    erro_mess = f'Domain not scraped at all because we do not access to the closest url returned by WaybackMachine API, Error 429 : {self.req_resp.url}'
                elif isinstance(self.req_resp,requests.models.Response) and self.req_resp.status_code != 200:
                    erro_mess = f'Domain not scraped at all because we do not access to the closest url returned by WaybackMachine API, status_code: {self.req_resp.status_code}'
                elif isinstance(self.req_resp,str):
                    erro_mess = self.req_resp

                if acces_opened :
                    proxy_checked = None
                    # remove docuemnt in error collection if present
                    output_delete_mongo = kw.get('coll_error').delete_one({'$and':[{'website':domain},{'year_query':int(year)}]})
                    if output_delete_mongo.deleted_count == 1:
                        logging.info(f'A docuument was removed from error collection responding to this query : {domain}, {year}')
                    #%% Initialize scrapeur with homepage
                    WB_scraper = scr4(self.req_resp, host_cible = domain, year_query_wayback_machine = year_api, offset_range_wayback_machine = year_range, proxy = proxy_checked, timeout = timeout_request, client = kw.get('client'), coll = kw.get('coll'), coll_error = kw.get('coll_error'), n_retries_api = number_retries, number_retries = n_retries_parameter, time_sleep = time_sleep)
                    WB_scraper.breadth_first_search(limit_size = limit_size, depth_limit = depth_limit, starting_level_scraping = 0)
                else:
                    # erro_mess = 'Domain not scraped at all because we do not access to the closest url returned by WaybackMachine API'
                    self.insert_Errors_into_mongo(host_cible = domain, year_query = year, processing_url = self.url_for_scraping, collection = kw.get('coll_error'), erro_mess = erro_mess)
#


















