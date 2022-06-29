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

from urllib.request import urlparse, urljoin
import requests
from bs4 import BeautifulSoup, SoupStrainer, Comment
from collections import deque, Counter
import pandas as pd
import os
import re
import numpy as np
import time
import json
import time
# from utilities_WB import test_proxies, get_proxies
import random
from datetime import datetime, timezone
from Scripts.Modules.utilities_WB import retry_acces



from fake_useragent import UserAgent
ua = UserAgent()

# Change the parser below to try out different options
# bs_parser = 'html.parser'
bs_parser = 'lxml'
# bs_parser = 'lxml-xml'
# bs_parser = 'html5lib'




class Scraper_wayback_machine_4point0:

    #%% Get host URL
    def get_host_of_url(self, url):
        url_temp = urlparse(url)
        host = ""
        if not url_temp.scheme:
            url_temp = urlparse(url,"http")
        #
        if url_temp.netloc:
            host = url_temp.scheme + "://"+ url_temp.netloc
        elif url_temp.path:
            host = url_temp.scheme + "://"+ url_temp.path

#        if force_www == True:
#            if not re.search("^www\.", urlparse(host).netloc, re.IGNORECASE): ### added 2019-03-20 ## bug n. 00001
#                url_temp = urlparse(host)
#                url_temp = url_temp._replace(netloc="www." + url_temp.netloc)
#                host = url_temp.scheme + "://"+ url_temp.netloc
#                #
        return host

    #%% Acces to url
    def acces_url(self, url, proxies = None, timeout_request = 5):
        try:
#            with requests.get('http://httpbin.org/get', stream=True) as r:
#                response_req = r
            headers = {'User-Agent': ua.random}
            response_req = requests.get(url, headers = headers, proxies = proxies, timeout = timeout_request)

        except Exception as e:
            response_req = "Error_detected: " + str(e)
#            print("Error_detected: acces URL %s : %s" %(url, str(e)))
        return response_req

    #%%  Initialize function
    def __init__(self, req_resp, host_cible, year_query_wayback_machine, offset_range_wayback_machine = 1, proxy = None, timeout = 5, number_retries = 3, *a, **kw):
        """


        Parameters
        ----------
        url : TYPE
            DESCRIPTION.
        host_cible : TYPE
            DESCRIPTION.
        root_query_wayback_machine : TYPE
            DESCRIPTION.
        year_query_wayback_machine : TYPE
            DESCRIPTION.
        offset_range_wayback_machine : int, optional
            Offset in years. The default is 2, which means 2 years offset.
        proxy : TYPE, optional
            DESCRIPTION. The default is None.
        timeout : TYPE, optional
            DESCRIPTION. The default is 25.

        Returns
        -------
        None.

        """

        ##
        headers = {'User-Agent': ua.random}


        acces_opened = False
        self.waybackmachine_domain = 'http://web.archive.org/'
        self.client = kw.get('client')
        self.coll = kw.get('coll')
        self.coll_error = kw.get('coll_error')
        self.time_sleep = kw.get('time_sleep')
        self.proxy = proxy
        self.n_retries_api = kw.get('n_retries_api')
        self.number_retries_parameter = number_retries
        self.number_retries_for_link = 0
        self.processing_url = req_resp.url

        self.timestamp_format_snapshot = '%Y%m%d%H%M%S'
        self.year_query_wayback_machine = year_query_wayback_machine
        self.host_cible = host_cible
        self.offset_range_wayback_machine = offset_range_wayback_machine
        self.timeout = timeout
        self.depth = 0
        self.count_size = 0

        self.opened_url = req_resp
        # while number_retries != 0 and acces_opened == False:
        #     number_retries = number_retries - 1
        #     self.opened_url = self.acces_url(url, proxies = { "http": proxy, "https": proxy}, timeout_request = timeout)

        #     if isinstance(self.opened_url, requests.models.Response):
        #         acces_opened = True
        #     time.sleep(0.05)
        ##
        # if isinstance(self.opened_url, requests.models.Response): ## added 2019-03-20 bug n. 00002
        #     self.url_host = self.opened_url.url
        # else:
        #     # self.url_host = self.get_host_of_url(url)
        #     self.url_host = ""

    #%% Download page from request
    def download_page(self, requests_response):
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

    #%% Get soup for BeautifulSoup
    def get_soup(self, page): #,  tag=None, attributes={}
        return BeautifulSoup(page, features="lxml") #, self.parser, parse_only=strainer)

    #%% Extract title
    def extract_title(self,soup):
        try:
            title = soup.title.text
        except:
            title = ""
        if title:
            return title
        else:
            return "No_title"

    #%% Get just visible element from page (NOT USED)
    # def visible(self, element):
    #     if element.name in ['style','script']:#.parent.name in ['style', 'script', '[document]', 'head', 'title']:
    #         return False
    #     elif re.match('<!--.*-->', str(element.encode('utf-8'))):
    #         return False
    #     return True

    # (NOT USED)
    def tag_visible(element):
        if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
            return False
        if isinstance(element, Comment) or re.match('<!--.*-->', str(element.encode('utf-8'))):
            return False
        return True


    #%%  Extract content from page (NOT USED)
    def extract_content(self, soup, titles_re = re.compile("^h[0-9]{1,2}"), paragraph_re = re.compile("^p$")):
        """
        This function was made by Davide in 2019
        """

        text_extracted = ""
        continue_while_exception = True
        while continue_while_exception:
            try:
                extracted_tag_element = soup.find(name = titles_re).parent.extract()
                ## remove script text (normally calling java)
                [s.extract() for s in extracted_tag_element('script')]
                text_extracted = "\n".join((text_extracted, extracted_tag_element.get_text(separator="\n"))) #.text.strip()))#
            except:
                continue_while_exception = False
        #
        continue_while_exception = True
        while continue_while_exception:
            try:
                extracted_tag_element = soup.find(name = paragraph_re).parent.extract()
                ## remove script text (normally calling java)
                [s.extract() for s in extracted_tag_element('script')]
                text_extracted = "\n".join((text_extracted, extracted_tag_element.get_text(separator="\n"))) #.text.strip()))#
            except:
                continue_while_exception = False


        #remove double multiliners
        text_extracted = re.sub("(\n )+|\n+","\n",text_extracted)
        title = self.extract_title(soup)
        return [title, text_extracted]
    #
    def text_from_soup(self, soup):
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
            ## dev
            ########################################################
            ############### DEPRECATED by Davide 2021-11-25
            # if reg_tags.search(t.name):
                # if t.find('a'):
                    # text_to_test = ''
                    # all_a_tag = t.findAll("a")
                    # for t_a_test in all_a_tag:
                        # text_to_test = text_to_test + t_a_test.text +'\n'
                    # if text_to_test.strip() == t.text.strip():
                        # # print(t.text)
                        # t = t.text.strip() ### modified by Davide 2021-11-25
                        # #continue ### modified by Davide 2021-11-25
                    # else:
                        # # t.text.strip()
                        # # print(t)
                        # t = t.text.strip()
                # else:
                    # t = t.text.strip()
            ########################################################
            t = t.text.strip() ### modified by Davide 2021-11-25
            if len(t) > 0:
                text = ""
                for line in t.splitlines():
                    line = line.strip()
                    if len(line) > 0:
                        text = text + ' '+ line  ### modified by Davide 2021-11-25
                clean_text = clean_text + '\n\n\n'+ text ### modified by Davide 2021-11-25 !!! important à verifier l'efficacité
        return title_text, clean_text

    #%% extract new links form page
    def extract_links(self, page):
        # url_host = self.get_host_of_url(self.url_host)
#        print(url_host)
        host_cible = self.host_cible
        if not page:
            return []
    #    links = re.compile('<a[^>]+href=["\'](.*?)["\']', re.IGNORECASE)
    #    return [urljoin(host, link) for link in links.findall(page)]
        #
        soup = BeautifulSoup(page, parse_only = SoupStrainer("a"), features="lxml")
#        test_scrap.opened_url.url

        # extract links:
        links_list_buffer = [link.attrs['href'] for link in soup.findAll("a") if 'href' in link.attrs]
#        print("links_list_buffer : %s"%str(links_list_buffer))
        # manipulate links
#        links_list_1 = [link for link in links_list_buffer if url_host in link and re.search("http", link)]
#        print("list1 : %s"%str(links_list_1))
#        links_list_buffer = [link for link in links_list_buffer if link not in links_list_1]
        links_list_2 = [link for link in links_list_buffer if re.search("http", link)]
        links_list_buffer = [link for link in links_list_buffer if link not in links_list_2]
        links_list_2 = [urljoin(host_cible, link) for link in links_list_2]
#        print("list2 : %s"%str(links_list_2))
        links_list_3 = [host_cible + "/"+ link if host_cible not in link else link for link in links_list_buffer]
#        print("list3 : %s"%str(links_list_3))
        regex_wayback = re.compile(self.waybackmachine_domain)
        final_list = [urljoin(self.waybackmachine_domain,x) if not regex_wayback.search(x) else x for x in links_list_2 + links_list_3 ]
#
        return final_list  #+links_list_1 #[urljoin(url_host, link)  for link in links_list ]

# page, url_opened.url
    #%% get new links form page
    def get_links(self, page_downloaded):
#        host_url = self.get_host_of_url(page_url)
        #
#        host = urlparse(host_url).netloc
        #
#        host = re.sub("www","",host)#re.sub("www\.","",host)
        #links =links_list
        links = self.extract_links(page_downloaded)
#        links = [link for link in links if host == urlparse(test_scrap.get_host_of_url(link)).netloc]
        #
#        len(host) + 24 # take first 19 (https://subdomains.) + 5 (marge) + len_character_host
#        host_len = len(host) + 15 # take first 12 (https://www.) + 3 (marge)
#        links = [link for link in links if host in link[:host_len]]
        ##
#        reseaux_sociaux_re = re.compile("google\.com|plus\.google\.|facebook\.|twitter\.|youtube\.|instagram\.|flickr\.|slideshare\.|linkedin\.")
#        links = [link for link in links if not reseaux_sociaux_re.search(link)]
        #
        return links
    #%% Methods to insert raw data into MongoDB
    def insert_into_mongo(self, soup, collection):
        title_text = "No title"
        text = "No text"
        # title_page_2fun, text_page_2_fun = self.extract_content(soup)
        title_text, text = self.text_from_soup(soup)
        # html = ""
        if text == "":
            # print("no text")
            error_message = 'No text found'
            # html = str(self.opened_url.content)
            self.insert_Errors_into_mongo(self.coll_error, error_message)
            return

        try:
            timetext = re.search("[0-9]{14}", self.opened_url.url).group()
            date_time_date = datetime.strptime(timetext, self.timestamp_format_snapshot)
            date_time_date = date_time_date.replace(tzinfo=timezone.utc)
        except:
            timetext = ""
            date_time_date = datetime.min
            date_time_date = date_time_date.replace(tzinfo=timezone.utc)
        try:
            downloaded_link = re.search("(" + self.host_cible +")"+"(.+|.?)[^/$]", self.opened_url.url).group()
        except:
            downloaded_link  = self.opened_url.url

        redirected = False
        last_processsed_url = self.processing_url
        if isinstance(self.opened_url,requests.models.Response):
            last_processsed_url = self.opened_url.url
            if len(self.opened_url.history) > 0:
                redirected = True

        year_query = int(self.year_query_wayback_machine[:4])


        query_update = {'$and':[{"website": self.host_cible}, {"downloaded_link": downloaded_link},{'year_query' : year_query},{'original_query_URL_wayback_machine': self.processing_url},  {'URL_wayback_machine_processed': last_processsed_url}]}

        value_update = {'$set':{"n_page_downloaded": self.count_size, "Title_page" : title_text, "TextContent": text, "snapshot_date_string": timetext, "snapshot_date":date_time_date,'year_obtained' : int(date_time_date.strftime("%Y")), 'Redirected': redirected, 'Depth': self.depth, 'n_retries_api' : self.n_retries_api, 'nretries_link': self.number_retries_for_link , 'lastModified': datetime.now()}} # to add in case we optmise proxy usage: 'proxy_used':self.proxy,
        ### ADDED DAVIDE 2022-06-20
        collection.update_one(query_update , value_update, upsert = True)

        ### DEPRECATED: DAVIDE 2022-06-20: insert each link downloaded without overriding
        ### insert each link downloaded without overriding
        # collection.insert_one({"website": self.host_cible, "downloaded_link": downloaded_link, "n_page_downloaded": self.count_size, "Title_page" : title_text, "TextContent": text, 'year_query': year_query , "snapshot_date_string": timetext, "snapshot_date":date_time_date,'year_obtained' : int(date_time_date.strftime("%Y")) ,'original_query_URL_wayback_machine': self.processing_url, 'Redirected': redirected, 'URL_wayback_machine_processed': last_processsed_url, 'Depth': self.depth, 'n_retries_api' : self.n_retries_api, 'nretries_link': self.number_retries_for_link , 'proxy_used':self.proxy}) #, "html":html

    def insert_Errors_into_mongo(self, collection, erro_mess):

        # title_page, text_page = self.extract_content(soup)
        redirected = False
        last_processsed_url = self.processing_url
        if isinstance(self.opened_url,requests.models.Response):
            last_processsed_url = self.opened_url.url
            if len(self.opened_url.history) > 0:
                redirected = True

        year_query = int(self.year_query_wayback_machine[:4])

        query_update = {'$and':[{"website": self.host_cible},{'year_query' : year_query},{'original_query_URL_wayback_machine': self.processing_url},  {'URL_wayback_machine_processed': last_processsed_url}]}

        value_update = {'$set':{'Redirected': redirected, 'Depth': self.depth, 'n_retries_api' : self.n_retries_api, 'nretries_link': self.number_retries_for_link , "error_message": erro_mess, 'lastModified': datetime.now()}} # to add in case we optmise proxy usage: 'proxy_used':self.proxy,

        ### ADDED DAVIDE 2022-06-20
        collection.update_one(query_update , value_update, upsert = True)

        ### DEPRECATED: DAVBIDE 2022-06-20: insert each link downloaded without overriding
        ### insert each link downloaded without overriding
        # collection.insert_one({"website": self.host_cible, 'original_query_URL_wayback_machine': self.processing_url, 'Redirected': redirected, 'URL_wayback_machine_processed': last_processsed_url, 'year_query': year_query, 'Depth': self.depth, 'n_retries_api' : self.n_retries_api, 'nretries_link': self.number_retries_for_link , 'proxy_used':self.proxy, "error_message": erro_mess})

    #%% Breadth-first search
    def breadth_first_search(self, limit_size = 50, depth_limit = 3, starting_level_scraping = 0):

        host_cible = self.host_cible
        #
        visited = set()
        exceptions = set()
        # count_over_depth = 0
        queue = deque()
        queue_level_index = deque()
        level_scraping = starting_level_scraping
        self.depth = level_scraping
        #%% Get content homepage
        try:
            visited.add(self.opened_url.url)
        except:
            print(f" error in visited.add {self.opened_url}")

        if isinstance(self.opened_url, str):
            error_message = self.opened_url
            # print("error_prim")
            self.insert_Errors_into_mongo(self.coll_error, error_message)

        elif isinstance(self.opened_url,requests.models.Response):
            page = self.download_page(self.opened_url)
            soup = self.get_soup(page)
            #%% update
            self.insert_into_mongo(soup, self.coll)

        #%% Extend link first level
        list_links_to_extend = [item for item, count in sorted(Counter(self.get_links(page)).items()) if item not in queue or item not in visited or item not in exceptions]


        queue.extend(list_links_to_extend)
        queue_level_index.extend([level_scraping + 1]*len(list_links_to_extend))

        #%% initialize exceptions
        list_exception = re.compile('\.pdf$|\.png$|\.jpg$|\.gif$|\.docx$|\.doc$|\.zip$|\.xls$|\.xlsx$|\.mp3$|\.mp4$', re.IGNORECASE)
        host_cible_exeption = re.compile(host_cible, re.IGNORECASE)

        products_exeption = re.compile("/products/", re.IGNORECASE)
        #
        while queue and self.count_size <= limit_size:

            # The following action is moved to another point : Davide 2022-06-15
            # self.count_size = self.count_size + 1

            url = queue.popleft()
            self.processing_url = url
            level_scraping = queue_level_index.popleft()
            self.depth = level_scraping

            ## test conditions
            if level_scraping > depth_limit:
                break
            try:
                url_visited_test = re.search("(" + host_cible+")"+"(.+|.?)[^/$]",url).group()
            except:
                url_visited_test = url
            if url_visited_test in visited:
                continue
            if url in exceptions:
                continue
            if list_exception.search(url):
                exceptions.add(url)
                continue
            if products_exeption.search(url):
                exceptions.add(url)
                continue
            if not host_cible_exeption.search(url):
                exceptions.add(url)
                continue

            #%% Check if url in date offset
            check_offset_year = False
            try:
                offset = self.offset_range_wayback_machine
                date = int(re.search("(/web/)([0-9]{8})([0-9]+)(/http)",url).groups()[1][:4])
                if int(self.year_query_wayback_machine[:4]) - offset <= date <= int(self.year_query_wayback_machine[:4]) + offset:
                    check_offset_year = True
                else:
                    check_offset_year = False
            except:
                check_offset_year = True

            if check_offset_year:
                # The following action is inserted here from the begging of the while block : Davide 2022-06-15
                # This assure us to count just some eligible new url to scrap
                self.count_size = self.count_size + 1

                # essaie x fois
                # number_retries = 0 #### DEPRECATED  : DAVIDE 2022-06-16
                acces_opened = False

                self.opened_url, self.number_retries_for_link = retry_acces(url, number_retries = self.number_retries_parameter, proxy = self.proxy, timeout_request = self.timeout, time_sleep = self.time_sleep)

# ======================= DEPRECATED : DAVIDE 2022-06-16 ======================
#                 while number_retries != self.number_retries and acces_opened == False:
#                     self.opened_url = self.acces_url(url, proxies = { "http": self.proxy, "https": self.proxy} , timeout_request = self.timeout)
#
#                     if isinstance(self.opened_url,requests.models.Response):
#                         acces_opened = True
#
#                     number_retries = number_retries + 1
#                     self.number_retries_for_link = number_retries
# =============================================================================

                if isinstance(self.opened_url,requests.models.Response):
                        acces_opened = True

                if not acces_opened:
                    #%% update
                    if isinstance(self.opened_url,requests.models.Response):
                        error_message = f'ACCES NOT EXCECUTED : {str(self.opened_url)} \t status code: {self.opened_url.status_code}'
                    elif isinstance(self.opened_url, str):
                        error_message = self.opened_url
                    # print("error: ACCES NOT EXCECUTED")
                    self.insert_Errors_into_mongo(self.coll_error, error_message)


                    #%% Add url in visited
                    try:
                        url_normalised = re.search("(" + host_cible+")"+"(.+|.?)[^/$]",url).group()
                        visited.add(url_normalised)
                    except:
                        visited.add(url)

                elif acces_opened:
                    page = self.download_page(self.opened_url)

                    #%% check if error in getting the html
                    if isinstance(page, str) and re.search("^Error_detected",str(page)):
#                        print("Error_detected during self.download_page(): url: %s \t %s" %(url, url_opened))
                        #
                        error_message = "Error_detected during self.download_page():" + str(self.opened_url.status_code)
                        #
                        self.insert_Errors_into_mongo(self.coll_error, error_message)
                        #
                         #%% Add url in visited
                        try:
                            url_normalised = re.search("(" + host_cible+")"+"(.+|.?)[^/$]",url).group()
                            visited.add(url_normalised)
                        except:
                            visited.add(url)
                        #%% go to next
                        continue

                    #%% get content if no error in dowloading the page
                    soup = self.get_soup(page)
                    self.insert_into_mongo(soup, self.coll)

                    #%% Add url in visited
                    try:
                        url_normalised = re.search("(" + host_cible+")"+"(.+|.?)[^/$]",url).group()
                        visited.add(url_normalised)
                    except:
                        visited.add(url)
                    #%% extend links next level
                    list_links_to_extend = [item for item, count in sorted(Counter(self.get_links(page)).items()) if item not in queue or item not in visited or item not in exceptions]
                    queue.extend(list_links_to_extend)
                    queue_level_index.extend([level_scraping + 1]*len(list_links_to_extend))


        return
