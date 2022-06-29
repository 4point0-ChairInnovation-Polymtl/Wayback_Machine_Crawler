#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on 2022-06-27

@author: Davide Pulizzotto, GitHub: puli83, Mail : davide.pulizzotto@gmail.com

@credits: Mikaël Heroux-Vaillancourt (mikael.heroux-vaillancourt@polymtl.ca)

@license: MIT License Copyright (c) 2022 Davide Pulizzotto
"""

from pymongo import MongoClient
import pandas as pd
import os
import re

class GetErrorHomepages:

    def __init__(self,uri_string_connection, name_db, collection_name, collection_error_name, path_output, filename):
        #%% 1. Connect to Mongo
        # 1.1 Connect to Mongo client
        client = MongoClient(uri_string_connection)
        # 1.2 Connect to your database
        db = client[name_db]
        # 1.3 Connect to your collection of well webscraped data
        coll = db[collection_name]
        # 1.4 Connect to your collection that includes errors fromfirst round executed with 00_Run_multiprocess_crawler
        coll_err = db[collection_error_name]
        #%% 2. Query to MongoDB depending to your state of advancement in the cyclyng process of webscraping
        #%% 2.1 Get website have not been crawled at all. In this case, you need to run again the process 1 (oo_Run_multiprocess_crawler) on those websites.
        # We just get error flagged with website_not_scraped. ATTENTION: some website could have not been flagged with this variable and still not have been scrapped. This can arrive when we have acces to the homepage (so the website_not_scraped flag is not activated) but the homepage does not contain any text and/or any further url to download. It cvan alsso arrive that a homepage contains no text but contains further urls which have been correctly downloaded. All these phenomenon justify the difference of counts from initiali inputs and the outopuyt we get in this script. for exemple, yoiu can have 100 website to scrap, them after first round, you still need to scrap 10 webistes. Butm in your data colelction you still not have 12 webiste. This means for 2 websites Wayback Machine provided an acces to the homepage, but his not contains any text and any further url to download.
        df_list_website_to_crawl = pd.DataFrame(list(coll_err.find({ 'website_not_scrapped': True}, {'website':1, 'year_query':1})))
        self.n_homepage_to_crawl = len(df_list_website_to_crawl)
        #%% 2.2 write output
        if self.n_homepage_to_crawl > 0:
            # 2.2.2. write a file txt to feed next round using file 02_Run_next_rounds_crawler
            with open(os.path.join(path_output,filename),'w') as f:
                for idx, row in  df_list_website_to_crawl.iterrows() :
                    line_to_write = ', '.join([row.website, str(row.year_query)]) + '\n'
                    f.write(line_to_write)
        return

class GetErrorSimpleUrl:

    def __init__(self,uri_string_connection, name_db, collection_name, collection_error_name, path_output, filename):
        #%% 1. Connect to Mongo
        # 1.1 Connect to Mongo client
        client = MongoClient(uri_string_connection)
        # 1.2 Connect to your database
        db = client[name_db]
        # 1.3 Connect to your collection of well webscraped data
        coll = db[collection_name]
        # 1.4 Connect to your collection that includes errors fromfirst round executed with 00_Run_multiprocess_crawler
        coll_err = db[collection_error_name]
        #%% 2. Get error not equal to 'No text found' and containing 40x HTTP error, in order to generate an imput file containing a list of wayback machine urls
        Depth = 1
        #
        df_list_urls_error = pd.DataFrame(list(coll_err.find({'$and':
                                                              [
                                                                  {'error_message':{'$ne':'No text found'}},
                                                                  {'error_message':{'$not':{'$regex':'status_code : 40[0-9]'}}},
                                                                  {'website_not_scrapped' : {'$exists' : False}},
                                                                  {'Depth' : Depth}
                                                               ]
                                                              },

                                                             ##### we need to get the website, year_query and url
                                                             {'website' : 1,
                                                              'year_query' : 1,
                                                              'original_query_URL_wayback_machine' : 1,
                                                              'URL_wayback_machine_processed':1,
                                                              # 'n_page_downloaded' : 1,
                                                              'Depth': 1})))

        self.n_url_to_scrap = len(df_list_urls_error)
        #%% 2.2 write output
        if self.n_url_to_scrap > 0:
            #%% 3. write output
            # 3.1. write a file txt to feed next round using file 02_Run_next_rounds_crawler
            with open(os.path.join(path_output,filename),'w') as f:
                for idx, row in  df_list_urls_error.iterrows() :
                    line_to_write = ', '.join([row.website, str(row.year_query), re.sub(' +|\n', '',row.original_query_URL_wayback_machine), re.sub(' +|\n', '',row.URL_wayback_machine_processed), str(row.Depth)]) + '\n'
                    f.write(line_to_write)
        return

if __name__ == '__main__':
    print()
# #%% 0. Import libraies
# from pymongo import MongoClient
# import pandas as pd
# import os
# # import re

# if __name__ == '__main__':
#     print("Working directory: " + os.getcwd())

#     #%% 1. Connect to Mongo
#     # 1.1 Connect to Mongo client
#     client = MongoClient()
#     # 1.2 Connect to your database
#     db = client.TEN_V6
#     # 1.3 Connect to your collection of well webscraped data
#     coll = db['data']
#     # 1.4 Connect to your collection that includes errors fromfirst round executed with 00_Run_multiprocess_crawler
#     coll_err = db['data_err']

#     #%% 2. Query to MongoDB depending to your state of advancement in the cyclyng process of webscraping
#     #%% 2.1 Get website have not been crawled at all. In this case, you need to run again the process 1 (oo_Run_multiprocess_crawler) on those websites.
#     # We just get error flagged with website_not_scraped. ATTENTION: some website could have not been flagged with this variable and still not have been scrapped. This can arrive when we have acces to the homepage (so the website_not_scraped flag is not activated) but the homepage does not contain any text and/or any further url to download. It cvan alsso arrive that a homepage contains no text but contains further urls which have been correctly downloaded. All these phenomenon justify the difference of counts from initiali inputs and the outopuyt we get in this script. for exemple, yoiu can have 100 website to scrap, them after first round, you still need to scrap 10 webistes. Butm in your data colelction you still not have 12 webiste. This means for 2 websites Wayback Machine provided an acces to the homepage, but his not contains any text and any further url to download.
#     df_list_website_to_crawl = pd.DataFrame(list(coll_err.find({ 'website_not_scrapped': True}, {'website':1, 'year_query':1})))



#     # 3rd round last 215 website
#     # 4th round 103


#     # df_list_website_to_crawl = pd.DataFrame(list(coll_err.find({ 'Depth': 0}, {'website':1, 'year_query':1})))
#     # # 2.2 remove duplicated retived from the collection including errors
#     # df_list_website_to_crawl = df_list_website_to_crawl[['website', 'year_query']]
#     # df_list_website_to_crawl = df_list_website_to_crawl.drop_duplicates()

#     # # test find special error for home page (webiste for which we had acces but any page was correctly downloaded
#     # test_special_error =  pd.DataFrame(list(coll_err.find({'$and':[{'Depth':0},{ 'website_not_scrapped': {'$exists': 0}}]}, {'website':1, 'year_query':1})))
#     # test_special_error =test_special_error[['website', 'year_query']]
#     # test_special_error=test_special_error.drop_duplicates()



#     # df_data =  pd.DataFrame(list(coll.find({},{'website':1, 'year_query':1})))
#     # df_data = df_data[['website', 'year_query']]
#     # df_data = df_data.drop_duplicates()
#     # # 3.3 Remove from df_list_urls_error those urls whoch have been well webscrapped
#     # df_list_urls_error = df_list_urls_error.loc[~df_list_urls_error['URL_wayback_machine'].isin(list(df_list_urls_data.URL_wayback_machine))]


#     #%% 2.2 write output
#     # 2.2.1. Determine the directory and the file name to write your output. ATTENTION: CHANGE THE NAME OF YOUR FILE AT EACH NEW ROUND
#     path_output = '/Volumes/T7/Research/Waybackmachine_Scraper/fourPOINTzero_scraper_wayback_machine/Import'
#     filename = 'data_TEN_V6_4th_homepage_round.txt'
#     # 2.2.2. write a file txt to feed next round using file 02_Run_next_rounds_crawler
#     with open(os.path.join(path_output,filename),'w') as f:
#         for idx, row in  df_list_website_to_crawl.iterrows() :
#             line_to_write = ', '.join([row.website, str(row.year_query)]) + '\n'
#             f.write(line_to_write)




