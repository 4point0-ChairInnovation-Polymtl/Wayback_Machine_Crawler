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
#%% import libraries and modules
import os
# from utilities_WB import test_proxies, get_proxies
# from Scripts.Scraper_wayback_machine_4point0_two import Scraper_wayback_machine_4point0 as scr4
import multiprocessing
import logging
import sys
import shutil
import time
import datetime
import requests
import re
import argparse
from tqdm import tqdm
import Scripts.Configurations.configs as configs
from Scripts.Modules.ProxyAPIWaybackManager import ProxyAPIWaybackManager
from Scripts.Modules.utilities_WB import divideWebsiteFileInput, importWebsitesFiles, retry_acces, insert_Errors_into_mongo, insert_into_mongo
from Scripts.Modules.GetErrors import GetErrorHomepages, GetErrorSimpleUrl


#%% Definition spider for crawling homepages on wayback machine after have asking the main url to the API of Wayback Machine Archive
def run_spider(index, settings, stringdirimport, filename):
    client = configs.connectClientMongoDB(uri_string_connection = settings['mongodb_uri_string_connection'])
    db = configs.connectDataBase(client, name_db = settings['mongodb_database_name'])
    coll = configs.connectCollectionClient(db, collection_name = settings['mongodb_collection_name'])
    coll_error = configs.connectCollectionOnError(db, collection_error_name = settings['mongodb_collection_error_name'])

    # process = CrawlerProcess(settings)
    settings['LOG_FILE'] = settings['LOG_FILE'] + '_' + str(index) +'.log'

    logging.basicConfig(filename = settings['LOG_FILE'], format='%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s', level = logging.DEBUG)
    # print(settings['LOG_FILE'])
    # print(coll)
    logging.info("\n\n\n\n\n\nSpider "+str(index)+" START\n\n\n\n\n\n")
    print("Spider "+str(index)+" START at %s"%str(datetime.datetime.now()))

    ProxyAPIWaybackManager(fileIndex = index, settings = settings, dirimport = os.path.join(os.getcwd(),stringdirimport), filename = filename, client = client, coll = coll, coll_error = coll_error)

    logging.info("\n\n\n\n\n\nSpider "+str(index)+" END\n\n\n\n\n\n")
    print("Spider "+str(index)+" END at %s"%str(datetime.datetime.now()))
    return

#%% Definition spider for scraping single pages after have getting erros from previous crawler
def run_spider_single_link(index, settings, stringdirimport, filename):
    client = configs.connectClientMongoDB(uri_string_connection = settings['mongodb_uri_string_connection'])
    db = configs.connectDataBase(client, name_db = settings['mongodb_database_name'])
    coll = configs.connectCollectionClient(db, collection_name = settings['mongodb_collection_name'])
    coll_error = configs.connectCollectionOnError(db, collection_error_name = settings['mongodb_collection_error_name'])
    waybackmachine_domain = 'http://web.archive.org/'

    # process = CrawlerProcess(settings)
    settings['LOG_FILE'] = settings['LOG_FILE'] + str(index) +'.log'


    timeout_request = settings['timeout_request']
    time_sleep = settings['time_sleep']
    n_retries = settings['N_retries_for_url']


    logging.basicConfig(filename = settings['LOG_FILE'], format='%(asctime)-15s %(name)-5s %(levelname)-8s %(message)s', level = logging.DEBUG)
    # print(settings['LOG_FILE'])
    # print(coll)
    logging.info("\n\n\n\n\n\nSpider "+str(index)+" START\n\n\n\n\n\n")
    print("Spider "+str(index)+" START at %s"%str(datetime.datetime.now()))

    sites_file = importWebsitesFiles(index, stringdirimport, filename)
    with open(sites_file) as sites:
        # dev
        # url = 'http://web.archive.org/web/20120711125714/http://www.choratech.com/about'
        ##
        for row in tqdm(sites, desc = "Processing urls in file..."):
            ################
            # DEV
            ################
            row = row.strip("\n")
            row = row.strip()
            website, year_query, original_url, processed_url, Depth = row.split(', ')


            ######### deprecated : Davide 2022-06-17
            # try:
            #     domain = re.search("(/web/)([0-9]{8})([0-9]+/)(http.+)",url).groups()[3]
            # except:
            #     domain = re.sub(waybackmachine_domain, '', url)

            # ext = tldextract.extract(domain)
            # domain = ext.domain + "." + ext.suffix
            domain = website
            ## 01: execute request
            acces_opened = False
            #### Acces to url
            req_reponse, nretries = retry_acces(processed_url, number_retries = n_retries, timeout_request = timeout_request, time_sleep = time_sleep)

            #### Check resopinse after several retries
            erro_mess = 'Initialized message error'

            if isinstance(req_reponse,requests.models.Response) and req_reponse.status_code == 200 and req_reponse.url == 'https://web.archive.org/429.html':
                time.sleep(10)
                req_reponse, nretries = retry_acces(processed_url, number_retries = n_retries, timeout_request = timeout_request, time_sleep = time_sleep)

            if isinstance(req_reponse,requests.models.Response) and req_reponse.status_code == 200 and req_reponse.url != 'https://web.archive.org/429.html':
                acces_opened = True
            elif isinstance(req_reponse,requests.models.Response) and req_reponse.status_code == 200 and req_reponse.url == 'https://web.archive.org/429.html':
                erro_mess = f'Domain not scraped at all because we do not access to the closest url returned by WaybackMachine API, Error 429 : {req_reponse.url}'
            elif isinstance(req_reponse,requests.models.Response) and req_reponse.status_code != 200:
                erro_mess = f'Error_from_request : status_code : {req_reponse.status_code}'
            elif isinstance(req_reponse, str):
                erro_mess = f'Error_retry_acces : message : {str(req_reponse)}'


            # Generate output:
            if acces_opened == False:
                logging.info(f"Url not downloaded : with error message: {erro_mess}")
                insert_Errors_into_mongo(host_cible = domain, year_query = year_query, original_query_url = original_url, last_processsed_url = processed_url, Depth = int(Depth),  collection = coll_error, erro_mess = erro_mess, number_retries_for_link = nretries)

            elif acces_opened == True:

                query_delete = {'$and':[{"website": domain},{'year_query' : int(year_query)},{'original_query_URL_wayback_machine': original_url},  {'URL_wayback_machine_processed': processed_url}]}
                output_delete_mongo = coll_error.delete_one(query_delete)
                if output_delete_mongo.deleted_count == 1:
                    logging.info(f'A document was removed from error collection responding to this query : website: {domain}, year_query : {int(year_query)}, original_query_URL_wayback_machine: {original_url},  URL_wayback_machine_processed: {processed_url} ')

                insert_into_mongo(requests_response = req_reponse, host_cible = domain, year_query = year_query,  original_query_url = original_url, last_processsed_url = processed_url, collection = coll, coll_err = coll_error, number_retries_for_link = nretries, Depth = int(Depth))


                ##### TRUST ME, IT DID NOT WORK: mais pour 2 depth ou moins c'est correct (depth = 1 -> OK) si on veut regler le problùeme du septh = 1 + x, il faut changer completement le script de moissonage. Voici les op.érations du nouveau outils de moissonage
                # 1) saturer homepage avec un script de SINGLE link downloading (without brwedth_first_search algorithm) et la requete ùas l'API de waybacvh contenue dfans ProxyAPIWaybackMachine. Dans la base de données, sauvegarder le HTML pour venir moissoner le depth 1. Creation d'une colleciton metadata oùu enregistrer et updater le nom,bre de page realemment téléharger, afin d"optimiser les tourt subsequent.
                # 2) saturer depth 1 with previous script et en prenant le html du depth 0 ùa parttir de mongo (afgin d'évite de faire trop de requete oour ke meme lien). sauvegarder le html pour prochain depth
                # 3)  saturer le depth 2 avec a mPeme logique du pont 2)
                # 4) repter jusuqu'a depth == depth_limit
# =============================================================================
#                 # èèè developpement
#                 if crawl == True:
#                     # Initialize scrapeur with specific starting depth level
#                     year_api = year_query + '0701'
#                     WB_scraper = scr4(req_reponse, host_cible = domain, year_query_wayback_machine = year_api, offset_range_wayback_machine = year_range, timeout = timeout_request, client = client, coll = coll, coll_error = coll_error, n_retries_api = n_retries_api, number_retries = n_retries, time_sleep = time_sleep)
#                     WB_scraper.breadth_first_search(limit_size = limit_size, depth_limit = depth_limit, starting_level_scraping = 0)
#
#                 else:
#                     insert_into_mongo(requests_response = req_reponse, host_cible = domain, year_query = year_query,  original_query_url = original_url, last_processsed_url = processed_url, collection = coll, coll_err = coll_error, number_retries_for_link = nretries, Depth = int(Depth), n_retries_api = int(n_retries_api))
#
# =============================================================================


    logging.info("\n\n\n\n\n\nSpider "+str(index)+" END\n\n\n\n\n\n")
    print("Spider "+str(index)+" END at %s"%str(datetime.datetime.now()))
    return

#%% run main crawler in a multiprocess
def run_multiprocess(dirpath, filename, nprocess, settings):
    n_file = divideWebsiteFileInput(dirpath, filename, n_chunks = nprocess)
    print("Divided import file in %i files to activate multiple spiders in parallel."%n_file)

    jobs = []

    stringdirimport = dirpath
    # print(stringdirimport)

    ### ATTENTION: je crois que avoir un log generale c.est un doublon, pas necessaire. Davide 2022-02-23
    #logging.basicConfig(filename=os.path.join(dirlogging,'LOG_General_' + filenameOnly + '.log'), format='%(asctime)s %(message)s', level=logging.DEBUG)

    # string_to_print = 'Start processing file : %s \n\n'%filename
    print('START PROCESSING file %s'%filename)
    # index = 0
    # run_spider(index, settings, stringdirimport, filename, client, coll, coll_error)

    for index in range(n_file):
        # print(i)
        p = multiprocessing.Process(target=run_spider, args=(index, settings, stringdirimport, filename))
        jobs.append(p)
        p.start()

    # the following code allows to continue the scirpt until all process end
    for j in jobs:
        j.join()
        print(j)

    shutil.rmtree(os.path.join(dirpath,"TEMP"))

#%% run single page scraipng in a multiprocess
def run_multiprocess_single_link(dirpath, filename, nprocess, settings):
    n_file = divideWebsiteFileInput(dirpath, filename, n_chunks = nprocess)
    print("Divided import file in %i files to activate multiple spiders in parallel."%n_file)

    jobs = []

    stringdirimport = dirpath
    # print(stringdirimport)

    ### ATTENTION: je crois que avoir un log generale c.est un doublon, pas necessaire. Davide 2022-02-23
    #logging.basicConfig(filename=os.path.join(dirlogging,'LOG_General_' + filenameOnly + '.log'), format='%(asctime)s %(message)s', level=logging.DEBUG)

    # string_to_print = 'Start processing file : %s \n\n'%filename
    print('START PROCESSING file %s'%filename)
    # index = 0
    # run_spider(index, settings, stringdirimport, filename, client, coll, coll_error)

    for index in range(n_file):
        # print(i)
        p = multiprocessing.Process(target=run_spider_single_link, args=(index, settings, stringdirimport, filename))
        jobs.append(p)
        p.start()

    # the following code allows to continue the scirpt until all process end
    for j in jobs:
        j.join()
        print(j)

    shutil.rmtree(os.path.join(dirpath,"TEMP"))


if __name__ == '__main__':
    print("Working directory: " + os.getcwd())

    parser = argparse.ArgumentParser(prog='Wayback_Machine_Crawler', description='Description of your program')
    parser.add_argument('-i','--pathtofile', help='Insert the entire path to import file (i.e.: Import/data.txt). The data file have to be a .txt and each row contains the domain of a website to crawl and the year for which look for, separated by comma-space ", " (i.e.: example.com, 2018). (required: %(required)s)', required=True)
    parser.add_argument('-uri','--uri_mongo_connection', help='Insert connection to MongoDb in the String URI Format. For localhost use the following string: "mongodb://localhost". More information here: https://www.mongodb.com/docs/manual/reference/connection-string/. (required: %(required)s)', required=True)
    parser.add_argument('-db','--mongodb_database_name', help='Insert the name of the MongoDB database into which create collections (required: %(required)s)', required=True)
    parser.add_argument('-coll','--mongodb_collection_name', help='Insert the name of the collection into which insert documents during the scraping (required: %(required)s)', required=True)
    parser.add_argument('-coll_err','--mongodb_collection_error_name', help='Insert the name of the collection into which insert errors occurred during the scraping (required: %(required)s)', required=True)
    parser.add_argument('-log','--dirlogging', help='Insert the path to folder in which save logging (default: %(default)s). ', default='LOG')
    parser.add_argument('-np','--number_parallel_process', help='Insert the number of parallelisation to execute (type: %(type)s; default: %(default)s)', default=2, type = int)
    parser.add_argument('-nr','--number_retries_acces_url', help='Insert the number of times to retry to acces to a single url. Bigger is this number, longer is the crawlong process, but less error can occurs (type: %(type)s; default: %(default)s)', default=3, type = int)
    parser.add_argument('-time_sleep','--time_sleep', help='Insert the seconds to wait before executing a new retry (this is only used in the correction error process. Bigger is this value, longer is the correction crawling process, but more reliable it becomes. (type: %(type)s; default: %(default)s)', default=0.1, type = float)
    parser.add_argument('-timeout','--timeout_request', help='Insert the number of second to wait to receive the answer from server (this is the request module argument). (type: %(type)s; default: %(default)s)', default=10, type = int)
    parser.add_argument('-npages','--limit_number_pages_for_each_website', help='Insert the limit of pages to download for each domain. Bigger is this value, longer is the crawling process. (type: %(type)s; default: %(default)s)', default=20, type = int)
    parser.add_argument('-depth','--depth_to_crawl', help='Insert the maximum depth that will be allowed to crawl for any domain. Bigger is this value, longer is the crawling process. IN THIS VERSION OF THE %(prog)s is not possible to crawl more then depth = 1. (type: %(type)s; default: %(default)s)', default=1, type = int)
    parser.add_argument('-year_range','--year_range', help='Insert the time range into which looking for in year unit. For example, if your entry is "example.com, 2018" and your -year_range is 1, %(prog)s allow to search between 2017 and 2019. (type: %(type)s; default: %(default)s)', default=1, type = int)

    #%%## initialise arguments
    args = vars(parser.parse_args())
    #
    dirpath, filename = os.path.split(args['pathtofile'])
    uri_string = args['uri_mongo_connection']
    mongodb_db = args['mongodb_database_name']
    mongodb_coll = args['mongodb_collection_name']
    mongodb_coll_err = args['mongodb_collection_error_name']
    dirlogging = args['dirlogging']
    n_retries = args['number_retries_acces_url']
    nprocess = args['number_parallel_process']
    timeout_request = args['timeout_request']
    limit_size = args['limit_number_pages_for_each_website']
    depth_limit = args['depth_to_crawl']
    year_range = args['year_range']
    time_sleep = args['time_sleep']

    # #%% get arguments from bash command line
    # for i, arg in enumerate(sys.argv):
    # ##  dev 202008 : create -limitpages argument from comand line
    # #     if arg == "-limitpages":
    # #         limitpages = sys.argv[i+1]
    # #         print("limitpages : " + str(limitpages))
    # # end dev
    #     # print( arg)
    #     if arg == "-importfromonefile":
    #         dirpath, filename = os.path.split(sys.argv[i + 1])
    #     # print(f"Argument {i:>6}: {arg}")
    #         # print(dirpath, filename)
    #     #
    #     if arg == "-dirlogging":
    #         dirlogging = sys.argv[i + 1]

    #     if arg == '-N_parallel_process':
    #         try:
    #             nprocess = int(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -N_round_scraping has to be an integer.\n\n\n\n')

    #     if arg == '-N_retries_acces_url':
    #         try:
    #             n_retries = int(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -N_round_scraping has to be an integer.\n\n\n\n')

    #     if arg == '-timeout_request':
    #         try:
    #             timeout_request = float(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -timeout_request has to be a float using the point (.) as decimal separator (i.e.: 1.5, 1.2, 3.4, 1.0, etc. ).\n\n\n\n')

    #     if arg == '-Limit_number_pages_for_each_website':
    #         try:
    #             limit_size = int(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -N_round_scraping has to be an integer.\n\n\n\n')

    #     if arg == '-Depth_to_scrap_for_website':
    #         try:
    #             depth_limit = int(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -N_round_scraping has to be an integer.\n\n\n\n')

    #     if arg == '-Year_range_Wayback_machine':
    #         try:
    #             year_range = int(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -N_round_scraping has to be an integer.\n\n\n\n')

    #     if arg == '-time_sleep':
    #         try:
    #             time_sleep = int(sys.argv[i + 1])
    #         except:
    #             raise ValueError('\n\n\n\nThe argument -time_sleep has to be an integer.\n\n\n\n')

    #     if arg == '-mongodb_uri_string_connection':
    #         uri_string = sys.argv[i + 1]
    #         print(f"This_is_your_uri_for_connection : {uri_string}")

    #     if arg == '-mongodb_database_name':
    #         mongodb_db = sys.argv[i + 1]

    #     if arg == '-mongodb_collection_name':
    #         mongodb_coll = sys.argv[i + 1]

    #     if arg == '-mongodb_collection_error_name':
    #         mongodb_coll_err = sys.argv[i + 1]

    # #%% check mandatory arguments
    # if filename == '':
    #     raise ValueError('\n\n\n\nThe argument -importfromonefile is mandatory!\n\n\n\n')
    # if uri_string == '':
    #     raise ValueError('\n\n\n\nThe argument -mongodb_uri_string_connection is mandatory!\n\n\n\n')
    # if mongodb_db == '':
    #     raise ValueError('\n\n\n\nThe argument -mongodb_database_name is mandatory! \n\n\n\n')
    # if mongodb_coll == '':
    #     raise ValueError('\n\n\n\nThe argument -mongodb_collection_name is mandatory! \n\n\n\n')
    # if mongodb_coll_err == '':
    #     raise ValueError('\n\n\n\nThe argument -mongodb_collection_error_name is mandatory! \n\n\n\n')
#%% ensure depth == 1
    if depth_limit > 1:
        depth_limit = 1
        print(f'In this version of the Wayback Machine Crawler you only can crawl 1 depth for each domain. Further improvemeents are coming.')

    #%% set directory
    filenameOnly, file_extension = os.path.splitext(filename)

    if not os.path.exists(dirlogging):
        dirlogging = 'LOG'
        if not os.path.exists(dirlogging):
            print('Creating dirlogging directory to store logs {!r}'.format(dirlogging))
            os.makedirs(os.path.realpath(dirlogging))
        else:
            print(f'Logging directory already exist : {dirlogging}')
    #
    #%% settings
    settings = {
        'LOG_FILE' : os.path.join(dirlogging,'LOG_' + filenameOnly),
        'N_retries_for_url' : n_retries,
        'timeout_request' : timeout_request,
        'limit_size' : limit_size,
        'depth_limit' : depth_limit,
        'year_range' : year_range,
        'time_sleep' : time_sleep,
        'mongodb_uri_string_connection' : uri_string,
        'mongodb_database_name' : mongodb_db,
        'mongodb_collection_name' : mongodb_coll,
        'mongodb_collection_error_name' : mongodb_coll_err,
        'BOT_NAME': '4POINT0BOT',
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 10,
        'WAYBACK_MACHINE_TIME_RANGE': (20150101, 20220215),
        'ROBOTSTXT_OBEY': True
        }

    #%% run main crawler
    run_multiprocess(dirpath = dirpath, filename = filename, nprocess = nprocess, settings = settings)

    #%% check error for homepages
    buffer_len_error = 0
    len_err_homepage = -1
    Crawl_again = True
    n_rerun_crawl = 0
    while Crawl_again:
        # GET HOMEPAGE ERROR TO CRAWL AGAIN
        filename_crawl_again = filenameOnly + '_homepage_crawl_again-temp.txt'
        errorHome = GetErrorHomepages(uri_string_connection = uri_string, name_db = mongodb_db, collection_name = mongodb_coll, collection_error_name = mongodb_coll_err, path_output = dirpath, filename = filename_crawl_again)
        # update buffer
        len_err_homepage = errorHome.n_homepage_to_crawl
        # print(errorHome.n_homepage_to_crawl)
        # this condition is necessary in case there is no error in the first iteration.
        if not len_err_homepage == 0 and len_err_homepage != buffer_len_error:
            n_rerun_crawl = n_rerun_crawl +1
            print(f'A new crawler started to correct errors in crawling webistes. His re-run number is {n_rerun_crawl}. {len_err_homepage} homepages have not been crawled in previous crawling and we retry again... ')
            time.sleep(240)
            # run crawler again
            settings['LOG_FILE'] = os.path.join(dirlogging,'LOG_' + re.sub('\.txt$','', filename_crawl_again))
            run_multiprocess(dirpath = dirpath, filename = filename_crawl_again, nprocess = nprocess, settings = settings)
            buffer_len_error = errorHome.n_homepage_to_crawl
        else:
            Crawl_again = False
            # print(buffer_len_error)
            # print(len_err_homepage)
            # print(n_rerun_crawl)
            if os.path.exists(os.path.join(dirpath,filename_crawl_again)):
                os.remove(os.path.join(dirpath,filename_crawl_again))

    #%% check error for single pages
    buffer_len_error = 0
    len_err_singleUrls = -1
    Scrap_again = True
    n_rerun_scraping = 0
    while Scrap_again:
        # GET HOMEPAGE ERROR TO CRAWL AGAIN
        filename_scrap_again = filenameOnly + '_homepage_scraping_singleUrls-temp.txt'
        errorSingleUrls = GetErrorSimpleUrl(uri_string_connection = uri_string, name_db = mongodb_db, collection_name = mongodb_coll, collection_error_name = mongodb_coll_err, path_output = dirpath, filename = filename_scrap_again)
        # update buffer
        len_err_singleUrls = errorSingleUrls.n_url_to_scrap
        # print(errorHome.n_homepage_to_crawl)
        # this condition is necessary in case there is no error in the first iteration.
        if not len_err_singleUrls == 0 and len_err_singleUrls != buffer_len_error:
            n_rerun_scraping = n_rerun_scraping +1
            print(f'A new scraping process started to correct errors for single pages. His re-run number is {n_rerun_scraping}. {len_err_singleUrls} single pages have not been scrapped in previous scraping process and we retry again... ')
            time.sleep(60)
            # run crawler again
            settings['LOG_FILE'] = os.path.join(dirlogging,'LOG_' + re.sub('\.txt$','', filename_scrap_again))
            run_multiprocess_single_link(dirpath = dirpath, filename = filename_scrap_again, nprocess = nprocess, settings = settings)
            buffer_len_error = errorSingleUrls.n_url_to_scrap
        else:
            Scrap_again = False
            # print(buffer_len_error)
            # print(len_err_homepage)
            # print(n_rerun_crawl)
            if os.path.exists(os.path.join(dirpath,filename_scrap_again)):
                os.remove(os.path.join(dirpath,filename_scrap_again))









