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

########################## IMPUT DATABASE NAME and COLLECTIONS NAMES insides funtions below: ######
def connectClientMongoDB(uri_string_connection):
    from pymongo import MongoClient
    return MongoClient(uri_string_connection)

def connectDataBase(client, name_db):
    db = client[name_db]  ## give he name of DB on client
    return db

def connectCollectionClient(db, collection_name):
    coll = db[collection_name] # Give the name for the collection with your data
    return coll

def connectCollectionOnError(db, collection_error_name):
    coll = db[collection_error_name] # Give the name for the collection includings errors.
    return coll
