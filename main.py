#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import csv
from oauth2client import client
import httplib2
from apiclient.discovery import build
from google.auth.transport.requests import Request
from google.oauth2 import service_account
import json
import os
from hashlib import md5 as md5_obj
from zipfile import ZipFile, BadZipFile
import re
import time

# if not os.path.exists("/data/out/tables"): os.makedirs("/data/out/tables")

if not os.path.exists("/data/config.json"):
    # Interactive mode
    interactive_mode = True
    print("/data/config.json not found, running in interactive debug mode")
    # r_token = input("Refresh token: ")
    # client_id = input("Client ID: ")
    # client_sercret = input("Client Secret: ")
    bucket = ""
    debugMode = 1
    maxResults = 100
    accepted_dirnames = ""
    service_acc = {}
else:
    with open("/data/config.json", "r") as fid:
        config = json.load(fid)
    # a_token is None when refreshing, obviously
    # a_token = config["access_token"]
    interactive_mode = False
    # r_token = config["parameters"]["#r_token"]
    # client_id = config["parameters"]["client_id"]
    # client_sercret = config["parameters"]["#client_secret"]
    bucket = config["parameters"]["bucket_name"]
    debugMode = int(config["parameters"]["debug_mode"])
    maxResults = config["parameters"]["max_results"]
    accepted_dirnames = config["parameters"][
        "accepted_dirnames"
    ]  # preferably defaults to ["installs", "subscribers", "subscriptions"]
    service_acc = json.loads(config["parameters"]["service_acc"])

accepted_dirnames = list(map(str.strip, accepted_dirnames.split(",")))
if accepted_dirnames[-1] == "":
    accepted_dirnames = accepted_dirnames[:-1]
if debugMode:
    print("Will accept only: %s" % ",".join(accepted_dirnames))

# Temporary override - KBC configuration schema does not seem to work properly
# accepted_dirnames = ["installs","subscriptions", "sales", "earnings"]
# accepted_dirnames = [] #testing purpose only, let us download zips only
# fileBufferSize = 16 * 1024 * 1024 #16 MB
# a_token = None
# token_expiration = None
token_endpoint = "https://www.googleapis.com/oauth2/v4/token"
user_agent = "ex_google_storage/1.0"
outputFilename = "/data/out/tables/storage.csv"

service_creds = service_account.Credentials.from_service_account_info(
    service_acc, scopes=["https://www.googleapis.com/auth/devstorage.read_only"]
)
# creds = client.GoogleCredentials(None,client_id,client_sercret,r_token,token_expiration,token_endpoint,user_agent)
http = httplib2.Http()
service_creds.refresh(Request())
Service = build("storage", "v1", credentials=service_creds)
# creds.refresh(http)


if debugMode:
    print("Debug mode is on, I will talk a lot")


class ObjectDownloadError(Exception):
    pass


def Charset(response):
    return response["content-type"][response["content-type"].find("=") + 1 :]


def MD5(u):
    return md5_obj(u.encode("utf-16le")).hexdigest()


class Header:
    Headers = []

    def __init__(self, h, md, handle):
        self.Header = h
        self.Hash = md
        self.Handle = handle
        Header.Headers.append(self)

    @staticmethod
    def Exists(md):
        for o in Header.Headers:
            if o.Hash == md:
                return True
        return False

    @staticmethod
    def Get(md):
        for o in Header.Headers:
            if o.Hash == md:
                return o
        return None

    @staticmethod
    def TidyUp():
        for o in Header.Headers:
            o.Handle.close()


class Extractor:
    def __init__(self, bucket, service_creds):
        # self.a_token = a_token
        # self.r_token = r_token
        # self.client_id = client_id
        # self.client_sercret = client_sercret
        self.Bucket = bucket
        self.service_creds = service_creds
        self.Credentials = None
        self.LastRequest = None
        self.LastResponse = None
        self.Service = Service
        self.HeaderWritten = False
        self.Handle = None
        self.Writer = None

        self.Headers = []
        self.Handles = []
        self.Writers = []

        self.INames = []
        self.ExportedItems = 0
        self.ExportedObjects = 0

    # def RenewAccessToken(self):
    # 	if isinstance(service_creds, dict):
    # 		self.Service =
    # 		return
    # 	global token_endpoint, token_expiration, user_agent
    # 	self.Credentials = client.GoogleCredentials(self.a_token,self.client_id,self.client_sercret,self.r_token,token_expiration,token_endpoint,user_agent)
    # 	self.http = self.Credentials.authorize(httplib2.Http())
    # 	self.Credentials.refresh(self.http)
    # 	self.Service = build("storage","v1",http=self.http)
    def GetObject(self, name):
        params = {"bucket": self.Bucket, "object": name.replace("/", "%2F")}
        headers = {}
        self.service_creds.apply(headers)
        resp, content = http.request("https://storage.googleapis.com/%(bucket)s/%(object)s" % params, headers=headers)
        content = content.decode(Charset(resp))
        if resp.status != 200:
            global debugMode
            if debugMode:
                raise ObjectDownloadError(
                    "Error downloading object %s from bucket %s\nResponse headers: %s\nResponse body: %s"
                    % (name, self.Bucket, str(resp), content)
                )
            else:
                raise ObjectDownloadError("Error downloading object %s from bucket %s" % (name, self.Bucket))
        if content[-1] == "\n":
            content = content[:-1]
        return content

    def ListObjects(self):
        if self.LastRequest == None:
            request = self.Service.objects().list(bucket=self.Bucket, maxResults=maxResults)
        else:
            print("Listing another set of objects...")
            request = self.Service.objects().list_next(self.LastRequest, self.LastResponse)
        if request == None:
            global debugMode
            if debugMode:
                print("No items left in the bucket")
            return None  # no items left
        response = request.execute()
        self.LastRequest = request
        self.LastResponse = response
        return response["items"]

    def GetZipObjects(self, name):
        params = {"bucket": self.Bucket, "object": name.replace("/", "%2F")}
        headers = {}
        self.service_creds.apply(headers)
        resp, content = http.request("https://storage.googleapis.com/%(bucket)s/%(object)s" % params, headers=headers)
        fid = open("/data/temp.zip", "wb")
        fid.write(content)
        fid.close()
        fid = open("/data/temp.zip", "rb")
        z = ZipFile(fid, "r")
        objects = []
        for f in z.filelist:
            if not f.orig_filename.lower().endswith(".csv"):
                continue
            o = z.read(f).decode("utf-8")
            if o[-1] == "\n":
                o = o[:-1]
            objects.append(o)
        global debugMode
        if debugMode:
            print("ZIP contained %d objects" % len(objects))
        return objects

    def AppendItems(self, items):
        global debugMode
        for item in items:
            iname = None
            idimension = None
            isZip = False
            dname = os.path.dirname(item["name"]).replace("/", "_")
            if item["name"].lower().endswith(".csv"):
                if item["name"].count("/") == 2:
                    repatt = "(.*)/(.*)/(.*)_(.*)_(.*)_(.*).csv"
                    reskip = 1
                else:
                    repatt = "(.*)/(.*)_(.*)_(.*)_(.*).csv"
                    reskip = 0
                p = re.compile(repatt)
                sre = p.search(item["name"])
                if sre == None:
                    if debugMode:
                        print("Trouble identifying %s, skipping" % item["name"])
                    continue
                iname = sre.group(1 + reskip)
                idimension = sre.group(5 + reskip)
                self.INames.append(iname)
                if iname not in accepted_dirnames:
                    if debugMode:
                        print("Skipping %s, not wanted." % iname)
                    continue
                if debugMode:
                    print("name: %s, dimension: %s" % (iname, idimension))
                # if debugMode: print("Skipping %s, a .csv file, we are interested in the archives"%item["name"])
                # continue #remove to download all .csv files as well
                objects = [self.GetObject(item["name"])]
            else:
                if debugMode:
                    print("Running archive:", item["name"])
                if os.path.dirname(item["name"]).replace("/", "_") not in accepted_dirnames:
                    if debugMode:
                        print("Skipping %s, not wanted." % item["name"])
                    continue
                try:
                    objects = self.GetZipObjects(item["name"])
                except BadZipFile as e:
                    print("Bad zip file {0}".format(item["name"]))
                    raise Exception(e)
                isZip = True
                if debugMode:
                    print("ZIP returned %d objects" % len(objects))
            for o in objects:
                o = o.split("\n")
                reader = csv.reader(o)
                rows = list(reader)
                if debugMode:
                    print("No. of rows: %d" % len(rows))
                if len(rows) == 0:
                    continue
                h = rows.pop(0)
                md = MD5(str(h))
                if debugMode:
                    print("Header hash: %s" % md)
                if not Header.Exists(md):
                    if debugMode:
                        print("Header is new")
                    if iname != None:
                        tableName = "%s_%s_%s.csv" % (iname, idimension, md)
                    else:
                        tableName = "%s_%s.csv" % (os.path.dirname(item["name"]).replace("/", "_"), md)
                    if debugMode:
                        print("Attempting new output table name: %s" % tableName)
                    if os.path.exists(
                        os.path.join("/data/out/tables/", tableName)
                    ):  # table is from same output/dimension but has a different header
                        if debugMode:
                            print("Data inconsistency occurred while parsing %s. I will try to manage" % item["name"])
                        nameCT = 0
                        newTableName = "%s_%03d.csv" % (tableName.replace(".csv", ""), nameCT)
                        while os.path.exists(os.path.join("/data/out/tables/", newTableName)):
                            nameCT += 1
                            newTableName = "%s_%03d.csv" % (tableName.replace(".csv", ""), nameCT)
                        tableName = newTableName
                    tablePath = os.path.join("/data/out/tables/", tableName)
                    header = Header(h, md, open(tablePath, "w"))
                    if debugMode:
                        print("Created new output '%s'" % tablePath)
                    writer = csv.writer(header.Handle)
                    writer.writerow(h)
                else:
                    if debugMode:
                        print("Header already in db")
                    header = Header.Get(md)
                    writer = csv.writer(header.Handle)
                for row in rows:
                    writer.writerow(row)
            self.ExportedObjects += len(objects)
            self.ExportedItems += 1

    def TidyUp(self):
        if self.Handle != None:
            self.Handle.close()


if __name__ == "__main__":
    ex = Extractor(service_creds=service_creds, bucket=bucket)
    total = 0
    print("Started writing results, this might take some time...")
    items = ex.ListObjects()
    while True:
        if items == None:
            break
        ex.AppendItems(items)
        total += len(items)
        if debugMode:
            print("Listed through another %d items, total %d listed so far" % (len(items), total))
            if interactive_mode:
                # c = input("Do you want to continue? Y/N ").lower()
                # if (c!="y"): break
                pass
        items = ex.ListObjects()
    ex.TidyUp()
    print("Done writing results.")
    ex.INames = list(set(ex.INames))
    if debugMode:
        print("List of dirnames I came by: ", ex.INames)
        print("Exported totals: %d objects from %d items" % (ex.ExportedObjects, ex.ExportedItems))
    if interactive_mode:
        input("Waiting for your debugging work to be done. When ready, press ENTER")
