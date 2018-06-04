#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import csv
from oauth2client import client
import httplib2
from apiclient.discovery import build
import json
import os
from hashlib import md5 as md5_obj
from zipfile import ZipFile

if not os.path.exists("/data/out/tables"): os.makedirs("/data/out/tables")

if not os.path.exists("/data/config.json"):
	#Interactive mode
	interactive_mode = True
	print("/data/config.json not found, running in interactive debug mode")
	r_token = input("Refresh token: ")
	client_id = input("Client ID: ")
	client_sercret = input("Client Secret: ")
	bucket = input("Bucket name: ")
	debugMode = 1
	maxResults = int(input("Max results per page: "))
else:
	with open("/data/config.json","r") as fid:
		config = json.load(fid)
	#a_token is None when refreshing, obviously
	#a_token = config["access_token"]
	interactive_mode = False
	r_token = config["parameters"]["#r_token"]
	client_id = config["parameters"]["client_id"]
	client_sercret = config["parameters"]["#client_secret"]
	bucket = config["parameters"]["bucket_name"]
	debugMode = int(config["parameters"]["debug_mode"])
	maxResults = config["parameters"]["max_results"]

#fileBufferSize = 16 * 1024 * 1024 #16 MB
a_token = None
token_expiration = None
token_endpoint = "https://www.googleapis.com/oauth2/v4/token"
user_agent = "ex_google_storage/1.0"
outputFilename = "/data/out/tables/storage.csv"

creds = client.GoogleCredentials(None,client_id,client_sercret,r_token,token_expiration,token_endpoint,user_agent)
http = creds.authorize(httplib2.Http())
creds.refresh(http)

if debugMode:
	print("Debug mode is on, I will talk a lot")

class ObjectDownloadError(Exception): pass

def Charset(response):
	return response["content-type"][response["content-type"].find("=")+1:]

def MD5(u):
	return md5_obj(u.encode("utf-16le")).hexdigest()

class Header:
	Headers = []
	def __init__(self,h,md,handle):
		self.Header = h
		self.Hash = md
		self.Handle = handle
		Header.Headers.append(self)
	@staticmethod
	def Exists(md):
		for o in Header.Headers:
			if o.Hash==md:
				return True
		return False
	@staticmethod
	def Get(md):
		for o in Header.Headers:
			if o.Hash==md: return o
		return None
	@staticmethod
	def TidyUp():
		for o in Header.Headers:
			o.Handle.close()

class Extractor:
	def __init__(self,a_token,r_token,client_id,client_sercret,bucket):
		self.a_token = a_token
		self.r_token = r_token
		self.client_id = client_id
		self.client_sercret = client_sercret
		self.Bucket = bucket
		self.Credentials = None
		self.LastRequest = None
		self.LastResponse = None
		self.Service = None
		self.HeaderWritten = False
		self.Handle = None
		self.Writer = None
		
		self.Headers = []
		self.Handles = []
		self.Writers = []
	def RenewAccessToken(self):
		global token_endpoint, token_expiration, user_agent
		self.Credentials = client.GoogleCredentials(self.a_token,self.client_id,self.client_sercret,self.r_token,token_expiration,token_endpoint,user_agent)
		self.http = self.Credentials.authorize(httplib2.Http())
		self.Credentials.refresh(self.http)
		self.Service = build("storage","v1",http=self.http)
	def GetObject(self,name):
		params = {"bucket": self.Bucket, "object": name.replace("/","%2F")}
		resp, content = self.http.request("https://storage.googleapis.com/%(bucket)s/%(object)s"%params)
		content = content.decode(Charset(resp))
		if resp.status!=200:
			global debugMode
			if debugMode:
				raise ObjectDownloadError("Error downloading object %s from bucket %s\nResponse headers: %s\nResponse body: %s"%(name,self.Bucket,str(resp),content))
			else:
				raise ObjectDownloadError("Error downloading object %s from bucket %s"%(name,self.Bucket))
		if content[-1]=='\n': content = content[:-1]
		return content
	def ListObjects(self):
		if self.LastRequest == None:
			request = self.Service.objects().list(bucket=self.Bucket,maxResults=maxResults)
		else:
			print("Listing another set of objects...")
			request = self.Service.objects().list_next(self.LastRequest,self.LastResponse)
		if request==None:
			global debugMode
			if debugMode: print("No items left in the bucket")
			return None #no items left
		response = request.execute()
		self.LastRequest = request
		self.LastResponse = response
		return response["items"]
	def GetZipObjects(self,name):
		params = {"bucket": self.Bucket, "object": name.replace("/","%2F")}
		resp, content = self.http.request("https://storage.googleapis.com/%(bucket)s/%(object)s"%params)
		fid = open("/data/temp.zip","wb")
		fid.write(content)
		fid.close()
		fid = open("/data/temp.zip","rb")
		z = ZipFile(fid,"r")
		objects = []
		for f in z.filelist:
			if not f.orig_filename.lower().endswith(".csv"): continue
			o = z.read(f).decode("utf-8")
			if o[-1]=="\n": o=o[:-1]
			objects.append(o)
		return objects
	def AppendItems(self,items):
		for item in items:
			if (item["name"].lower().endswith(".csv")):
				global debugMode
				#if debugMode: print("Skipping %s, a .csv file, we are interested in the archives"%item["name"])
				continue
				objects = [self.GetObject(item["name"])]
			else:
				print("Running archive:",item["name"])
				objects = self.GetZipObjects(item["name"])
			for o in objects:
				o = o.split("\n")
				reader = csv.reader(o)
				rows = list(reader)
				if len(rows)==0: continue
				h = rows.pop(0)
				md = MD5(str(h))
				if (not Header.Exists(md)):
					header = Header(h,md,open("/data/out/tables/%s.csv"%os.path.dirname(item["name"]),"w"))
					writer = csv.writer(header.Handle)
					writer.writerow(h)
				else:
					header = Header.Get(md)
					writer = csv.writer(header.Handle)
				for row in rows:
					writer.writerow(row)
	def TidyUp(self):
		if self.Handle!=None: self.Handle.close()

if __name__=="__main__":
	ex = Extractor(None,r_token,client_id,client_sercret,bucket)
	ex.RenewAccessToken()
	total = 0
	print("Started writing results, this might take some time...")
	items = ex.ListObjects()
	while True:
		if items==None: break
		ex.AppendItems(items)
		total+=len(items)
		if debugMode:
			print("Wrote another %d items, total %d written so far" % (len(items),total))
			if interactive_mode:
				c = input("Do you want to continue? Y/N ").lower()
				if (c!="y"): break
		items = ex.ListObjects()
	ex.TidyUp()
	print("Done writing results.")
	if interactive_mode:
		input("Waiting for your debugging work to be done. When ready, press ENTER")
