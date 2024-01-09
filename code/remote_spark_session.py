import sys, platform,json
import os,time,datetime
import ast,glob
import datetime
import requests
import requests_kerberos

import logging
logging.bacicConfig(level=logging.INFO, format='app="rm_spark_high_availability_framework" module="%(module)s" timestamps="%(asctime)s" level="%(levelname)s" function="%(funcName)s" message="%(message)s"')

import importlib
topdir = os.path.join(os.path.dirname(__file__), "..")
sys.path.append(topdir)
importlib.reload(sys)
from zookeeper.zkModules import *

import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from py4j.protocol import Py4JJavaError

class SparkInstanceBuilder:
    
    def __inti__(self, environment=None, retries=2, retry_window=20, zookeeper_node=None, zkpath=None, app_name=None):
        try:
            self.sparkEnv=environment
            self.retry=0
            self.retry_window=20
            self.sparkRetry=retries
            self.zkpath=zkpath
            self.zookeeper_node=zookeeper_node
            self.sparkNode=""
            self.app_name=app_name
            
            if (self.sparkEnv is None or self.sparkEnv == "" \
                or \
                self.zookeeper_node is None or self.zookeeper_node == "" \
                or \
                self.zkpath is None or self.zkpath == "" \
                or \
                self.app_name is None or self.app_name == ""):
                raise Exception (f"Missing mandatory parameters, terminating the request to create spark instance")
            
            self.sparkClusterMataInfo = json.loads(self.fetchZkNode(zkpath=self.zkpath, environment=self.sparkEnv, node=self.zookeeper_node))
            
            if (self.sparkClusterMataInfo is None or self.sparkClusterMataInfo == "" or self.sparkClusterMataInfo == "Failure")
                logging.error(f"Process failed while fetching spark cluster details from zookeeper, terminating the request. Pls, validate details under zookerper config")
                raise Exception(f"Process failed while fetching spark cluster details from zookeeper, terminating the request. Pls, validate details under zookerper config")
        except Exception as intiateErr:
            logging.error(f"Process to set intial config parameters has failed with error - {intiateErr}. Terminating the process")
            
            
    def fetchZkNode(self):
        pass
            
            
            
            
            