import datetime
from datetime import datetime, timedelta
import calendar
import requests
import json
import urllib3
import configparser
import csv
config = configparser.ConfigParser()
config.read('credentials.ini')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)