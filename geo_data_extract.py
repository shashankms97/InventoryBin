import requests, csv
from queue import Queue
from threading import Thread
from multiprocessing import Manager
import time
import shelve, os
import logging
logging.basicConfig(filename="Zipcodes.log", 
                   filemode='w', 
                   format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                   datefmt='%H:%M:%S',
                   level=logging.DEBUG)
logger = logging.getLogger('GeoData')

mykeys = ['AIzaSyCm0Gp-EZrU-I28_IgxlIIxDSbLcTyIPzE', 'AIzaSyCMTLqKjb8bvGoNYU-QAaQsXSTA3N5PaUA']

class WorkerThread(Thread):
   def __init__(self, queue):
       Thread.__init__(self)
       self.queue = queue

   def run(self):
       while True:
           shared_dict, zipcode = self.queue.get()
           self.fetch_geo_data(shared_dict, zipcode)
           self.queue.task_done()

   def fetch_geo_data(self, shared_dict, zipcode):
       myurl = "https://maps.googleapis.com/maps/api/geocode/json?address=%s&key=AIzaSyCm0Gp-EZrU-I28_IgxlIIxDSbLcTyIPzE"%(zipcode)
       r = requests.get(myurl)
       geo_dict = r.json()
       shared_dict[zipcode] = geo_dict
       return
         
def read_file(filename):
    with open(filename, 'r') as fp:
        for ele in fp:
            ele = ele.strip('\n')
            yield ele

def write_to_file(shared_dict, fname):
    flag = 0
    with open(fname, 'a') as f:
        writer = csv.writer(f, delimiter= '|')
        for zipcode, jsondict in shared_dict.items():
            res_list = jsondict.get('results', [])
            status  = jsondict.get("status", '')
            if status in ['OVER_QUERY_LIMIT', 'REQUEST_DENIED']:
                flag = 1
            if not res_list:
                logger.error("%s zipcode failed %s"%(zipcode, status))
                continue
            res_dict = res_list[0]
            location = res_dict.get('geometry', {}).get('location', {})
            latitude = location.get('lat', '')
            longitude = location.get('lng', '')
            writer.writerow([zipcode, latitude, longitude])
    if flag:
        sys.exit()
    return 

def get_last_zipcode(fname):
    my_zip_list = [0]
    with open(fname, 'r') as f:
        reader = csv.reader(f, delimiter = "|")
        for csv_line in reader:
            zipcode, latitude, longitude = csv_line
            my_zip_list.append(zipcode)
    my_zip_list.sort()
    return my_zip_list[-1]

def main():
   manager = Manager()
   fname = "geodata.csv"
   last_zip_code = 0
   if os.path.exists(fname):
       last_zip_code = get_last_zipcode(fname)
   #zip_code_ls = read_file('AllZipCodes.txt')
   zip_code_ls = read_file('missing.txt')
   queue = Queue()
   shared_dict = manager.dict()
   for x in range(4):
       worker = WorkerThread(queue)
       worker.daemon = True
       worker.start()

   for i, zipcode in enumerate(zip_code_ls):
       if int(zipcode) < int(last_zip_code): 
          logger.info('continued %s'%zipcode)
          continue
       if (i+1)%50 == 0:
           queue.put((shared_dict, zipcode))
           queue.join()
           write_to_file(shared_dict, fname)
           shared_dict = manager.dict()
       else: 
           queue.put((shared_dict, zipcode))
   if not queue.empty():
       queue.join()
       write_to_file(shared_dict, fname) 
main()
