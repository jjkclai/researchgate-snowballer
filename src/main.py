# In[]:

import os

#abspath = os.path.abspath(__file__)
#dname = os.path.dirname(abspath)
#os.chdir(dname)

os.chdir("input your working directory here")

# In[]:

from extract import *

from csv import DictWriter
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import concurrent.futures
import pickle
import re
import subprocess
import threading
import time

# In[]:

max_workers = 10
max_worker_loads = 10

base_url = "https://www.researchgate.net/"
core_url = [
        "https://www.researchgate.net/publication/256837250_Temporal_Event_Sequence_Simplification", 
#        "https://www.researchgate.net/publication/331625794_LDA_Ensembles_for_Interactive_Exploration_and_Categorization_of_Behaviors"
        ]

# In[]:

def get_webserver():
    process = subprocess.Popen("java -cp ./Dependencies/htmlunit-driver-2.37.0-jar-with-dependencies.jar:./Dependencies/selenium-server-standalone-3.141.59.jar org.openqa.grid.selenium.GridLauncherV3".split(), shell=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    return process

# In[]:

def get_webdriver():
    options = Options()
    options.headless = True

    user_agent = UserAgent().random
    options.add_argument(f"user-agent={user_agent}")

    #driver = webdriver.Firefox(options=options)
    driver = webdriver.Remote(desired_capabilities=webdriver.DesiredCapabilities.HTMLUNITWITHJS, options=options)
    
    return driver

# In[]:
    
def write_to_csv(dict_list):
    with open("snowball.csv", "w") as outfile:
        field_names = ["lvl", "pid", "title", "author", "type", "time", "doi", "citations", "references", "abstract"]
        writer = DictWriter(outfile, field_names)
        writer.writeheader()
        writer.writerows(dict_list)
    
# In[]:

def write_to_pickle(i, m, c, r):
    with open("snowball.pickle", "wb") as outfile:
        pickle.dump(i, outfile)
        pickle.dump(m, outfile)
        pickle.dump(c, outfile)
        pickle.dump(r, outfile)

# In[]:

def read_from_pickle():
    with open("snowball.pickle", "rb") as outfile:
        i = pickle.load(outfile)
        m = pickle.load(outfile)
        c = pickle.load(outfile)
        r = pickle.load(outfile)
    
    return i, m, c, r

# In[]:
    
def worker(worker_type, urls):
    metadata = []
    citations = {}
    references = {}
    
    driver = 0
    
    wid = "0"
    if worker_type != 0:
        wid = re.split("[_]", threading.current_thread().name)[1]
    
    print("Worker %s: Starting..." % wid)
    start_time = time.time()
    start_time_ = start_time
    
    driver = get_webdriver()
    
    for url in urls:
        pid = re.split("[/_]", url)[4]
        
        print("Worker %s: Retrieving paper %s..." % (wid, pid))
        
        loop = True
        while loop:
            try:
                driver.get(url)
                
                WebDriverWait(driver, 5).until(expected_conditions.presence_of_element_located((By.CLASS_NAME, "brand")))
                
                print("Worker %s: Retrieving metadata..." % wid)
                metadata_ = extract_metadata(driver)
                
                citations_ = {}
                references_ = {}
                
                if worker_type == 0:
                    print("Worker %s: Retrieving citations..." % wid)
                    expand_citations(driver)
                    citations_ = extract_citations(driver, base_url)
                    print("Worker %s: Retrieving references..." % wid)
                    expand_references(driver)
                    references_ = extract_references(driver, base_url)
                
                elif worker_type > 0:
                    print("Worker %s: Retrieving citations..." % wid)
                    expand_citations(driver)
                    citations_ = extract_citations(driver, base_url)
                    
                elif worker_type < 0:
                    print("Worker %s: Retrieving references..." % wid)
                    expand_references(driver)
                    references_ = extract_references(driver, base_url)
                
                break
            
            except Exception as e:
                print("Worker %s: Error occured... Restarting webdriver..." % wid)
                print("\t", e)
                driver.close()
                driver = get_webdriver()
                
        print("Worker %s: Updating list of citations..." % wid)
        citations.update(citations_)
        
        print("Worker %s: Updating list of references..." % wid)
        references.update(references_)
        
        print("Worker %s: Updating list of metadata..." % wid)
        metadata_["pid"] = pid
        metadata_["lvl"] = str(worker_type)
        metadata.append(metadata_)
        
        print("Worker %s: Complete... (Total runtime %s seconds)" % (wid, (time.time() - start_time_)))
        start_time_ = time.time()
        
    driver.quit()
    
    print("Worker %s: Closing... (Total runtime %s seconds)" % (wid, (time.time() - start_time)))
    
    return metadata, citations, references

# In[]:
    
def split_list(l, n):
    n = max(1, n)
    return [l[i:i+n] for i in range(0, len(l), n)]

# In[]:
    
def roll_snowball(i, metadata, citations, references):
    if i== 0:
        print("Snowball: Rolling core layer...")
        
        metadata_, citations, references = worker(0, core_url)
        
        metadata.extend(metadata_)
        
        return metadata, citations, references
    
    else:
        print("Snowball: Rolling layer %s..." % i)
        
        c_list = split_list(list(citations.values()), max_worker_loads)
        r_list = split_list(list(references.values()), max_worker_loads)
        
        citations = {}
        references = {}
        
        print("Snowball: Rolling citations...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = max_workers) as executor:
            futures = {executor.submit(worker, i, c): c for c in c_list}
            for future in concurrent.futures.as_completed(futures):
                metadata_, citations_, _ = future.result()
                metadata.extend(metadata_)
                citations.update(citations_)
        
        print("Snowball: Rolling references...")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers = max_workers) as executor:
            futures = {executor.submit(worker, -i, r): r for r in r_list}
            for future in concurrent.futures.as_completed(futures):
                metadata_, _, references_ = future.result()
                metadata.extend(metadata_)
                references.update(references_)
        
        print("Snowball: Preparing snow for next level...")
        
        metadata_pid = [m["pid"] for m in metadata]
        
        for pid, _ in list(citations.items()):
            if pid in metadata_pid:
                del citations[pid]
                
        for pid, _ in list(references.items()):
            if pid in metadata_pid:
                del references[pid]
        
        return metadata, citations, references
        
# In[]:

m = []
c = {}
r = {}

start_time = time.time()

process = get_webserver()
time.sleep(10)

for i in range(5):
    m, c, r = roll_snowball(i, m, c, r)
    
    write_to_csv(m)
    write_to_pickle(i+1, m, c, r)

process.kill()

print("Program complete... (Total runtime %s seconds)" % (time.time() - start_time))
