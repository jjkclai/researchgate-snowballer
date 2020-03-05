# In[]:

from bs4 import BeautifulSoup
from dateutil.parser import parse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions
from urllib.parse import urljoin

import re

# In[]:

def is_date(string, fuzzy=True):
    try: 
        parse(string, fuzzy=fuzzy)
        return True

    except ValueError:
        return False
    
def parse_date(string, fuzzy=True):
    return str(parse(string, fuzzy=fuzzy).year)

# In[]:
    
def get_title(soup):
    try:
        return [item.get_text(strip=True) for item in soup.select("h1[itemprop='headline']")][0]
    except:
        return "NA"
    
# In[]:
        
def get_author(soup):
    try:
        return [item.get_text(strip=True) for item in soup.select("div[itemprop='name']")]
    except:
        return "NA"

# In[]:
        
def get_abstract(soup):
    try:
        return [item.get_text(strip=True) for item in soup.select("div[itemprop='description']")][0]
    except:
        return "NA"

# In[]:
        
def get_counts(soup):
    citations_count = "NA"
    references_count = "NA"
    
    try:
        counts = [item.get_text(strip=True) for item in soup.select("span > div.nova-c-nav__item-label")]
        
        try:
            citations_count = [s for s in re.split("[()]", counts[0]) if s.isdigit()][0]
        except:
            pass
        
        try:
            references_count = [s for s in re.split("[()]", counts[1]) if s.isdigit()][0]
        except:
            pass
    except:
        pass
    
    return citations_count, references_count

# In[]:
    
def get_type(soup):
    try:
        return [item.get_text(strip=True) for item in soup.select("span[class='publication-meta__type']")][0]
    except:
        return "NA"
    
# In[]:

def get_doi(soup):
    try:
        return [item.get_text(strip=True) for item in soup.select("div.nova-e-text > a.nova-e-link")][1]
    except:
        return "NA"
    
# In[]:

def get_time(soup):
    try:
        return [parse_date(item) for item in [item_.get_text(strip=True) for item_ in soup.select("span")] if is_date(item)][0]
    except:
        return "NA"

# In[]:

def extract_metadata(driver):
    soup = BeautifulSoup(driver.page_source, features="lxml")
    soup_publication_meta = BeautifulSoup(str(soup.select("div[class='publication-meta']")), features="lxml")
    
    title = get_title(soup)
    author = get_author(soup)
    abstract = get_abstract(soup)
    citations_count, references_count = get_counts(soup)
    publication_meta_type = get_type(soup_publication_meta)
    publication_meta_doi = get_doi(soup_publication_meta)
    publication_meta_time = get_time(soup_publication_meta)
    
    return {
            "title": title,
            "author": author,
            "abstract": abstract,
            "citations": citations_count,
            "references": references_count,
            "type": publication_meta_type,
            "doi": publication_meta_doi,
            "time": publication_meta_time }

# In[]:
    
def expand_citations(driver):
    button_element = driver.find_element_by_class_name("citations")
    button_element.click()
    
    while(True):
        try:
            button_element = WebDriverWait(driver, 3).until(expected_conditions.element_to_be_clickable((By.XPATH,"//button[.='Show more']")))
            button_element.click()
        except:
            break

# In[]:

def extract_citations(driver, base_url):
    soup = BeautifulSoup(driver.page_source, features="lxml")
    soup_ = BeautifulSoup(str(soup.select("div[id='citations']")), features="lxml")
    
    urls = [urljoin(base_url, item['href']) for item in soup_.select("a.nova-v-publication-item__action")]
    publication_id = [re.split("[/_]", item)[4] for item in urls]
    
    return dict(zip(publication_id, urls))

# In[]:

def expand_references(driver):
    button_element = driver.find_element_by_class_name("references")
    button_element.click()
    
    while(True):
        try:
            button_element = WebDriverWait(driver, 3).until(expected_conditions.element_to_be_clickable((By.XPATH,"//button[.='Show more']")))
            button_element.click()
        except:
            break

# In[]:

def extract_references(driver, base_url):
    soup = BeautifulSoup(driver.page_source, features="lxml")
    soup_ = BeautifulSoup(str(soup.select("div[id='references']")), features="lxml")
    
    urls = [urljoin(base_url, item['href']) for item in soup_.select("a.nova-v-publication-item__action")]
    publication_id = [re.split("[/_]", item)[4] for item in urls]
    
    return dict(zip(publication_id, urls))
