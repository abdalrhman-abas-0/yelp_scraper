from httpx_html import HTMLSession
import time
from time import sleep
import pandas as pd
import numpy as np
import time
from time import sleep
import pandas as pd
import numpy as np
import json

import datetime
from fake_useragent import UserAgent
import os
from tqdm import tqdm
import re

#%% Inputs.

pages_to_scrape = 5
record_file = "tracker accountants in San Francisco at 2023-04-29 19.43.31.txt" 
url ="https://www.yelp.com/search?find_desc=accountants&find_loc=San+Francisco%2C+CA"

#%% main variables

# clears the terminal.
os.system('cls')

# changes the directory to the project folder.
os.chdir('yelp')

search_subject = re.search(r"(?<=find_desc=)\w+", url)[0]
search_location = re.search(r"(?<=find_loc=)\w+", url.replace('+','_'))[0].replace('_',' ')

# controls the starting point of the scraping process.
site = url

primary_stage = True
'''if true it enables the run of the primary stage, and it's False when completing a 
previous scraping process where the PRIMARY stage has been completed before.'''

secondary_stage = True
'''if true it enables the run of the secondary stage, and it's False when completing a 
previous scraping process where the SECONDARY stage has been completed before.'''

nap = 5
'''time in seconds to sleep between the requests incase of request failure.'''

p_save_index = 0
'''the number of the last saved primary file
(incase of building upon old uncompleted scaping ), it's zero for 
'''
s_save_index = 0
'''the number of the last saved secondary file
(incase of building upon old uncompleted scaping ), it's zero for 
'''

I_P = 0
'''the the index of the last successfully scraped search page "primary stage".'''

I_S = 0
'''the the index of the last successfully scraped search page "secondary stage".'''

#%% txt tracker File

 # if there a record file were given it resets the MAIN variables above to continue upon the previous scraping process,
# and if record file was given as 'no' it leave them to their default values to start a new scraping process. 

if record_file.lower() != 'no':
    txt_tracker = record_file
else:
    date_time = str( f'{datetime.datetime.now()}')[:f'{datetime.datetime.now()}'.index('.')].replace(':','.')
    txt_tracker = f'tracker {search_subject} in {search_location} at {date_time}.txt'
    first_creation = open('./outputs/' + txt_tracker, "w")
    first_creation.close()
    
try:
    with open('./outputs/'+ txt_tracker , 'r') as file:
        file = file.readlines()
        '''an txt file which records the progress of the scraping process'''
        for line in file:
            
            if 'primary.csv' in line:
                # as every pages saved in it's own csv file the save index and 
                # the I_P which is the last page scraped successfully are the same
                p_save_index = I_P = int(re.search(f'(?!{search_location} )\d+',line)[0])
                
            elif re.search(r'^\d+', line.strip("\n")) != None:
                # adding 1 to the I_S to start from 
                # the profile after the last saved profile
                I_S = int(line.strip("\n")) + 1
                
            elif 'secondary.csv' in line:
                s_save_index = int(re.search(f'(?!{search_location} )\d+',line)[0])
                               
            
            elif 'PRIMARY' in line:
                primary_stage = False 
                                
            elif 'SECONDARY' in line:
                secondary_stage = False

    
    site = url + f"&start={(I_P*10)}"
    
except:
    pass

#%% crawler Function

def crawler(session, user_agent, site, element_path,previous_page, element_function = None ):
    """ a request maker
    this function enables the program to keep making requests over a given period 
    "nap" until the request is successful and the given element 
    "element_path" is found if the function took so long and the request was failing 
    consistently take a look at the 
    "controls.json" file you may find: 
    ["interruption"] variable: 
        true which means that the connect is interrupted or the given element_path was not found.
    ["break"] variable: 
        false which you can set it to true which allow the program to go in a 
        loop where it sleeps 10 for every iteration then check the connection 
        or change the headers and set it back to false to make the request again.
    

    Args:
        session (class): httpx session to use when making requests.
        user_agent(str): the user agent to send in the headers of the request.
        site(str): the url of the webpage needed to be scraped.
        element_to_find(str): the path "xpath/css" to locate the "element_to_find". 
        response(httpx_html.HTML): the response object to parse them for the desired info.
        previous_page (str): the previously scrape page url
            to send it under the "Referer" in the request header.
        element_function(function: optional): any function that can be used inside the crawler 
            to ensure that the function still runs until 
            the intended element is extracted. Defaults to None.

    Returns:
        element_to_find(object): the element to look for after the request is made,
            if the element is not found the request is remade until it's found.
        response(httpx_html.HTML): the response of the request made.

    """
    breaker = False
    
    while breaker == False:

        try:
            # getting the request headers for the controls.json
            with open('controls.json') as j_file:
                content = json.load(j_file)
                
            breaker = content['break']
            nap = content['nap']
                
            header = content['headers'] 
            header['User-Agent'] = user_agent
            
            if previous_page != None:# not always need for each site
                header['Referer'] = previous_page

            r = session.get(site, headers= header)
            response = r.html
            
            # looking for the given element_path through xpath and css
            try:   
                element_to_find = response.xpath(element_path)
            except:
                element_to_find = response.find(element_path)
                
            if element_function != None:   
                element_to_find = element_function(element_to_find)
            
            if len(element_to_find) == 0 or element_to_find == None:   
                content["empty element"] = True    
                with open('controls.json', 'w') as j_file:  
                    json.dump(content, j_file)
            else:
                break

        except:
            with open('controls.json') as j_file:
                content = json.load(j_file)
                
            if  content["interruption"] == False:
                content["interruption"] = True    
                with open('controls.json', 'w') as j_file:  
                    json.dump(content, j_file)
                
            user_agent = UserAgent().random
            sleep(nap)
            
            while breaker == True:
                with open('controls.json') as j_file:
                    content = json.load(j_file)
                breaker = content['break']
                sleep(10)

    content["interruption"] = False 
    with open('controls.json', 'w') as j_file:  
        json.dump(content, j_file)
            
    return element_to_find, response


#%% Data Frame Builder Function

def df_builder(stage, search_subject, search_location):
    """ primary_df/secondary_df Builder
    it looks for a saved PRIMARY/SECONDARY csv file in the "record_file"
    given in the inputs to continue from it, and if it didn't find it
    it concatenates the individually saved primary/secondary csv files
    to one df and save it as PRIMARY/SECONDARY csv file depending 
    on the stage given primary/secondary.
    

    Args:
        stage (str): the last stage of the scraping process primary/secondary.
        search_subject (str): the subject of the search, used to 
            find the PRIMARY/SECONDARY file.
        search_location (str): the location of the search, used to 
            find the PRIMARY/SECONDARY file.

    Returns:
        df_ (pandas.DataFrame): primary_df/secondary_df which will be used 
            to continue the scraping process.

    """
    try:#checking if there was SECONDARY file already saved
        with open('./outputs/' + txt_tracker,'r') as file:
            contents = file.read()
        P_csv_file = re.search(f'Yelp {stage.upper()} {search_subject} in {search_location}.csv',contents)[0].replace('_', ' ')
        df_ = pd.read_csv('./outputs/' + P_csv_file) 
            
    except:
        
        with open('./outputs/' + txt_tracker,'r') as file:
             
            for line in file:
                
                if f'1 {stage.lower()}' in line:
                    df_0 = pd.read_csv('./outputs/' + str(line).strip('\n')) 
                    continue
                    
                elif 'https://' not in line and stage.lower() in line: #and ' 0 secondary' not in line
                    df_ = pd.concat([df_0, pd.read_csv('./outputs/' + str(line).strip('\n'))], axis=0, ignore_index= True)    
                    df_0 = df_
                    
        df_ = df_.drop_duplicates()
        df_.to_csv('./outputs/' + f"Yelp {stage.upper()} {search_subject} in {search_location}.csv", index= False)

        with open('./outputs/' + txt_tracker,'a') as file:         
            file.write('\n')
            file.write (f"Yelp {stage.upper()} {search_subject} in {search_location}.csv") 
            
    return df_

#%% Available Result Pages For A search

# this part looks for the number of pages available on the scraped site for a 
# given subject and location and adjusts the number of pages_to_scrape accordingly.
# the response of this page in case of a new scraping process is used to 
# scrape the data for the fist page in the primary stage.

ua = UserAgent().random
   
""" scraping the internet"""      
session = HTMLSession()
if primary_stage == True or pages_to_scrape > I_P:
    r = session.get(site)
    response = r.html
    # returns the number of results available for a given search
    results_available = int(response.find('div[class*="text-align--center"] > span')[0].text.split(" ")[-1]) 
    print(f'{results_available} pages available.')  
else:
    results_available = pages_to_scrape + 10 # just to stay ahead of the pages to scrape
    print("page available for the search are more than the pages to scrape !!")
      
pages_to_scrape = list(range(1,pages_to_scrape + 1))
    
if len(pages_to_scrape) > results_available:
    pages_to_scrape = pages_to_scrape[:results_available + 1]

#%% Primary Scraper variables 

previous_page = url

result_list =[]
"""contains the scraped info from the primary stage"""

results_scraped = 0
"""counts the scraped pages."""

next = False 
"""dictates if the primary scraper will starts scraping using the same response 
    used to get the results_available above or not
"""

#%% Primary Scraper

# this part "primary stage" scrapes each page and saves its outputs in csv file.
# it scrapes the profiles names and the business names.
if primary_stage == True or len(pages_to_scrape) > I_P:    
    
    for Page in tqdm(pages_to_scrape[I_P:] ,unit= "page", ncols = 110, colour= '#0A9092'):
        
        if next == True:
            non_sponsored, response = crawler(session, ua, site,'//h3/span/a', previous_page)
        else:# scrapes the first page 
            non_sponsored = response.xpath('//h3/span/a')
        
        if len(non_sponsored)>= 10:
            non_sponsored = non_sponsored[-10:]
        
        for B in non_sponsored:# iterates through the results 
            
            name = B.text
            profile = url[:20] + list(B.links)[0]
            # handling redirecting url 
            if "&request_id" in profile: 
                tru_url = profile[profile.index('url=https%')+len('url=https%'):profile.index('&request_id')]
                divider_mark = tru_url[tru_url.index('com')+len('com'):tru_url.index('biz')]
                profile = 'https://'+tru_url[tru_url.index('www'):].replace(divider_mark, '/')
            
            result = {"Business Name": name,
                      "Profile": profile
                      }

            for key, value in result.items():
                if value == '':
                    result[key] = "Not Listed"
                
            result_list.append(result)
        
        results_scraped += len(result_list)
        
        p_save_index  += 1
          
        p_df = pd.DataFrame(result_list)
        p_df.to_csv('./outputs/' + f"yelp {search_subject} in {search_location} {p_save_index} primary.csv", index= False)

        try:
            file = open('./outputs/' + txt_tracker ,'a') 
        except:
            file = open('./outputs/' + txt_tracker ,'w')             
        file.write('\n')
        file.write (f"yelp {search_subject} in {search_location} {p_save_index} primary.csv") 
        file.write('\n')
        file.write(F'{site}')   
        file.close() 
        
        result_list = []
        
        I_P = Page
        
        previous_page = None #site
        site = url + f"&start={I_P * 10}"
        next = True
        sleep(1)
        
        
    print(f"Primary is done, {results_scraped + len(result_list)} results Scraped successfully !!")
    
    
else:
    print('primary stage is already completed.')
    start_time = time.time()

#%% building the primary DataFrame

# building the PRIMARY DataFrame, inspect it's contents, save it as csv file
# & delete the individually saved primary csv files used in it's construction.
    
primary_df = df_builder('primary', search_subject, search_location)
primary_df.info()

print(primary_df.tail(), '\n')

files = os.listdir('./outputs/')
for file in files:
    if 'primary' in file :
        os.remove('./outputs/' + file)

#%% Secondary Scraper variables

# resetting the variables to be reused by the secondary scraper.

results_scraped = 0  
result_list = []

def extract_json(json_r):
    """ turns str extracted from httpx_html.Element into json
        it's made to be used in the crawler function to ensure
        that the crawler function returns the element.
    Args:
        json_r (str): uncleaned str which contains a json.
    Returns:
        data (dict): cleaned json format.
    """
    scripts = json_r[1].text
    head = scripts.index('{')
    back = len(scripts)-scripts[len(scripts)::-1].index('}')
    j_script =  scripts[head:back]                    
    data = json.loads(j_script)
    
    return data

#%% Secondary Scraper

# this part "secondary stage" scrapes each page and saves its outputs in csv file.
# it scrapes the Phone, Address, Website, Rating, Review Count for each profile page.
if secondary_stage == True:    
    ua = UserAgent().random
    
    start_time = time.time()
           
    for profile in tqdm(primary_df["Profile"][I_S:] ,unit= "profile", ncols = 110, colour= '#0A9092'):  
        
        data, response = crawler(session, ua, profile, '//script[@type="application/json"]', previous_page ,extract_json )
                
        address = website = phone = ratting = review ="Not Listed" 
        
        for D in data:
            if '.phoneNumber' in D: 
                try:
                    phone = data[D]['formatted']
                except:
                    pass
                
            if '.location.address' in D:    
                try:    
                    address = data[D]["formatted"]
                except:
                    pass    
                
            if '.externalResources.website' in D:    
                try:    
                    website = 'https://www.'+ data[D]['url'].split(';')[-1]
                except:
                    pass      
                              
            try:    
                ratting = int(data[D]['rating'])
            except:   
                pass
                      
            try:    
                review = int(data[D]['reviewCount'])
            except:
                pass
            
        result = {"Phone": phone,
                     "Address": address,
                     "Website": website,
                     "Rating": ratting,
                     "Review Count": review
                     }
        
        for key, value in result.items():
            if value == '':
                result[key] = "Not Listed"
                        
        result_list.append(result)
        
        if len(result_list) == 10: # saving the outputs to a csv file every 100 page
            results_scraped += len(result_list)
            
            I_S = primary_df[primary_df["Profile"]== profile].index.values.astype(int)[0]
            
            s_save_index += 1
            print(s_save_index)
        
            s_df = pd.DataFrame(result_list)
            s_df.to_csv('./outputs/' + f"Yelp {search_subject} in {search_location} {s_save_index} secondary.csv", index= False)  
            
            result_list = []
            
            file = open('./outputs/' + txt_tracker ,'a')
            file.write('\n')
            file.write (f"Yelp {search_subject} in {search_location} {s_save_index} secondary.csv") 
            file.write('\n')
            file.write(F'{I_S}')   
            file.close()
            
        previous_page = None #profile
        sleep(1)
         
    # to capture the unsaved profiles if there was any 
    if len(result_list) > 0 and primary_df[primary_df["Profile"]== profile].index.values.astype(int)[0] == len(primary_df)-1:
        I_S = primary_df[primary_df["Profile"]== profile].index.values.astype(int)[0]
        s_save_index += 1
           
        s_df0 = pd.DataFrame(result_list)
        s_df0.to_csv('./outputs/' + f"Yelp {search_subject} in {search_location} {s_save_index} secondary.csv", index= False)
        file = open('./outputs/' + txt_tracker ,'a')
        file.write('\n')
        file.write (f"Yelp {search_subject} in {search_location} {s_save_index} secondary.csv") 
        file.write('\n')   
        file.write(F'{I_S}')
        file.close()

    print(f"Secondary is done, {results_scraped + len(result_list)} results Scraped successfully !!")
    
else:
    print('secondary stage is already completed.')
    start_time = time.time() 

#%% building the secondary DataFrame

# building the SECONDARY DataFrame, inspect it's contents, save it as csv file 
# & delete the individually saved secondary csv files used in it's construction. 
    
secondary_df = df_builder('secondary', search_subject, search_location)
secondary_df.info()

print(secondary_df.tail(), '\n')

files = os.listdir('./outputs/')
for file in files:
    if 'secondary' in file :
        os.remove('./outputs/' + file) 

#%% End Result DataFrame

# building the end result data frame, inspect it's contents ,
# save it as csv file & delete the PRIMARY and SECONDARY files used to build it.

df = primary_df.join(secondary_df)
df.info()
print(df.tail(), '\n')
df.to_csv('./outputs/' + f"Yelp {search_subject} in {search_location}.csv", index= False)

files = os.listdir('./outputs/')
for file in files:
    if 'PRIMARY' in file or 'SECONDARY' in file or txt_tracker in file :
        os.remove('./outputs/' + file)
print("\nthe scraping has been done successfully.")