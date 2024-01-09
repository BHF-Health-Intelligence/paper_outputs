import requests
import pandas as pd 
import json
from datetime import datetime, timedelta  
import time
import xml.etree.ElementTree as ET
import numpy as np 
import shutil
import os


"""
    This script pulls article and journal data from EuropePMC

    See: https://europepmc.org/RestfulWebService#!/Europe32PMC32Articles32RESTful32API/fields

    Daniel O'Connell - British Heart Foundation - 2022 
    
"""

#Set some parameters
output_file="./output/papers.xlsx"
past_days=365*5

def days_ago(num_days):
    #Return a yyyy-mm-dd string that is num_days days ago
    return datetime.strftime(datetime.today()-timedelta(days=num_days), "%Y-%m-%d")  


def query_epmc(date_from,date_to,resultType="core",pagesize="100",res_format="json",sorttype="P_PDATE_D desc",nextcursormark="*"):
    
    #Function to get results from European PMC using sensible default values 
    
    #Form the link
    link="https://www.ebi.ac.uk/europepmc/webservices/rest/search?query=FIRST_PDATE%3A%5B{}%20TO%20{}%5D%20%26%20GRANT_AGENCY%3A%22BRITISH%20HEART%20FOUNDATION%22&resultType={}&cursorMark={}&pageSize={}&format={}&sort={}".format(date_from,date_to,resultType,nextcursormark,pagesize,res_format,sorttype.replace(" ","%20"))    
    return requests.get(link)
   

def get_nests(child,id):
    
    #Get the info contained in nested child nodes 
    tagnames=[]
    tagvals=[]
    order=[]
    
    for z,gchild in enumerate(list(child)): #Iterate over the grandchildren
        for i in gchild.iter(): #And the nodes within
            
            if type(i.text)!=type(None): #Filter out junk
                if i.text.strip()!="": #If this tag has something included
            
                    tagnames.append(i.tag) #Store tag name
                    tagvals.append(i.text) #value
                    order.append(z) #and the "order" - the group to which this tag belongs
    
    #Then for each group
    ret=pd.DataFrame()
    for ordy in pd.unique(order):
        
        #Get the relevant columns and values only
        cols=[x for x,y in zip(tagnames,order) if y==ordy ]
        vals=[x for x,y in zip(tagvals,order) if y==ordy ]
        
        #Form a dictionary to pair them up
        ret_dict={}
        for colname in pd.unique(cols):
            ret_dict[colname]=[y for x,y in zip(cols,vals) if x==colname]
        
        #Sometimes there are multiple values per key. Join them up here
        for key,val in ret_dict.items():
            if len(val)>1:
                ret_dict[key]=["; ".join(val)]
        
        #Convert dictionary to dataframe and append
        ret_=pd.DataFrame.from_dict(data=ret_dict)
        # ret=ret.append(ret_,ignore_index=True)
        ret = pd.concat([ret,ret_],ignore_index=True)
    
    #For some reason, journalInfo is stored differently, so implement this fix
    if child.tag=="journalInfo":
        ret=ret.bfill().iloc[0].to_frame().transpose()
        
    #Add the ID column for linking purposes 
    ret["id"]=id

    return ret


def read_xml(xml_string):
    
    #Get root of XML tree
    tree=ET.ElementTree(ET.fromstring(xml_string))
    root=tree.getroot()

    #Get the nextcursormark here
    cursor=[i.text for i in root.iter("nextCursorMark")]
    if cursor==[]:
        cursor=np.nan
        
    #Create an overall frame for unnested children 
    dat=pd.DataFrame()
    
    #Keep a running list of the variable names with nested nodes - these will be concatenated later 
    frames_with_children=[]
    
    #Iterate over all papers
    for i,paper in enumerate(root.iter('result')):
        # print(i)
        
        #Get the child nodes (ie the columns)
        for child in list(paper): 
            #If this child is nested (ie it also has children eg authorList)
            if list(child)!=[]:
                
                #Then parse those separately
                output=get_nests(child,id=list(paper)[0].text)  
                
                #Check if we have a list for this type of tag
                try:
                    exec("framelist_"+ child.tag)
                except NameError: #if we don't - create one
                    exec("framelist_"+ child.tag + " = []")
                    frames_with_children.append("framelist_"+ child.tag)
                
                #Then store output in the unique list
                exec("framelist_" + child.tag + ".append(output)") 
            
            else: #If the node is childless, store info here 
                try:
                    dat.at[i,child.tag]=child.text
                except ValueError: #This indicates the column is the wrong type - happens if the data hitherto is different type
                    dat[child.tag]=dat[child.tag].astype(object) #Convert to object (so we can mix types)
                    dat.at[i,child.tag]=child.text #Then insert the data 
                    
    #Put all the frames in a handy dictionary
    framedict=dict()
    framedict["fulldat"]=dat

    for frame in frames_with_children:
        exec("framedict['" + frame.split("_")[1]+"']=pd.concat("+frame+")")            
    
    #Add in nextcursormark 
    framedict["cursorMark"]=cursor
    
    return framedict


def process_data(dat_all):
    
    dat_full=dat_all["fulldat"]
    
    #If we have grants info, get it
    if ("grantsList") in dat_all.keys():
        dat_grants=dat_all["grantsList"].reset_index(drop=True)
    else: #If we don't, define an empty(ish) dataframe
        dat_grants=pd.DataFrame(data={"grantId":""},index=[0])
    
    #Same for journal info
    if ("journalInfo") in dat_all.keys():
        dat_journs=dat_all["journalInfo"].reset_index(drop=True)
    else: #If we don't, define an empty(ish) dataframe
        dat_journs=pd.DataFrame(data={"id":""},index=[0])


    #Preallocate
    dat_full["printPublicationDate"]=""
    dat_full["journalIssueId"]=""
    dat_full["ESSN"]=""
    dat_full["ISSN"]=""
    dat_full["journal_name"]=""


    for id in pd.unique(dat_full.id):
        
        ind=dat_full[dat_full.id==id].index[0]

        #If this paper id is in the grant info dataframe
        if id in dat_grants.id.values: 
            #Save the results
            dat_full.at[ind,"grants"]="; ".join([ x for x in dat_grants[dat_grants.id==id].grantId if (x!="") and (x==x)])

        #If this paper id is in the grant info dataframe
        if id in dat_journs.id.values: 
            #Save the results
            dat_full.at[ind,"journal_name"]="; ".join([ x for x in dat_journs[dat_journs.id==id].title if (x!="") and (x==x)])
            dat_full.at[ind,"ISSN"]="; ".join([ x for x in dat_journs[dat_journs.id==id].ISSN if (x!="") and (x==x)])
            dat_full.at[ind,"ESSN"]="; ".join([ x for x in dat_journs[dat_journs.id==id].ESSN if (x!="") and (x==x)])
            dat_full.at[ind,"journalIssueId"]="; ".join([ x for x in dat_journs[dat_journs.id==id].journalIssueId if (x!="") and (x==x)])
            dat_full.at[ind,"printPublicationDate"]="; ".join([ x for x in dat_journs[dat_journs.id==id].printPublicationDate if (x!="") and (x==x)])
        
        
    return dat_full


def read_archive():
    # return pd.read_csv(output_file)
    return pd.read_excel(output_file)


def highlight_rows(row):
    #Nicked from the net
    value = row.loc['new']
    if value == True:
        colour = '#e7f20a' #Yellow
    else:
        colour = '#ffffff'
    return ['background-color: {}'.format(colour) for r in row]



if __name__=="__main__":

    #First, backup the current archive:
    shutil.copy2(output_file,os.path.join("backups","backup.xlsx"))

    #And then read it into memory:
    dat_arc=read_archive()

    #Ensure the list is of unique articles only 
    dat_arc=dat_arc.drop_duplicates(subset="DOI")

    #Then, get results between today and a previously defined date
    print("Searching for articles between",days_ago(past_days)+" and "+days_ago(0))
    results=query_epmc(date_from=days_ago(past_days),date_to=days_ago(0),res_format="xml")

    #If the code comes back bad, exit 
    if results.status_code!=200:
        raise Exception("Status code is: {}".format(results.status_code))
    else: #Otherwise, parse the results
        dat_all=read_xml(results.text)
        dat_full=process_data(dat_all)

    #Get any remaining results using the cursormarks 
    stop=0 #dummy variable
    res_list=[dat_full]
        
        
    print("Got results from",dat_full.firstPublicationDate.min(),"to",dat_full.firstPublicationDate.max())  
        
    while stop==0:
        if pd.notna(dat_all["cursorMark"]): #is there a next cursor mark? This indicates there are more results
            #So fetch them
            print("Getting more results...")
            results=query_epmc(date_from=days_ago(past_days),date_to=days_ago(0),nextcursormark=dat_all["cursorMark"][0],res_format="xml")
            dat_all=read_xml(results.text)

            
            #If the full dataframe is empty, then we must have all the results, so break the loop
            if dat_all["fulldat"].empty:
                print("Got all results")
                stop=1
                break
            
            #Otherwise, parse and store results
            dat_full=process_data(dat_all)
            res_list.append(dat_full)
            print("Got results from",dat_full.firstPublicationDate.min(),"to",dat_full.firstPublicationDate.max())  

            time.sleep(1) #Always be polite
            
        else: #If there is no mark, then we have all results 
            print("Got all results")
            stop=1
    
    #Concatenate the results      
    dat_full=pd.concat(res_list) 

    #Clean up the frame
    dat_full["pubYear"]=dat_full["dateOfCompletion"]
    
    rename_dict={"authorString":"Authors",
                 "title":"Title",
                 "pmid":"PMID",
                 "id":"ID",
                 "doi":"DOI",
                 "pubYear":"Date of Completion",
                 "abstractText":"Abstract",
                 "journal_name":"Journal Name",
                 "printPublicationDate":"Print Publication Date",
                 "journalIssueId":"Journal Issue ID",
                 "electronicPublicationDate":"Electronic Publication Date",
                 "grants":"Grants",
                 "source":"Source",
                 "affiliation":"Affiliation"}
    
    
    dat_full=dat_full.rename(mapper=rename_dict,axis=1)
   
    #Select columns we want to keep:
    keep_cols=["Source",
    "PMID",
    "DOI",
    "Title",
    #"Date of Completion",
    "Electronic Publication Date",
    "Print Publication Date",
    "Authors",
    "Affiliation",
    "Journal Name",
    #"Journal Issue ID",
    "Grants",
    #"ISSN",
    #"ESSN",
    "Abstract"]
    
    dat_full=dat_full[keep_cols]

    #Add in today's date
    dat_full["Date Added"]=days_ago(0)

    #Find the new articles based on title
    #dat_full["new"] = ~dat_full.Title.isin(dat_arc.Title)

    #Find the new articles based on DOI
    dat_full["new"] = ~dat_full.DOI.isin(dat_arc.DOI)
    
    print("Adding",str(dat_full.new.sum()),"new articles")
    
    #Clean the text a little:
    dat_full.DOI=dat_full.DOI.str.strip()
    dat_full.Title=dat_full.Title.str.strip()

    #Combine the full data with the archived data and drop duplicates based on DOI
    dat_arc["new"]=False
    dat=pd.concat([dat_arc,dat_full]).drop_duplicates(subset=["DOI"])

    #Sort by publish date:
    dat=dat.sort_values(by=["new","Print Publication Date"],ascending=[False,False]).reset_index(drop=True)

    #Format the new articles by highlighting the new rows
    dat=dat.style.apply(highlight_rows,axis=1)

    #Save output
    dat.to_excel(output_file,encoding='utf-8',index=False,columns=[col for col in dat.columns if col!="new"])