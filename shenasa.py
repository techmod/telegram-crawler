import pandas as pd
import numpy as np
import requests
import subprocess
import os
import json

post_url = "http://192.168.115.153:8000/api/news/"
headers = {'Content-type': 'application/json'}
##################### delete previous new_files
files_in_directory = os.listdir('C:/telegram-messages-dump-master/new_NEWS')
filtered_files = [file for file in files_in_directory if file.endswith(".csv")]
for file in filtered_files:
	path_to_file = os.path.join('C:/telegram-messages-dump-master/new_NEWS', file)
	os.remove(path_to_file)
##########################recieve manifest###########################
url='http://192.168.115.153:8000/api/manifest/'
r=requests.get(url)
data = r.json()
print(data['count'])
t=data['count']
print(t)
l = data['results'][t-1]['resources']
telegarm_channel=[d['name'] for d in l if 'name' in d]
recive_manifest=[d['name'] for d in l if 'name' in d]
print(telegarm_channel)
######################################################################
############################

telegram_query=[]
telegram_query_1=[]
for i in recive_manifest:
        #print(";telegram-messages-dump -p 98910******* -o C:"+"\\"+"telegram-messages-dump-master\\{}.csv --continue".format(i))
           
        telegram_query.append(";telegram-messages-dump -p 98910******* -o C:"+"\\"+"telegram-messages-dump-master\\update\\{}.csv --continue".format(i)) #rename phone number!!!
telegram_query_1=''.join(telegram_query)    
print(telegram_query_1)
print(len(telegram_query))


#text_file = open("/home/armin/Documents/update.ps1", "w")######## please modify aderss
text_file = open("C:/telegram-messages-dump-master/update.ps1", "w")
text_file.write(telegram_query_1)
text_file.close()
telegram_query.clear()

#crawler batch file
#subprocess.call([r'/home/armin/Documents/update.bat'])
subprocess.call([r'C:/telegram-messages-dump-master/update.bat'])######## please modify aderss


################################recieve resources############
url='http://192.168.115.153:8000/api/resource/'
r=requests.get(url)
resources= r.json()
t=len(resources['results'])

resource_name=[]
for j in range(len(resources['results'])):
  resource_name.append(resources['results'][j]['name'])
print(resource_name)
###############################################################

######################################read base csv########################
for i in range(len(telegarm_channel)):
  print(i)
  print(telegarm_channel)
  resource_name[i]= pd.read_csv (r'C:/telegram-messages-dump-master/base/{}.csv'.format(telegarm_channel[i]))#first csv crawled  ######## please modify aderss

print(resource_name[0])


########################################## read updated csv ################
for i in range(len(telegarm_channel)):
  print(i)
  print(telegarm_channel[i])
  telegarm_channel[i]= pd.read_csv (r'C:/telegram-messages-dump-master/update/{}.csv'.format(telegarm_channel[i])) ######## please modify aderss

print(telegarm_channel[0])
df1=pd.DataFrame()
for i in  range(len(telegarm_channel)):
  number_of_new_News=(len(telegarm_channel[i].iloc[:,0].values.tolist()))-(len(resource_name[i].iloc[:,0].values.tolist()))
  platform=[]
  platform_name='تلگرام'
  for j in range(number_of_new_News):
    platform.append(platform_name)
    
  link_number=telegarm_channel[i].iloc[:,0].values.tolist()[len(resource_name[i].iloc[:,0].values.tolist()):len(telegarm_channel[i].iloc[:,0].values.tolist())]
   
  News_link=[]
  for k in range(number_of_new_News):
    News_link.append('https://t.me/'+str(recive_manifest[i])+'/'+str(link_number[k]))    

  json_file = {'Platform':platform,'Message_Link':News_link,'Message_Time':telegarm_channel[i].iloc[:,1].values.tolist()[len(resource_name[i].iloc[:,0].values.tolist()):len(telegarm_channel[i].iloc[:,0].values.tolist())],
  'Sender_Name':telegarm_channel[i].iloc[:,2].values.tolist()[len(resource_name[i].iloc[:,0].values.tolist()):len(telegarm_channel[i].iloc[:,0].values.tolist())],
  'Message':telegarm_channel[i].iloc[:,4].values.tolist()[len(resource_name[i].iloc[:,0].values.tolist()):len(telegarm_channel[i].iloc[:,0].values.tolist())]}
  df=pd.DataFrame(json_file)
  pd.DataFrame(json_file).to_csv('C:/telegram-messages-dump-master/new_NEWS/{}.csv'.format(recive_manifest[i]),index=0) ######## please modify aderss
  telegarm_channel[i].to_csv('C:/telegram-messages-dump-master/base/{}.csv'.format(recive_manifest[i]),index=0)     ######## please modify aderss
  df1=pd.concat([df1,df])
  print(number_of_new_News)
df1.to_csv('C:/telegram-messages-dump-master/new_NEWS/append_News.csv',index=0)#####added

#print(number_of_new_News)
#print(len(telegarm_channel))
    
  #News_link_number=telegarm_channel[i].iloc[:,0].values.tolist()[len(resource_name[i].iloc[:,0].values.tolist()):len(telegarm_channel[i].iloc[:,0].values.tolist())
    
for i in range(len(df1.iloc[:,0])):
  data={'Platform':df1.iloc[i,0],'Message_Link':df1.iloc[i,1],'Message_Time':df1.iloc[i,2],'Sender_Name':df1.iloc[i,3],'Message':df1.iloc[i,4]}
  print(df1.iloc[i,4])
  r = requests.post(post_url, data=json.dumps(data), headers=headers)