import googleapiclient.discovery 
from googleapiclient.discovery import build
import pymysql
import pandas as pd
from pymongo import MongoClient
import streamlit as st
import time as t

api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyDsuWe5FK6Q8VGTV9iNOExCwDQGis8KhbI'
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)


def channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics", id=channel_id)
    response = request.execute()
    print(response)
    # lst=[]

    for i in response['items']:
        ch_data=dict(
        Channel_Id=i['id'],
        Channel_Name=i['snippet']['title'],
        Subscription_Count=i['statistics']['subscriberCount'],
        view_count=i['statistics']['viewCount'],
        Channel_Description=i['snippet']['description'],
        Channel_pAt=i['snippet']['publishedAt'],
        Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']) 
        # lst.append(ch_data)
    return ch_data


# GETTING VIDEO_ID DETAILS
def get_videos_ids(channel_id):
    video_ids = []
    request = youtube.channels().list(id=channel_id, part='contentDetails')
    response = request.execute()
    for i in response ['items']:
        Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads']
    
    token = None

    while True:
        request1 = youtube.playlistItems().list(
            part='contentDetails',
            maxResults=50,
            pageToken=token,
            playlistId=Playlist_Id)
        response1 = request1.execute()

        for i in response1['items']:
            video_ids.append(i['contentDetails']['videoId'])

        if 'nextPageToken' in response:
            token = response1.get('nextPageToken')
        else:
            break
    return video_ids



# TO GET each VIDEO DETAILS:  
def video_details(video_ids_list):
    video_info_list = []
    for i in video_ids_list:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i
        )
        response = request.execute()
        if 'items' in response and response['items']:
            data = dict(Channel_Name=response['items'][0]['snippet']['channelTitle'],
                Channel_Id=response['items'][0]['snippet']['channelId'],
                video_id=response['items'][0]['id'],
                video_title=response['items'][0]['snippet']['title'],
                video_description=response['items'][0]['snippet']['description'],
                tags=response['items'][0]['snippet'].get('tags', []),
                pAt=response['items'][0]['snippet']['publishedAt'],
                comment_count=response['items'][0]['statistics'].get('commentCount'),
                like_count=response['items'][0]['statistics'].get('likeCount'),
                view_count=response['items'][0]['statistics'].get('viewCount'),
                fav_count=response['items'][0]['statistics'].get('favoriteCount'),
                thumbnail=response['items'][0]['snippet']['thumbnails'],
                duration=response['items'][0]['contentDetails']['duration']
            )
            video_info_list.append(data)
        else:
            video_info_list.append(None)
    return video_info_list


def comment_details(video_info_list):
    all_com = []
    for j in video_info_list:
        
        try: 
            request = youtube.commentThreads().list(
            part="snippet,replies",
            videoId=j)
            response_com = request.execute()

            for i in range (len(response_com['items'])):
                data_comments = dict(
                Comment_Id=response_com['items'][i]['snippet']['topLevelComment']['snippet']['authorChannelId']['value'],
                Comment_Author=response_com['items'][i]['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                Comment_Text=response_com['items'][i]['snippet']['topLevelComment']['snippet']['textDisplay'],
                Comment_PublishedAt=response_com['items'][i]['snippet']['topLevelComment']['snippet']['publishedAt'],
                video_Id=response_com['items'][i]['snippet']['topLevelComment']['snippet']['videoId'])
                all_com.append(data_comments)

        except :
            None

    return all_com          


import pymongo
from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
newdb = client['Youtube_data_harvest']


def data_harvest (channel_id):
    video_ids_list=get_videos_ids(channel_id)
    channel_data=channel_details(channel_id)
    video_detail=video_details(video_ids_list)
    comment_details_data = comment_details(video_ids_list) 
        
    data= {
    'Channel Data': channel_data,
    'Video Data': video_detail,
    'Comment_Data': comment_details_data 
    }
    newcol = newdb['channel_data']
    newcol.insert_one(data)
    return "upload completed successfully"


connection_params = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Nahid123',
}
connection = pymysql.connect(**connection_params)
cursor = connection.cursor()
database_name = 'Youtube_data_harvest'
cursor.execute(f"CREATE DATABASE IF NOT EXISTS {database_name}")
connection = pymysql.connect(host= 'localhost',user = 'root',password = 'Nahid123',database = 'Youtube_data_harvest')
cursor = connection.cursor()

def channels_table():
    # drop_query='''DROP TABLE IF EXISTS channels'''
    # cursor.execute(drop_query)
    # connection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS channels (Channel_Name VARCHAR(100),
                                                            Channel_Id VARCHAR(100) PRIMARY KEY,
                                                            Subscription_Count BIGINT,
                                                            view_count BIGINT,
                                                            Channel_Description TEXT,
                                                            Channel_pAt VARCHAR(50),
                                                            Playlist_Id VARCHAR(50))'''
                                                        
                                                        
    cursor.execute(create_query)
        
    ch_list = []
    newdb = client['Youtube_data_harvest']
    newcol = newdb['channel_data']

    for c in newcol.find({}, {'_id': 0, 'Channel Data': 1}):
        ch_list.append(c['Channel Data'])
    df = pd.DataFrame(ch_list)

    for index, row in df.iterrows():
        insert_query = '''
        INSERT INTO channels(Channel_Name,
                                Channel_Id,
                                Subscription_Count,
                                view_count,
                                Channel_Description,
                                Channel_pAt,
                                Playlist_Id)
        VALUES(%s,%s,%s,%s,%s,%s,%s)
        '''

        values = (
            row['Channel_Name'],
            row['Channel_Id'],
            row['Subscription_Count'],
            row['view_count'],
            row['Channel_Description'],
            row['Channel_pAt'].replace("T","").replace("Z",""),
            row['Playlist_Id']
        )
        cursor.execute(insert_query, values)
    connection.commit()



def videos_table():
    try:
        # drop_query='''DROP TABLE IF EXISTS videos'''
        # cursor.execute(drop_query)
        cursor.execute('''CREATE TABLE IF NOT EXISTS videos(
                                                            Channel_Name VARCHAR(500),
                                                            Channel_Id VARCHAR(50),
                                                            video_id VARCHAR(50) PRIMARY KEY,
                                                            video_title VARCHAR(500),
                                                            video_description VARCHAR(5000),
                                                            tags VARCHAR(500),
                                                            pAt VARCHAR(100),
                                                            comment_count BIGINT,
                                                            like_count BIGINT,
                                                            view_count BIGINT,
                                                            fav_count INT,
                                                            thumbnail VARCHAR(100),
                                                            duration VARCHAR(100))''')

        vi_list = []
        newdb = client['Youtube_data_harvest']
        newcol = newdb['channel_data']
        for v in newcol.find({}, {'_id': 0, 'Video Data': 1}):
            for i in range(len(v['Video Data'])):
                vi_list.append(v['Video Data'][i])
        df2 = pd.DataFrame(vi_list)

        for index, row in df2.iterrows():
            insert_query= '''INSERT INTO videos(Channel_Name,Channel_Id,video_id,video_title,video_description,tags,
                            pAt,comment_count,like_count,view_count,fav_count,thumbnail,duration)
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

            values=(row['Channel_Name'],
                    row['Channel_Id'],
                    row['video_id'],
                    row['video_title'],
                    row['video_description'],
                    ",".join(row['tags']),
                    row['pAt'].replace('T','').replace('Z',''),
                    row['comment_count'],
                    row['like_count'],
                    row['view_count'],
                    row['fav_count'],
                    row['thumbnail']['default']['url'],
                    row['duration'].replace('PT', '').replace('H', ':').replace('M', ':').split('S')[0])         
            cursor.execute(insert_query,values) 
        connection.commit() 
    except:
        connection.commit()
videos_table()

def comments_table():
    # drop_query='''DROP TABLE IF EXISTS comments'''
    # cursor.execute(drop_query)
    
    create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(100) primary key,
                                                                  video_id VARCHAR(50),
                                                                  Comment_Text VARCHAR(1000),
                                                                  Comment_Author VARCHAR(500),
                                                                  Comment_PublishedAt VARCHAR(100)
                                                                  )'''

    cursor.execute(create_query)
    connection.commit()
    com_list = []
    newdb = client['Youtube_data_harvest']
    newcol = newdb['channel_data']
    for c in newcol.find({}, {'_id': 0, 'Comment_Data': 1}):
        for i in range(len(c['Comment_Data'])):
            com_list.append(c['Comment_Data'][i])
    df3 = pd.DataFrame(com_list)

    insert_query = '''INSERT IGNORE INTO comments(
                          Comment_Id,
                          video_Id,
                          Comment_Text,
                          Comment_Author,
                          Comment_PublishedAt)
                      VALUES (%s, %s, %s, %s, %s)'''


    for index, row in df3.iterrows():
        values = (
            row['Comment_Id'],
            row['video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            row['Comment_PublishedAt']
        )

        cursor.execute(insert_query, values)
    connection.commit()



def tables():
    channels_table()
    videos_table()
    comments_table()
    return 'Tables created successfully'


st.title(':green[WELCOME TO STREAMLIT]')
st.header(':red[YOUTUBE DATA HARVEST]')
st.caption(':blue[Data fetching Using Python, API key]')
st.caption(':blue[Data integration using MongoDB & Mysql]')
st.sidebar.title(":green[GUVI]")
st.sidebar.title(":red[Created by NAHID]")
st.sidebar.select_slider("Rate your experience",["Bad","Average","Good","Outstanding"])
st.info("Fetch Youtube channels data")

channel_id = st.text_input("Enter the channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]
input_channel_id = channel_id
input_channel_id = []

if st.button('show channel data'):
    output = channel_details(channel_id)
    st.success(output) 
with st.spinner("Your data is being processed"):
    t.sleep(2)  

if st.button('Migrate to MongoDB'):  
    for channel_id in channels:
        cd=[]
        client = MongoClient('mongodb://localhost:27017/')
        newdb = client['Youtube_data_harvest']
        newcol=newdb['channel_data']
        for ch_data in newcol.find({}, {'_id': 0, 'Channel Data': 1}):
                display=cd.append(ch_data['Channel Data']['Channel_Id'])
        if channel_id in cd:
            st.success('Channel Details of the Given ID Already Exists') 
            st.write(display) 
        for ch_data in newcol.find({'Channel Data.Channel_Id': channel_id}, {'_id': 0, 'Channel Data': 1}):
            st.write('Channel Data:', ch_data['Channel Data'])
                
        for vid_d in newcol.find({'Channel Data.Channel_Id': channel_id}, {'_id': 0, 'Channel Data': 1, 'Video Data': 1}):
            if 'Video Data' in vid_d:
                st.write('Video Data:', vid_d['Video Data'])
                    
        else:
            insert = data_harvest(channel_id)
            st.success(insert)
            st.write("Channel details successfully updated in MongoDB")     
    st.success(insert)
            
 

if st.button('Migrate to Sql') and input_channel_id !="":
    if channel_id in channels:
        st.write("Channel data tables already exists!!!")
         
    else:
        st.success(tables())
        st.write("Channel data successfully updated to Mysql")


    show = st.radio('SELECT THE TABLE to VIEW', ('channels', 'videos', 'comments'))

    def show_channel_tab():
            channels_list=[]
            newdb=client["Youtube_data_harvest"]
            newcol=newdb['channel_data']
            for chdata in newcol.find({},{"_id":0,"Channel Data":1}):
                channels_list.append(chdata["Channel Data"])
            channel_tab=pd.DataFrame(channels_list)
            return channel_tab

                
    def show_video_tab():
        videos_list=[]
        newdb=client["Youtube_data_harvest"]
        newcol=newdb['channel_data']
        for vidata in newcol.find({},{'_id':0,'Video Data':1}):
            for i in range(len(vidata['Video Data'])):
                videos_list.append(vidata['Video Data'][i])
        video_tab=pd.DataFrame(videos_list)
        return video_tab


    def show_comnt_tab():
        comment_list = []
        newdb=client["Youtube_data_harvest"]
        newcol=newdb['channel_data']
        for com_data in newcol.find({}, {'_id': 0, 'Comment_Data': 1}):
            for i in range(len(com_data['Comment_Data'])):
                comment_list.append(com_data['Comment_Data'][i])
        comnt_tab = pd.DataFrame(comment_list)
        return comnt_tab

    if show == "channels":
        Display = st.write(show_channel_tab())
        st.balloons()
        
    elif show == "videos":
        Display = st.write(show_video_tab())
        st.balloons()
        
    elif show == "comments":
        Display = st.write(show_comnt_tab())
        st.balloons()
    
    question = st.selectbox('Select your question', ('1.Display the channel names',
                                                    '2.Channels with highest Subscribers',
                                                    '3.Top 10 most viewed videos',
                                                    '4.10 Videos with highest comments',
                                                    '5.Top 10 Videos with highest likes',
                                                    '6.channel name of highest liked video',
                                                    '7.Names of all the videos and their corresponding channels',
                                                    '8.Videos with likes and their corresponding channel names',
                                                    '9.Average duration of all videos in each channel',
                                                    '10.Number of views of each channel'
                                                    ))


    if question == '1.Display the channel names':
        query1 = '''select Channel_Name from channels;'''
        cursor.execute(query1)
        t1=cursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Channel_Name"]))

    elif question == '2.Channels with highest Subscribers':
        query2 = '''select Channel_Name,Subscription_Count from channels order by Subscription_Count;'''
        cursor.execute(query2)
        t2=cursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel_Name","Subscription_Count"]))

    elif question == '3.Top 10 most viewed videos':
        query3 = '''select view_count,Channel_Name,video_title from videos where view_count is not null order by view_count desc 
        limit 10;'''
        cursor.execute(query3)
        t3=cursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Channel_Name","video_title","view_count"]))

    elif question == '4.10 Videos with highest comments':
        query4 = '''select comment_count, video_title from videos where comment_count is not null order by comment_count desc
        limit 10;'''
        cursor.execute(query4)
        t4=cursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["video_title","comment_count"]))

    elif question == '5.Top 10 Videos with highest likes':
        query5 = '''select Channel_Name, video_title,like_count from videos where like_count is not null order by like_count 
        desc limit 10;'''
        cursor.execute(query5)
        t5=cursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Channel_Name","video_title","like_count"]))

    elif question == '6.channel name of highest liked video':
        query6 = '''select Channel_Name, video_title, like_count from videos where like_count is not null order 
        by like_count desc limit 1 ;'''
        cursor.execute(query6)
        t6=cursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Channel_Name","video_title","like_count"]))

    elif question == '7.Names of all the videos and their corresponding channels':
        query7 = '''select Channel_Name, video_title from videos;'''
        print("Executing query:", query7)
        cursor.execute(query7)
        t7=cursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel_Name","video_title"]))


    elif question == '8.Videos with likes and their corresponding channel names':
        query8 = '''select Channel_Name, video_title, like_count from videos where like_count is not null order by 
        like_count desc;''' 
        cursor.execute(query8)
        t8=cursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Channel_Name", "video_title", "like_count"]))

    elif question == '9.Average duration of all videos in each channel':
        query9 = '''select Channel_Name,avg(duration)from videos group by Channel_Name;'''
        cursor.execute(query9)
        t9=cursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel_Name","duration"]))

    elif question == '10.Number of views of each channel':
        query10 = '''select Channel_Name, video_title , view_count from videos where view_count is not null;'''
        cursor.execute(query10)
        t10=cursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Channel_Name","video_title","view_count"]))
        st.balloons()


            
