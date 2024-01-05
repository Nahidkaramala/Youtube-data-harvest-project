import streamlit as st
from pprint import pprint
import googleapiclient.discovery 
from googleapiclient.discovery import build
import pymysql
import pandas as pd
from pymongo import MongoClient

api_service_name = "youtube"
api_version = "v3"
api_key='AIzaSyDsuWe5FK6Q8VGTV9iNOExCwDQGis8KhbI'
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=api_key)
channel_id="UCvF499ChQBnWb5ex162EiMg"

def channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    response_ch = request.execute()
    # print("API Response:", response_ch)

    if 'items' in response_ch and response_ch['items']:
        ch_data = dict(
            Channel_Name=response_ch['items'][0]['snippet']['title'],
            Channel_Id=response_ch['items'][0]['id'],
            Subscription_Count=response_ch['items'][0]['statistics']['subscriberCount'],
            view_count=response_ch['items'][0]['statistics']['viewCount'],
            Channel_Description=response_ch['items'][0]['snippet']['description'],
            Channel_pAt=response_ch['items'][0]['snippet']['publishedAt'],
            Playlist_Id=response_ch['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        )
        return ch_data
          

    # for i in response_ch['items']:
    #     ch_data=dict(
    #     Channel_Name=response_ch['items'][0]['snippet']['title'],
    #     Channel_Id=response_ch['items'][0]['id'],
    #     Subscription_Count=response_ch['items'][0]['statistics']['subscriberCount'],
    #     view_count=response_ch['items'][0]['statistics']['viewCount'],
    #     Channel_Description=response_ch['items'][0]['snippet']['description'],
    #     Channel_pAt=response_ch['items'][0]['snippet']['publishedAt'],
    #     Playlist_Id=response_ch['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    # return ch_data
ch_d=channel_details('channels_id')

def playlist_id(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response_playlist_id = request.execute()
    
    if 'items' in response_playlist_id and response_playlist_id['items']:
        playlist_id = response_playlist_id['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        return playlist_id   
    # else:
    #     return None
playlist_id_value = playlist_id(channel_id)

# GETTING VIDEO_ID DETAILS

def get_videos_ids(playlistId):
 
    video_ids = []
    token = None

    while True:
        request = youtube.playlistItems().list(
        part='snippet',
        maxResults=50,
        pageToken=token,
        playlistId=playlist_id_value)
        response=request.execute()

        for i in response['items']:
            video_ids.append(i['snippet']['resourceId']['videoId'])
        if 'nextPageToken' in response:
            token = response.get('nextPageToken')
        else:
            break
    return video_ids
video_ids_list = get_videos_ids(playlist_id_value)

# TO GET each VIDEO DETAILS:  
def video_details(video_ids_list):
    video_info_list = []
    for i in video_ids_list:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=i)
        response = request.execute()
        if 'items' in response and response['items']:
            data = dict(Channel_Name=response['items'][0]['snippet']['channelTitle'],
                Channel_Id=response['items'][0]['snippet']['channelId'],
                video_id=response['items'][0]['id'],
                video_title=response['items'][0]['snippet']['title'],
                video_description=response['items'][0]['snippet']['description'],
                tags=response['items'][0]['snippet'].get('tags', []),
                pAt=response['items'][0]['snippet']['publishedAt'],
                comment_count=response['items'][0]['statistics'].get('commentCount', []),
                like_count=response['items'][0]['statistics']['likeCount'],
                view_count=response['items'][0]['statistics']['viewCount'],
                fav_count=response['items'][0]['statistics']['favoriteCount'],
                thumbnail=response['items'][0]['snippet']['thumbnails'],
                duration=response['items'][0]['contentDetails']['duration']
            )
            video_info_list.append(data)
        else:
            video_info_list.append(None)

    return video_info_list
VD=video_details(video_ids_list)

def comment_details(video_ids_list):
    all_com = []
    for j in video_ids_list:
        
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
cm_d=comment_details(video_ids_list)


def data_harvest (channel_id):
    _id= channel_id
    channel_data=channel_details(channel_id)
    playlist_details=get_videos_ids(playlist_id_value)
    video_detail=video_details(video_ids_list)
    comment_details_data = comment_details(video_ids_list) 
        
    data= {
    'Channel Data': channel_data,
    'Video Data': video_detail,
    'Comment_Data': comment_details_data 
    }

    return data
data_harvest('channels_id')


from pymongo import MongoClient
client = MongoClient('mongodb://localhost:27017/')
newdb = client['Youtube_data_harvest']
newcol = newdb['channel_data']

def mongo_data_transfer(channel_id):
    channel_data =ch_d
    playlist_details = playlist_id_value
    video_detail = VD
    comment_details_data = cm_d

    channel= {
        'Channel Data': channel_data,
        'Playlist Data': playlist_details,
        'Video Data': video_detail,
        'Comment Data': comment_details_data
    }

    newcol.insert_one(channel) 
# mongo_data_transfer('channels_id')


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
    drop_query='''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_query)
    connection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS channels (Channel_Name VARCHAR(100),
                                                            Channel_Id VARCHAR(100) PRIMARY KEY,
                                                            Subscription_Count BIGINT,
                                                            view_count BIGINT,
                                                            Channel_Description TEXT,
                                                            Channel_pAt VARCHAR(50),
                                                            Playlist_Id VARCHAR(50))'''
                                                        
                                                        
    cursor.execute(create_query)
    connection.commit()
    
    ch_list = []
    db = client['Youtube_data_harvest']
    newcol = db['channel_data']

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

channels_table()


def videos_table():
    try:
        drop_query='''DROP TABLE IF EXISTS videos'''
        cursor.execute(drop_query)

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
        db = client['Youtube_data_harvest']
        newcol = db['channel_data']
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
    drop_query='''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    connection.commit()
    create_query = '''CREATE TABLE IF NOT EXISTS comments(Comment_Id varchar(100) primary key,
                                                                  video_id VARCHAR(50),
                                                                  Comment_Text VARCHAR(1000),
                                                                  Comment_Author VARCHAR(500),
                                                                  Comment_PublishedAt VARCHAR(100)
                                                                  )'''

    cursor.execute(create_query)
    connection.commit()
    com_list = []
    db = client['Youtube_data_harvest']
    newcol = db['channel_data']
    for c in newcol.find({}, {'_id': 0, 'Comment Data': 1}):
        for i in range(len(c['Comment Data'])):
            com_list.append(c['Comment Data'][i])
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
comments_table()


def tables():
    channels_table()
    videos_table()
    comments_table()

    return 'Tables created successfully'


def show_channel_tab():
        channels_list=[]
        database_name=client["Youtube_data_harvest"]
        newcol=database_name['channel_data']
        for chdata in newcol.find({},{"_id":0,"Channel Data":1}):
            channels_list.append(chdata["Channel Data"])
        channel_tab=pd.DataFrame(channels_list)
        return channel_tab
            
def show_video_tab():
    videos_list=[]
    database_name=client["Youtube_data_harvest"]
    newcol=database_name['channel_data']
    for vidata in newcol.find({},{'_id':0,'Video Data':1}):
        for i in range(len(vidata['Video Data'])):
            videos_list.append(vidata['Video Data'][i])
    video_tab=pd.DataFrame(videos_list)
    return video_tab

def show_comnt_tab():
    comment_list=[]
    database_name=client["Youtube_data_harvest"]
    newcol=database_name['channel_data']
    for com_data in newcol.find({}, {'_id': 0, 'Comment Data': 1}):
        for i in range(len(com_data['Comment Data'])):
            comment_list.append(com_data['Comment Data'][i])
    comnt_tab= pd.DataFrame(comment_list)
    return comnt_tab

st.title(':red[YouTube Data Harvesting & Warehousing] ' )
channel_id = st.text_input("Enter the channel ID")
channels = channel_id.split(',')
channels = [ch.strip() for ch in channels if ch]
     
with st.sidebar:
    st.markdown(
    """
    <style>
        div.sidebar {
            background-color: #ff69b4;  /* Pink background color */
        }
        h1 {
            color: #00008B;  /* Dark blue font color for h1 elements */
        }
    </style>
    """,
    unsafe_allow_html=True
)
    st.header(':blue[Skill Take Away]')
    st.caption(':blue[Python Scripting]')
    st.caption(':blue[Data Collection]')
    st.caption(':green[MongoDB]')
    st.caption(':pink[API Integration]')
    st.caption(':skyblue[Data fetching using MongoDB and SQL]')
col1,col2=st.columns(2)
with col1:
    if st.button('Collect and Store data'):
        for channel_id in channels:
            ch_ids=[]
            database_name=client["Youtube_data_harvest"]
            newcol=database_name['channel_data']
            for ch_data in newcol.find({}, {'_id': 0, 'Channel Data': 1}):
                ch_ids.append(ch_data['Channel Data']['Channel_Id'])

            if channel_id in ch_ids:
                st.success('Channel Details of the Given ID Already Exists')

            else:
                output = channel_details(channel_id)
                st.success(output)

with col2:
    if st.button('Migrate to Sql'):
        Display= tables()
        st.success(Display)

show = st.radio('SELECT THE TABLE FOR VIEW', ('CHANNELS', 'VIDEOS', 'COMMENTS'))

if show == "CHANNELS":
    Display=st.write(show_channel_tab())
    st.markdown(f'<div style="color: green; font-size: 20px;">{Display}</div>', unsafe_allow_html=True)

elif show == "VIDEOS":
    Display=st.write(show_video_tab())
    st.markdown(f'<div style="color: blue; font-size: 20px;">{Display}</div>', unsafe_allow_html=True)
    
elif show == "COMMENTS":
    Display= st.write(show_comnt_tab())
    st.markdown(f'<div style="color: orange; font-size: 20px;">{Display}</div>', unsafe_allow_html=True)
    
#SQL connection    
connection = pymysql.connect(
                            host= 'localhost',
                            user = 'root',
                            password = 'Nahid123',
                            database = 'Youtube_data_harvest')
cursor = connection.cursor()

question = st.selectbox('Select your question', ('1.Display the channel names',
                                                 '2.Channels with highest Subscribers',
                                                 '3.Top 10 most viewed videos',
                                                 '4.10 Videos with highest comments',
                                                 '5.Top 10 Videos with highest likes',
                                                 '6.channel name of highest liked video',
                                                 '7.Channel with highest likes',
                                                 '8.Videos with likes and their corresponding channel names',
                                                 '9.Average duration of all videos in each channel',
                                                 '10.Longest video',
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
    st.write(pd.DataFrame(t3, columns=["Channel_Name","view_count"]))

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
    query6 = '''select Channel_Name from videos where like_count is not null order by like_count desc limit 1 ;'''
    cursor.execute(query6)
    t6=cursor.fetchall()
    st.write(pd.DataFrame(t6, columns=["Channel_Name","like_count"]))

elif question == '7.Least commented video ':
    query7 = '''select Channel_Name, video_title,comment_count from videos where comment_count is not null order by 
    comment_count asc limit 1;'''
    cursor.execute(query7)
    t7=cursor.fetchall()
    st.write(pd.DataFrame(t7, columns=["Channel_Name","video_title","comment_count"]))

elif question == '8.Videos with likes and their corresponding channel names':
    query8 = '''select Channel_Name, video_title, like_count from videos where like_count is not null order by 
    like_count desc;''' 
    cursor.execute(query8)
    t8=cursor.fetchall()
    st.write(pd.DataFrame(t8, columns=["video_title","pAt"]))

elif question == '9.Average duration of all videos in each channel':
    query9 = '''select Channel_Name,avg(duration)from videos group by Channel_Name;'''
    cursor.execute(query9)
    t9=cursor.fetchall()
    st.write(pd.DataFrame(t9, columns=["Channel_Name","duration"]))

elif question == '10.Longest video':
    query10 = '''select Channel_Name, video_title from videos where duration is not null order by duration desc limit 1;'''
    cursor.execute(query10)
    t10=cursor.fetchall()
    st.write(pd.DataFrame(t10, columns=["video_title","duration"]))
