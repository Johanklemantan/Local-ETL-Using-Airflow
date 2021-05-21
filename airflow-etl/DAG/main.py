import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'start_date':datetime(2021,5,20),
    'owner':'Johan',
    'email':'johanklemantan@gmail.com',
    "depends_on_past": False,
    "retries":1,
    "retry_delay":timedelta(minutes=1),
}

dag = DAG(
    'project_week_1',
    default_args=default_args)

def review_spanish():
    review_spanish1 = pd.read_csv("/usr/local/airflow/data/reviews_q1.csv")
    review_spanish2 = pd.read_csv("/usr/local/airflow/data/reviews_q2.csv")
    review_spanish3 = pd.read_csv("/usr/local/airflow/data/reviews_q3.csv")
    review_spanish4 = pd.read_csv("/usr/local/airflow/data/reviews_q4.csv")
    review_spanish_result = pd.concat([review_spanish1,review_spanish2,review_spanish3,review_spanish4],axis=0)
    review_spanish_result.to_csv('/usr/local/airflow/data/final_review_spanish_result.csv', index=False)
    return review_spanish_result

def tweet_clean():
    tweet = pd.read_json("/usr/local/airflow/data/tweet_data.json",lines=True)
    oredoo = tweet[['user','text','id','retweeted','source','retweet_count','created_at']]
    oredoo['username']=[x['name'] for x in oredoo['user']]
    oredoo.drop(['user'],axis=1,inplace=True)
    oredoo.to_csv('/usr/local/airflow/data/final_tweet_oredoo.csv', index=False)
    return oredoo

def disaster_data():
    disaster = pd.read_csv('/usr/local/airflow/data/disaster_data.csv')
    disaster.to_csv('/usr/local/airflow/data/final_disaster.csv', index=False)
    return disaster

def file1000():
    file_1000 = pd.read_excel('/usr/local/airflow/data/file_1000.xls')
    file_1000.drop(['Unnamed: 0','First Name.1'],axis=1,inplace=True)
    file_1000.to_csv('/usr/local/airflow/data/final_file1000.csv', index=False)
    return file_1000

def sqlite_file():
    dbfile = '/usr/local/airflow/data/database.sqlite'
    con = sqlite3.connect(dbfile)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con))

    new_data=data[0]
    for i in range(1,len(data)):
        new_data = new_data.merge(data[i],how='left',on='reviewid')

    new_data.drop(['artist_y'],axis=1,inplace=True)
    new_data.rename({'artist_x':'artist'},axis=1,inplace=True)
    new_data.to_csv('/usr/local/airflow/data/final_sqlite_file.csv', index=False)
    con.close()
    return new_data


def chinook_albums():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_albums = data[0]
    chinook_albums.to_csv('/usr/local/airflow/data/final_chinook_albums.csv', index=False)
    return chinook_albums

def chinook_sequence():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_sequence = data[1]
    chinook_sequence.to_csv('/usr/local/airflow/data/final_chinook_sequence.csv', index=False)
    return chinook_sequence

def chinook_artist():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_artist = data[2]
    chinook_artist.to_csv('/usr/local/airflow/data/final_chinook_artist.csv', index=False)
    return chinook_artist

def chinook_customer():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_customer = data[3]
    chinook_customer.to_csv('/usr/local/airflow/data/final_chinook_customer.csv', index=False)
    return chinook_customer

def chinook_employee():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_employee = data[4]
    chinook_employee.to_csv('/usr/local/airflow/data/final_chinook_employee.csv', index=False)
    return chinook_employee

def chinook_genre():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_genre = data[5]
    chinook_genre.to_csv('/usr/local/airflow/data/final_chinook_genre.csv', index=False)
    return chinook_genre

def chinook_invoice():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_invoice = data[6]
    chinook_invoice.to_csv('/usr/local/airflow/data/final_chinook_invoice.csv', index=False)
    return chinook_invoice

def chinook_items():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    chinook_items = data[7]
    chinook_items.to_csv('/usr/local/airflow/data/final_chinook_items.csv', index=False)
    return chinook_items

def chinook_track():
    dbfile2='/usr/local/airflow/data/chinook.db'
    con2 = sqlite3.connect(dbfile2)
    data=[]
    for table in pd.read_sql_query("SELECT name FROM sqlite_master WHERE type ='table'", con2)['name']:
        data.append(pd.read_sql_query(f"select * from {table}", con2))
    playlist = data[9]
    playtrack = data[10]
    chinook_artist = data[2]
    playlisttrack = pd.merge(playtrack,playlist,how='left',on='PlaylistId')
    playlisttrack.drop(['PlaylistId'],axis=1,inplace=True)

    track = pd.read_sql_query("SELECT *\
    FROM \
    (SELECT *\
    FROM tracks \
    JOIN media_types \
    USING(MediaTypeId) \
    JOIN genres \
    USING(GenreId) \
    JOIN albums \
    USING(AlbumId))a\
    ",con2)
    track1 = pd.merge(track,chinook_artist,how='left',on='ArtistId')
    chinook_track = pd.merge(track1,playlisttrack,on='TrackId',how='left')
    chinook_track.drop(['AlbumId','MediaTypeId','GenreId','ArtistId','Title','TrackId'],axis=1,inplace=True)
    chinook_track.rename({
        'Name_x':'Track Name',
        'Name:1':'Media Type',
        'Name:2':'Genre',
        'Name_y':'Artist Name',
        'Name':'Playlist'
    },axis=1,inplace=True)
    chinook_track.to_csv('/usr/local/airflow/data/final_chinook_track.csv', index=False)
    return chinook_track

def to_sqlite_spanish():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_final_review_spanish = pd.read_csv('/usr/local/airflow/data/final_review_spanish_result.csv')
    df_final_review_spanish.to_sql('final_review_spanish_result', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_tweet_oredoo():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_final_tweet_oredoo = pd.read_csv('/usr/local/airflow/data/final_tweet_oredoo.csv')
    df_final_tweet_oredoo.to_sql('final_tweet_oredoo', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_disaster():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_final_disaster = pd.read_csv('/usr/local/airflow/data/final_disaster.csv')
    df_final_disaster.to_sql('final_disaster', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_file1000():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_final_file1000 = pd.read_csv('/usr/local/airflow/data/final_file1000.csv')
    df_final_file1000.to_sql('final_file1000', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_data_file():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_final_sql_file= pd.read_csv('/usr/local/airflow/data/final_sqlite_file.csv')
    df_final_sql_file.to_sql('final_sqlite_file', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_albums():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_albums = pd.read_csv('/usr/local/airflow/data/final_chinook_albums.csv')
    df_chinook_albums.to_sql('final_chinook_albums', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_sequence():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_sequence = pd.read_csv('/usr/local/airflow/data/final_chinook_sequence.csv')
    df_chinook_sequence.to_sql('final_chinook_sequence', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_artist():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_artist = pd.read_csv('/usr/local/airflow/data/final_chinook_artist.csv')
    df_chinook_artist.to_sql('final_chinook_artist', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_customer():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_customer = pd.read_csv('/usr/local/airflow/data/final_chinook_customer.csv')
    df_chinook_customer.to_sql('final_chinook_customer', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_employee():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_employee = pd.read_csv('/usr/local/airflow/data/final_chinook_employee.csv')
    df_chinook_employee.to_sql('final_chinook_employee', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_genre():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_genre = pd.read_csv('/usr/local/airflow/data/final_chinook_genre.csv')
    df_chinook_genre.to_sql('final_chinook_genre', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_invoice():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_invoice = pd.read_csv('/usr/local/airflow/data/final_chinook_invoice.csv')
    df_chinook_invoice.to_sql('final_chinook_invoice', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_items():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_items = pd.read_csv('/usr/local/airflow/data/final_chinook_items.csv')
    df_chinook_items.to_sql('final_chinook_items', conn, if_exists="replace", index=False)
    conn.close()

def to_sqlite_chinook_track():
    conn = sqlite3.connect('/usr/local/airflow/data/data-warehouse.sqlite')
    df_chinook_track = pd.read_csv('/usr/local/airflow/data/final_chinook_track.csv')
    df_chinook_track.to_sql('final_chinook_track', conn, if_exists="replace", index=False)
    conn.close()

t1_review_spanish = PythonOperator(
    task_id = 'Extract_and_Transform_Review_Spanish',
    python_callable=review_spanish,
    dag=dag)


t1_review_oredoo = PythonOperator(
    task_id = 'Extract_and_Transform_Review_Oredoo',
    python_callable=tweet_clean,
    dag=dag)

t1_disaster = PythonOperator(
    task_id = 'Extract_and_Transform_Disaster_Data',
    python_callable=disaster_data,
    dag=dag)

t1_file1000 = PythonOperator(
    task_id = 'Extract_and_Transform_File_Excel_1000',
    python_callable=file1000,
    dag=dag)

t1_sqlite_data = PythonOperator(
    task_id = 'Extract_and_Transform_Sqlite_Database',
    python_callable=sqlite_file,
    dag=dag)

t1_chinook_albums = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Albums',
    python_callable=chinook_albums,
    dag=dag)

t1_chinook_sequence = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Sequence',
    python_callable=chinook_sequence,
    dag=dag)

t1_chinook_artist = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Artist',
    python_callable=chinook_artist,
    dag=dag)

t1_chinook_customer = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Customer',
    python_callable=chinook_customer,
    dag=dag)

t1_chinook_employee = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Employee',
    python_callable=chinook_employee,
    dag=dag)

t1_chinook_genre = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Genre',
    python_callable=chinook_genre,
    dag=dag)

t1_chinook_invoice = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Invoice',
    python_callable=chinook_invoice,
    dag=dag)

t1_chinook_items = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Items',
    python_callable=chinook_items,
    dag=dag)

t1_chinook_track = PythonOperator(
    task_id = 'Extract_and_Transform_Chinook_Track',
    python_callable=chinook_track,
    dag=dag)



t2_load_review_spanish = PythonOperator(
    task_id = 'Load_Review_Spanish',
    python_callable=to_sqlite_spanish,
    dag=dag)

t2_load_tweet_oredoo = PythonOperator(
    task_id = 'Load_Tweet_Oredoo',
    python_callable=to_sqlite_tweet_oredoo,
    dag=dag)

t2_load_disaster = PythonOperator(
    task_id = 'Load_Review_Disaster',
    python_callable=to_sqlite_disaster,
    dag=dag)

t2_load_file_1000 = PythonOperator(
    task_id = 'Load_File_1000',
    python_callable=to_sqlite_file1000,
    dag=dag)

t2_load_sqlite_data = PythonOperator(
    task_id = 'Load_Sqlite_Data',
    python_callable=to_sqlite_data_file,
    dag=dag)

t2_load_chinook_albums = PythonOperator(
    task_id = 'Load_Chinook_Albums',
    python_callable=to_sqlite_chinook_albums,
    dag=dag)

t2_load_chinook_sequence = PythonOperator(
    task_id = 'Load_Chinook_Sequence',
    python_callable=to_sqlite_chinook_sequence,
    dag=dag)

t2_load_chinook_artist = PythonOperator(
    task_id = 'Load_Chinook_Artist',
    python_callable=to_sqlite_chinook_artist,
    dag=dag)

t2_load_chinook_customer = PythonOperator(
    task_id = 'Load_Chinook_Customer',
    python_callable=to_sqlite_chinook_customer,
    dag=dag)

t2_load_chinook_employee = PythonOperator(
    task_id = 'Load_Chinook_Employee',
    python_callable=to_sqlite_chinook_employee,
    dag=dag)

t2_load_chinook_genre = PythonOperator(
    task_id = 'Load_Chinook_Genre',
    python_callable=to_sqlite_chinook_genre,
    dag=dag)

t2_load_chinook_invoice = PythonOperator(
    task_id = 'Load_Chinook_Invoice',
    python_callable=to_sqlite_chinook_invoice,
    dag=dag)

t2_load_chinook_items = PythonOperator(
    task_id = 'Load_Chinook_Items',
    python_callable=to_sqlite_chinook_items,
    dag=dag)

t2_load_chinook_track = PythonOperator(
    task_id = 'Load_Chinook_Track',
    python_callable=to_sqlite_chinook_track,
    dag=dag)

t1_review_spanish >> t2_load_review_spanish
t1_review_oredoo >> t2_load_tweet_oredoo
t1_disaster >> t2_load_disaster
t1_file1000 >> t2_load_file_1000
t1_sqlite_data >> t2_load_sqlite_data
t1_chinook_albums >> t2_load_chinook_albums
t1_chinook_sequence >> t2_load_chinook_sequence
t1_chinook_artist >> t2_load_chinook_artist
t1_chinook_customer >> t2_load_chinook_customer
t1_chinook_employee >> t2_load_chinook_employee
t1_chinook_genre >> t2_load_chinook_genre
t1_chinook_invoice >> t2_load_chinook_invoice
t1_chinook_items >> t2_load_chinook_items
t1_chinook_track >> t2_load_chinook_track