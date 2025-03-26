import json
import boto3
from datetime import datetime
from io import StringIO
import pandas as pd

def album(data):
    album_list = []
    for row in data['items']:
        album_id = row['track']['album']['id']
        album_name = row['track']['album']['name']
        album_release_date = row['track']['album']['release_date']
        album_url = row['track']['album']['external_urls']['spotify']
        album_elements = { "album_id" : album_id , "album_name" : album_name , "album_release_date" : album_release_date , "album_url" : album_url}
        album_list.append(album_elements)
    return album_list

def artists(data):
    artist_list = []
    for row in data['items']:
        for key , value in row.items():
            if key == "track":
                for artists in value['artists']:
                    artist_element = {'artist_id' : artists['id'] , "artist_name" : artists['name'] , "artist_external_url" : artists['external_urls']['spotify']}
                    artist_list.append(artist_element)
    return artist_list

def songs(data):
    song_list = []
    for row in data['items']:
        song_id = row['track']['id']
        song_name = row['track']['name']
        song_duration = row['track']['duration_ms']
        song_url = row['track']['external_urls']['spotify']
        song_popularity = row['track']['popularity']
        album_id = row['track']['album']['id']
        artist_id = row['track']['album']['artists'][0]['id']
        song_element = {"song_id" : song_id , "song_name" : song_name , "song-duration" : song_duration , "song_url" : song_url , "song_popularity" : song_popularity , "album_id" : album_id , "arist_id" : artist_id}
        song_list.append(song_element)
        print(song_list)
    return song_list

def lambda_handler(event, context):
    s3_client = boto3.client('s3')
    bucket = 'spotify-etl-project-bucket-shijo'
    key = 'raw_data/to_process/'

    spotify_data = []
    spotify_keys = []
    for file in s3_client.list_objects_v2(Bucket=bucket, Prefix=key)['Contents']:
        file_key = file['Key']
        if file_key.split('.')[-1] == 'json':
            response = s3_client.get_object(Bucket=bucket, Key=file_key)
            jsonObject = json.loads(response['Body'].read().decode('utf-8'))
            spotify_data.append(jsonObject)
            spotify_keys.append(file_key)

    for data in spotify_data:
        album_list = album(data)
        artist_list = artists(data)
        song_list = songs(data)
        print(album_list)
        album_df = pd.DataFrame.from_dict(album_list)
        album_df = album_df.drop_duplicates(subset=['album_id'])

        artist_df = pd.DataFrame.from_dict(artist_list)
        artist_df = artist_df.drop_duplicates(subset=['artist_id'])

        songs_df = pd.DataFrame.from_dict(song_list)
        def convert_mixed_dates(date_str):
            try:
                return pd.to_datetime(date_str)
            except:
                if date_str.isdigit() and len(date_str) == 4:
                    return pd.to_datetime(f"{date_str}-01-01")
                return pd.NaT 
    
        album_df['album_release_date'] = album_df['album_release_date'].apply(convert_mixed_dates)
    
        song_key = "transformed_data/songs_data/songs_transformed_" + str(datetime.now) + ".csv"
        song_buffer = StringIO()
        songs_df.to_csv(song_buffer, index = False)
        song_content = song_buffer.getvalue()
        s3_client.put_object(Bucket=bucket, Key=song_key, Body=song_content)

        album_key = "transformed_data/album_data/album_transformed_" + str(datetime.now) + ".csv"
        album_buffer = StringIO()
        album_df.to_csv(album_buffer, index = False)
        album_content = album_buffer.getvalue()
        s3_client.put_object(Bucket=bucket, Key=album_key, Body=album_content)

        artist_key = "transformed_data/artist_data/artist_transformed_" + str(datetime.now) + ".csv"
        artist_buffer = StringIO()
        artist_df.to_csv(song_buffer , index = False)
        artist_content = artist_buffer.getvalue()
        s3_client.put_object(Bucket=bucket, Key=artist_key, Body=artist_content)

    s3_resource = boto3.resource('s3')
    for key in spotify_keys:
        copy_source = {
            'Bucket': bucket,
            'Key': key
        }
        s3_client.copy_object(CopySource=copy_source, Bucket=bucket, Key='raw_data/processed/' + key.split('/')[-1])
        s3_resource.Object(bucket, key).delete()