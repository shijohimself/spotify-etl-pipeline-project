from resources.playlist import data
import pandas as pd

# extracting songs data from spotify
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

#creating dataframe of songs data
songs_df = pd.DataFrame.from_dict(song_list)
print(songs_df)