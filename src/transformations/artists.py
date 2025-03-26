from resources.playlist import data
import pandas as pd

#extracting artists information from spotify
artist_list = []
for row in data['items']:
    for key , value in row.items():
        if key == "track":
            for artists in value['artists']:
                artist_element = {'artist_id' : artists['id'] , "artist_name" : artists['name'] , "artist_external_url" : artists['external_urls']['spotify']}
                artist_list.append(artist_element)

#creating dataframe of artists data
artist_df = pd.DataFrame.from_dict(artist_list)
artist_df = artist_df.drop_duplicates(subset=['artist_id'])
print(artist_df)