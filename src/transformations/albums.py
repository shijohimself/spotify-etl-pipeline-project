from resources.playlist import data
import pandas as pd


# extracting album information from the spotify
album_list = []
for row in data['items']:
    album_id = row['track']['album']['id']
    album_name = row['track']['album']['name']
    album_release_date = row['track']['album']['release_date']
    album_url = row['track']['album']['external_urls']['spotify']
    album_elements = { "album_id" : album_id , "album_name" : album_name , "album_release_date" : album_release_date , "album_url" : album_url}
    album_list.append(album_elements)

# creating dataframe of albums data
album_df = pd.DataFrame.from_dict(album_list)
album_df = album_df.drop_duplicates(subset=['album_id'])

#additional transformations
# album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'])
# album_df['album_release_date'] = pd.to_datetime(album_df['album_release_date'], errors='coerce')
# mask = album_df['album_release_date'].str.match(r'^\d{4}$', na=False)
# album_df = album_df.loc[mask, 'album_release_date'] = pd.to_datetime(album_df.loc[mask, 'album_release_date'] + '-01-01')
# print(album_df)

#datetime transformation
def convert_mixed_dates(date_str):
    try:
        return pd.to_datetime(date_str)
    except:
        if date_str.isdigit() and len(date_str) == 4:
            return pd.to_datetime(f"{date_str}-01-01")
        return pd.NaT 
    
album_df['album_release_date'] = album_df['album_release_date'].apply(convert_mixed_dates)
album_df.info()