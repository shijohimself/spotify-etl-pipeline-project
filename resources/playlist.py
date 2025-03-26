from resources.config import sp 
playlist_link = 'https://open.spotify.com/playlist/1d2fAul5T010POHxi1bQ4i'

playlist_uri = playlist_link.split("/")[-1]

data = sp.playlist_tracks(playlist_uri)

