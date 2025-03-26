import spotipy
from spotipy.oauth2 import SpotifyClientCredentials

client_credentials_manager = SpotifyClientCredentials(
    client_id="193a6160e5794b6fbfe681bc0de29f15",
    client_secret="50fd14ab93d04dd98fc25ae2a5a533c9"
)

sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)