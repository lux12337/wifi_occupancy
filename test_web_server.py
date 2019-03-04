import requests
import os

response = requests.get(
    'http://localhost:3000/one-time-request'
)

os.write(
    os.open('pomona_output-from-server.txt', os.O_WRONLY | os.O_CREAT),
    response.text.encode('utf8')
)
