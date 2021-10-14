'''

'''


__author__ = "Ethan Bellmer"
__version__ = "0.1"


from flask import Flask
from werkzeug.serving import WSGIRequestHandler

app = Flask(__name__)

from server import routes
