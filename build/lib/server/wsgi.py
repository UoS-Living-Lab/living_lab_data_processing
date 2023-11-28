'''

'''


__author__ = "Ethan Bellmer"
__version__ = "0.1"


from flask import Flask
from werkzeug.serving import WSGIRequestHandler
from server.routes import app_blueprint
app = Flask(__name__)
app.register_blueprint(app_blueprint)

