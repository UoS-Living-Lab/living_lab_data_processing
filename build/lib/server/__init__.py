from flask import Flask

from server.routes import app_blueprint
app = Flask(__name__)
app.register_blueprint(app_blueprint)



