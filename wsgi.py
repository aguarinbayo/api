from sample.server import Server

new_server = Server()
app = new_server.app

from werkzeug.contrib.fixers import ProxyFix
app.wsgi_app = ProxyFix(app.wsgi_app)