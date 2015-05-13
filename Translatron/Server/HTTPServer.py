#!/usr/bin/env python3
import cherrypy
import os.path
import json
from ansicolor import blue, red

class TranslatronServer(object):
    pass

def startHTTPServer(http_port=8080):
    print(blue("HTTP server starting up, listening on port %d..." % http_port))
    #Fixes six issue: http://goo.gl/ebeWDN
    cherrypy.config.update({'engine.autoreload.on': False})
    conf = {
        'global': {'server.socket_host': '0.0.0.0', 'server.socket_port': http_port},
        '/': {
            'tools.staticdir.on': True,
            'tools.staticdir.dir': os.path.join(os.path.abspath(os.getcwd()), "static"),
            'tools.staticdir.index' : 'index.html',
        },
    }
    print("Static: " + os.path.join(os.path.abspath(os.getcwd()), "static"))
    cherrypy.quickstart(TranslatronServer(), "/", conf)