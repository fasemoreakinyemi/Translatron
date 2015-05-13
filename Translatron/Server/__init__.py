#!/usr/bin/env python3
"""
Server wrapper
"""
import functools
from threading import Thread
from Translatron.Server.HTTPServer import startHTTPServer
from Translatron.Server.WebsocketInterface import startWebsocketServer

def startTranslatron(startWebsocket = True, startHTTP = True, join = True, http_port=8080):
    """
    Start servers required for Translatron

    Keyword argumetns:
        startWebsocket: Whether to start the websocket server
        startHTTP: Whether to start the CherryPy-based HTTP server
        join: Whether to wait for the server threads to exit
    """
    #Start websocket server
    wsThread = None
    if startWebsocket:
        wsThread = Thread(target=startWebsocketServer)
        wsThread.start()
    #Start HTTP server
    httpThread = None
    if startHTTP:
        httpThread = Thread(target=functools.partial(startHTTPServer, http_port=http_port))
        httpThread.start()
    #Join
    if join and wsThread is not None:
        wsThread.join()
    if join and httpThread is not None:
        httpThread.join()
