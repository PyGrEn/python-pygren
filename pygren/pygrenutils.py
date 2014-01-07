#!/usr/bin/env python
# encoding: UTF-8
"""
pygrenutils.py

Created by David Arsenault, Brett Beaudoin and Ryan Freckleton, The MITRE Corporation
Approved for Public Release; Distribution Unlimited. 13-3052
 ©2014 - The MITRE Corporation. All rights reserved.

Dependencies:
        'sqlalchemy' (http://www.sqlalchemy.org/)
"""
import inspect as inspectStack # Because sqlalchemy has 'inspect' also
from re import findall, M as MULTILINE
from threading import Thread
from urllib2 import build_opener
from datetime import datetime
from commands import getoutput
from os.path import basename, getmtime, isfile
from httplib import HTTPConnection
from urllib import urlencode
from time import time
from StringIO import StringIO

import rdflib

def parse(text):
    """
    Parse a nt, turle or n3 formatted string into triples that are simple
    strings.
    """
    g = rdflib.Graph()
    g.parse(StringIO(text), format='n3')
    for triple in g:
        yield tuple(map(unicode, triple))

class NetworkUtils(object):
    """Network utilities"""
    def __init__(self, ips=None, port=None):
        self.opener = build_opener()
        self.opener.addheaders = [('User-Agent', 'Mozilla/5.0'),
                                  ('Connection', 'close'),
                                  ('Cache-Control', 'no-cache')]
        self.ifconfig = getoutput("ifconfig|grep 'inet addr:'|grep -v '127.0.0.1'")
        self.myips = findall(r"(?<=addr:)\d+\.\d+\.\d+\.\d+",
                             self.ifconfig, MULTILINE)
        self.ips = ips
        self.port = port
        self.servers = {}

    def get(self, url, method="get", postdata=None):
        req = self.opener.open(url, postdata)
        res = req.read().strip()
        req.close()
        return res

class AsyncHTTP(object):
    # TODO: refactor into utility methods
    """Handles async HTTP requests"""
    def __fireAndForget(self, method, host, port, route, postdata=None):
        try:
            if postdata is not None and str(postdata).strip().startswith('{'):
                postdata = urlencode(eval(str(postdata)))
            headers = {'User-Agent':'Mozilla/5.0', 'Cache-Control':'no-cache',
                       'Accept-Encoding':'*', 'Accept':'*/*'}
                       #, 'Connection':'close'}
            if method == 'POST':
                headers['Content-Type'] = 'application/x-www-form-urlencoded'
            else:
                headers['Content-Type'] = 'text/html'
            if postdata is None:
                debug('Sending... %s %s:%d%s' % (method, host, port, route))
            else:
                debug('Sending... %s %s:%d%s (%s...)' %
                      (method, host, port, route, postdata[:80]))
            conn = HTTPConnection(host, port)
            conn.request(method, route, postdata, headers)
        except Exception, err:
            stringError = 'ERROR: %s' % err
            debug(stringError)
            raise

    def send(self, method, host, port, route, postdata=None):
        t = Thread(target=self.__fireAndForget,
                   args=(method, host, port, route, postdata))
        t.daemon = True
        t.start()

    def get(self, host, port, route):
        self.send('GET', host, port, route)

    def post(self, host, port, route, postdata):
        self.send('POST', host, port, route, postdata)


def debug(text):
    frame = inspectStack.currentframe().f_back
    module = inspectStack.getmodule(frame)
    dtg = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
    lineNum = "      %s" % frame.f_lineno
    lineNum = lineNum[-6:]
    file = "%s              " % basename(module.__file__)
    file = file[0:14]
    line = "%s %s %s %s" % ( dtg, lineNum, file, text )
    print line

def deDup(listIn, sort=False):
    try:
        listOut = list(set(listIn))
    except:
        listOut = []
        for i in listIn:
            if i in listOut: continue
            listOut.append(i)
    if sort:
        listOut.sort()
    return listOut

def fileAge(filename):
    if not isfile(filename):
        return 0
    else:
        return time() - getmtime(filename)

def keyHash(*args):
    # This algorithm will need to be tested for collisions
    concatString = u""
    for arg in list(args):
        concatString += unicode(arg)
    # hash should be a 32-bit unsigned integer
    hashed = 0
    for char in concatString:
        hashed = (hashed + (hashed << 19) + ord(char)) & 0xFFFFFFFF
    return hashed
