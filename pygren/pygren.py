#!/usr/bin/env python
# encoding: UTF-8
"""
pygren.py

Created by David Arsenault, Brett Beaudoin and Ryan Freckleton, The MITRE Corporation
Approved for Public Release; Distribution Unlimited. 13-3052
 ©2014 - The MITRE Corporation. All rights reserved.

Dependencies:
        'tornado'        (http://pypi.python.org/pypi/tornado)
        'daemon'         (http://pypi.python.org/pypi/python-daemon/)
        'sqlalchemy'     (http://www.sqlalchemy.org/)
"""
import commands #TODO remove and replace with direct python calls
import fileinput
import os
import re
import sys
import time
import urllib
import zlib

import os.path as pth

import pygrendb
import pygrenutils as utils
import dataloader

import daemon
import sqlalchemy as sa

from tornado import httpserver, ioloop, web
from ConfigParser import RawConfigParser
from ast import literal_eval
from pkg_resources import resource_filename

# Globals
staticWeb = {}
net = utils.NetworkUtils()
http = utils.AsyncHTTP()

rootDir = commands.getoutput("echo ${PYGREN_HOME}").strip()

if rootDir == "":
    try:
        __file__
    except:
        __file__ = os.path.basename(sys.argv[0])
    rootDir = os.path.dirname(os.path.realpath(__file__))
    commands.getoutput("export PYGREN_HOME=%s" % rootDir)

config_paths = ('/etc/pygren.cfg', pth.expanduser('~/.pygren.cfg'),
               resource_filename(__name__, "conf/pygren.cfg"))
for confFile in config_paths:
    if not pth.isfile(confFile):
        utils.debug("config file not found at %s" % confFile)
        continue
    cfg = RawConfigParser()
    cfg.read(confFile)
    utils.debug("Reading configuration from %s" % confFile)
    break

port = cfg.getint("graphserver", "port")
triplesPerLoad  = cfg.getint("graphserver", "triplesPerLoad")
logFile = cfg.get("graphserver", "logFile")
dbEngineURL = cfg.get("database", "dbEngineURL")

masters = [ip.strip() for ip in cfg.get('network', 'masters').split(',')]
masterIP = masters[0]
slaves = [ip.strip() for ip in cfg.get('network', 'slaves').split(',')]

class Routes(object):
    def __init__(self):
        self.routesHTML = None
        # Put all API route info into a list of lists.  This way we can order
        # the routes as needed for Tornado to work properly, and also re-sort
        # them for human-readable display.
        self.routes = []
        self.routes.append([ (r"/db/(rowcount|fields)/(.*)", DBHandler), "GET", "Expects a table name", "/db/rowcount/triples", True])
        self.routes.append([ (r"/db/(execute)", DBHandler), "POST", "Expects a SQL statement", "/db/execute", False])
        self.routes.append([ (r"/db/(reindex|vacuum)", DBHandler), "POST", "Executes the given command", "/db/reindex", False])

        self.routes.append([ (r"/graph/(get)/(vertex|predicate|triple)/(.*)", GraphHandler), "GET", "Expects an ID", "/graph/get/predicate/1", True])
        self.routes.append([ (r"/graph/(get)/(vertex|predicate|triple)", GraphHandler), "GET", "The action will apply to ALL rows", "/graph/get/triple", True])
        self.routes.append([ (r"/graph/(import)", GraphHandler), "POST", "Expects an ntriples file", "/graph/import", False])
        self.routes.append([ (r"/graph/(add)", GraphHandler), "POST", "Expects a local file path or a list of triples (e.g.: [['Brett', 'works_for', 'MITRE'], ['Joe', ...)", "/graph/add", False])

        self.routes.append([ (r"/graph/(join)/(.*)", GraphHandler), "GET", "Returns triples with the specified step", "/graph/join/1", True])
        self.routes.append([ (r"/graph/(join)", GraphHandler), "GET", "Returns all triples", "/graph/join", True])

        self.routes.append([ (r"/query/(queue)/(traverse)", QueryHandler), "POST", "Expects an intermediate traversal query from another slave", "/query/queue/traverse", False])
        self.routes.append([ (r"/query/(queue)/(process)/(slaves|master)", QueryHandler), "POST", "Starts processing the current query queue", "/query/queue/process/slaves", False])
        self.routes.append([ (r"/query/(queue)/(show)", QueryHandler), "GET", "Returns the current query queue", "/query/queue/show", True])
        self.routes.append([ (r"/query/(step)/(\d+)", QueryHandler), "GET", "Set the step number", "/query/step/3", False])
        self.routes.append([ (r"/query/(step)/(show|delete)", QueryHandler), "GET", "Show or reset the step number", "/query/step/show", True])
        self.routes.append([ (r"/query/(results)/(sql|scan|traverse)/(list|json|csv|tsv|d3js|gml|gefx|gephi|gdf)/(download)", QueryHandler), "GET", "Returns a file with query results", "/query/results/sql/json/download", False])
        self.routes.append([ (r"/query/(results)/(sql|scan|traverse)/(list|json|csv|tsv|d3js|gml|gefx|gephi|gdf)", QueryHandler), "GET", "Returns query results in specific format", "/query/results/traverse/csv", True])
        self.routes.append([ (r"/query/(results)/(sendtomaster)", QueryHandler), "POST", "Expects query results (master ordering slave to post results)", "/query/results/sendtomaster", False])
        self.routes.append([ (r"/query/(results)", QueryHandler), "POST", "Expects query results (slave posting to master)", "/query/results", False])
        self.routes.append([ (r"/query/(new)/(scan)/(list)", QueryHandler), "POST", "Expects a search string and returns a short list for autocomplete", "/query/new/scan/list", False])
        self.routes.append([ (r"/query/(new)", QueryHandler), "POST", "Expects one or more queries of a specified type", "/query/new", False])
        self.routes.append([ (r"/query/(reset)/(sql|scan|traverse)", QueryHandler), "GET", "Resets the steps, flags, etc for given query type", "/query/reset/sql", False])
        self.routes.append([ (r"/query/(pctdone)/(sql|scan|traverse)", QueryHandler), "GET", "Returns an integer between 0 and 100", "/query/pctdone/sql", True])
        self.routes.append([ (r"/query/(next)", QueryHandler), "POST", "Master triggers slave to process next hop", "/query/next", False])
        self.routes.append([ (r"/query/(slaveisdone)", QueryHandler), "POST", "Alert master that slave has completed the step or query", "/query/slaveisdone", False])

        self.routes.append([ (r"/api.(html|json)", APIHandler), "GET", "Displays the API in HTML or JSON format", "/api.html", True])

        self.routes.append([ (r"/(.*)", HTMLHandler), "GET", "Used for html, css, scripts, etc", "/favicon.ico", True])
        self.routes.append([ (r"/", HTMLHandler), "GET", "Returns index.html", "/", False])

    def getRoutes(self):
        return self.routes

    def getRoutesHTML(self, forceUpdate=False):
        if forceUpdate or rou.routesHTML is None:
            try:
                html = "<html>\n<table cellspacing='0' cellpadding='2' id='routes'>\n"
                html += "<tr><th>METHOD</th><th>ROUTE</th><th>COMMENTS</th><th>EXAMPLE</th></tr>\n"
                routes = self.routes
                routes.sort()
                for route in routes:
                    if route[4] == True:
                        example = "<a href='%s'>%s</a>" % (route[3], route[3])
                    else:
                        example = route[2]
                    html += "<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n" % (route[1], route[0][0], route[2], example)
                html += "</table>\n"
                html += "</html>\n"
                rou.routesHTML = html
                return html
            except Exception, err:
                return str(err)
        else:
            return rou.routesHTML


class HTMLHandler(web.RequestHandler):
    # This handles most web requests
    def get(self, path=""):
        try:
            if path == "" or path.lower().endswith(".html") or path.lower().endswith(".htm"):
                if path == "" or path == "/":
                    path = "web/index.html"
                self.set_header('Content-Type', 'text/html; charset=UTF-8')
                self.render(resource_filename(__name__, path), items=[])
            else:
                ext = path.lower()[path.rfind(".")+1:]
                if path.lower().startswith("images/") or ext in ["png", "gif", "jpg", "jpeg"]:
                    # This is an image file, so we read it as binary
                    ext = path.lower()[path.rfind(".")+1:]
                    ret = open(resource_filename(__name__, 'web/' + path), "rb").read()
                    staticWeb[path] = ret
                    self.set_header('Content-Type', 'image/' + ext)
                    self.write(str(ret))
                elif path.lower().startswith("js/") or path.lower().endswith(".js"):
                    ret = open(resource_filename(__name__, 'web/' + path), "r").read()
                    staticWeb[path] = ret
                    self.set_header('Content-Type', 'text/javascript')
                    self.write(str(ret))
                elif path.lower().startswith("css/"):
                    ret = open(resource_filename(__name__, 'web/' + path), "r").read()
                    staticWeb[path] = ret
                    self.set_header('Content-Type', 'text/css')
                    self.write(str(ret))
                elif not path.lower().endswith(".htm") and not path.lower().endswith(".html"):
                    self.set_header('Content-Type', 'text/plain; charset=UTF-8')
                    ret = staticWeb.get(path, None)
                    if ret is None:
                        try:
                            ret = open(resource_filename(__name__, 'web/' + path), "r").read()
                            staticWeb[path] = ret
                        except Exception, err:
                            ret = {"error": str(err)}
                    self.write(ret)
                else:
                    self.render(resource_filename(__name__, path), items=[])
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write(stringError)


class QueryHandler(web.RequestHandler):
    def get(self, *args):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        try:
            ret = {}
            if args[0] != "pctdone":
                utils.debug("REC'D from %s: GET %s" % (self.request.remote_ip, self.request.path))
                try:
                    queryType = args[1]
                    utils.debug("fn.queryPctDone['%s']['master'] = %s" % (queryType, str(fn.queryPctDone[queryType]['master'])))
                except:
                    pass
            if args[0] == "pctdone":
                queryType = args[1]
                ret = {"pctdone": fn.queryPctDone[queryType]}
            elif args[0] == "results":
                queryType = args[1]
                ret = {"pctdone": fn.queryPctDone[queryType], "results": []}

                if thisIsMaster:
                    if fn.queryPctDone[queryType]["slaves"] == 100 and fn.queryPctDone[queryType]["master"] == -1:
                        # Get results from all slaves
                        fn.queryPctDone[queryType]["master"] = 0
                        utils.debug("Requesting %s query results from slaves..." % queryType)
                        fn.results[queryType] = {}
                        postdata = "queryType=%s" % queryType
                        fn.sendMessageToAllSlaves("/query/results/sendtomaster", postdata)

                    elif fn.queryPctDone[queryType]["master"] == 100:
                        try:
                            resultsList = []
                            utils.debug("fn.results['%s'] = %s" % (queryType, str(fn.results[queryType])[0:200]))
                            for slave in fn.results[queryType].keys():
                                try:
                                    for row in fn.results[queryType][slave]:
                                        resultsList.append(row)
                                except:
                                    resultsList.append(fn.results[queryType][slave])
                            resultsList = utils.deDup(resultsList, True)
                            resultsList.sort()
                        except Exception, err:
                            stringError = "ERROR: %s" % str(err)
                            utils.debug(stringError)

                        if queryType in ["sql", "scan"]:
                            ret["results"] = resultsList
                        elif queryType == "traverse":
                            format = args[2]
                            if format == "list":
                                ret["results"] = resultsList # Because the results are already in list format
                            elif format == "json":
                                if len(resultsList) > 0:
                                    ret["results"] = [{"st": line[0], "s": line[1], "p": line[2], "ot": line[3], "o": line[4]} for line in resultsList if len(line) >= 5]
                                else:
                                    ret["results"] = []
                            elif format in ["csv", "tsv"]:
                                try:
                                    ret["results"] = []
                                    for line in [line for line in resultsList if len(line) >= 5]:
                                        if format == "csv":
                                            line = str(line)[1:-1]
                                            ret["results"].append(line)
                                        else:
                                            if str(line).startswith("[["):
                                                for triple in line:
                                                    ret["results"].append("\t".join(triple))
                                            elif str(line).startswith("["):
                                                ret["results"].append("\t".join(line))
                                            else:
                                                ret["results"].append(line)

                                    ret["results"] = "\n".join(ret["results"])
                                except Exception, err:
                                    stringError = "ERROR: %s" % str(err)
                                    utils.debug(stringError)
                                    self.write({"error": str(err)})

                            elif format == "gdf":
                                ret["results"] = []

                            elif format == "gefx":
                                ret["results"] = []

                            elif format == "gephi":
                                ret["results"] = []

                            elif format == "gml":
                                """
                                Example:
                                graph [
                                        comment "PyGrEn Graph"
                                        directed 1
                                        label "PyGrEn Graph"
                                        node [
                                                id 1
                                                label "30929"
                                        ]
                                        node [
                                                id 2
                                                label "Arsenault,David_L."
                                        ]
                                        edge [
                                                source 1
                                                target 2
                                                label "has_full_name"
                                        ]
                                ]
                                """
                                verts = []
                                append = verts.append # Faster than calling verts.append in every iteration
                                ret["results"] = 'graph [\ncomment "PyGrEn Graph"\ndirected 1\nlabel "PyGrEn Graph"\n'
                                # {"s": line[0], "p": line[1], "o": line[2]}
                                for triple in [triple for triple in resultsList if len(triple) >= 3]:
                                    s = triple[0]
                                    p = triple[1]
                                    o = triple[2]
                                    if s not in verts:
                                        append(s)
                                        ret["results"] += 'node [\nid %d\nlabel "%s"]\n' % (verts.index(s), s.replace("\"", "\\\""))
                                    if o not in verts:
                                        append(o)
                                        ret["results"] += 'node [\nid %d\nlabel "%s"]\n' % (verts.index(o), o.replace("\"", "\\\""))
                                    ret["results"] += 'edge [\nsource %d\ntarget %d\nlabel "%s"]\n' % (verts.index(s), verts.index(o), p.replace("\"", "\\\""))
                                ret["results"] += ']'

                            elif format == "d3js":
                                """
                                This JSON is specifically formatted to work with d3.js
                                Example:
                                {"verts":
                                        [       "30929",
                                                "Arsenault,David_L.",
                                                "33607",
                                                "bbeaudoin@mitre.org",
                                                "Beaudoin,Brett_C.",
                                                "030880SE-P1_2008",
                                                "03101BV0-JM_2010"
                                        ],
                                "edges":
                                        [
                                                {"edge": "has_full_name", "weight": 1, "source": 0, "target": 1},
                                                {"edge": "has_email", "weight": 1, "source": 2, "target": 3},
                                                {"edge": "has_full_name", "weight": 1, "source": 2, "target": 4},
                                                {"edge": "worked_project_key", "weight": 1, "source": 2, "target": 5},
                                                {"edge": "worked_project_key", "weight": 1, "source": 2, "target": 6}
                                        ]
                                }
                                """
                                ret["results"] = {"verts": [], "edge": []}
                                vIndex = ret["results"]["verts"].index

                                for triple in [triple for triple in resultsList if len(triple) >= 3]:
                                    s = triple[0]
                                    p = triple[1]
                                    o = triple[2]
                                    if s not in ret["results"]["verts"]:
                                        ret["results"]["verts"].append(s)
                                    if o not in ret["results"]["verts"]:
                                        ret["results"]["verts"].append(o)

                                    ret["results"]["edges"].append({"edge": p, "weight": 2, "source": vIndex(s), "target": vIndex(o)})


                        if len(args) == 4 and args[3] == "download":
                            self.set_header("Content-Type", "application/octet-stream")
                            self.set_header("Content-Disposition", "attachment; filename=results_%d.%s" % (int(time.time()), args[2]))

                else:
                    # Return this slave's results
                    utils.debug("Returning results to master")
                    ret = {"results": fn.getQueryResults(queryType)}

            elif args[0] == "reset":
                fn.initQuery(args[1])
                ret = {args[1]: "reset"}
            elif args[0] == "step":
                if args[1] != "show":
                    fn.stepNum = int(args[1])
                ret = {"step": fn.stepNum}
            if (str(ret).startswith("{") and str(ret).endswith("}")) or (str(ret).startswith("[") and str(ret).endswith("]")):
                try:
                    self.write(ret)
                except Exception, err:
                    stringError = "ERROR: %s" % str(err)
                    utils.debug(stringError)
                    self.write(stringError)
            else:
                self.write(str(ret))
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write({"error": str(err)})


    def post(self, *args):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        try:
            postdata = self.request.arguments
            fromIP = postdata.get("fromIP", [self.request.remote_ip])[0]
            utils.debug('REC\'D POST from %s: %s (%s ...)' % (fromIP, self.request.path, str(self.request.arguments)[0:200]))
            queryType = postdata.get('queryType', [None])[0]
        except Exception, err:
            stringError = 'ERROR: %s' % str(err)
            utils.debug(stringError)
            self.write({'error': str(err)})

        try:
            ret = {}
            if thisIsMaster and args[0] == "slaveisdone":
                # Master only - slave is alerting that it has completed its query or step.
                if queryType in ["sql", "scan"]:
                    # Keep track of how many of the slaves are done
                    fn.isDone[queryType].append(fromIP)
                    if len(fn.isDone[queryType]) == len(slaves):
                        fn.queryPctDone[queryType]["slaves"] = 100
                        utils.debug("All slaves have have completed the '%s' query" % queryType)
                    else:
                        fn.queryPctDone[queryType]["slaves"] = int((len(fn.isDone[queryType]) * 100.0) / len(slaves))
                else:
                    slaveIndex = slaves.index(fromIP)
                    utils.debug("queryType, slaveIndex, fromIP, hop = %s, %d, %s, %s" % (queryType, slaveIndex, fromIP, str(postdata.get("hop", [-1])[0])))
                    if queryType == "traverse":
                        try:
                            # e.g.: "hop=%d&verts=%d&triples=%d"
                            hop = postdata.get("hop", [-1])[0]
                            verts = postdata.get("verts", [-1])[0]
                            triples = postdata.get("triples", [-1])[0]
                            fn.hopsBySlaveIndex[slaveIndex] = int(hop)
                            utils.debug("fn.hopsBySlaveIndex = %s" % str(fn.hopsBySlaveIndex))

                            # e.g.: for 10 slaves and 2 hops maximum, if 5 slaves are done with the first step, subStepsDone would be 5 out of 20 => 0.25
                            subStepsDone = sum([float(v) for k, v in fn.hopsBySlaveIndex.iteritems()]) / len(slaves)

                            fn.queryPctDone[queryType]["slaves"] = int( 100 * subStepsDone / fn.maxHops)
                            if len(slaves) == len(fn.hopsBySlaveIndex.keys()):
                                #Check if all slaves are done.  If so, trigger next hop
                                if len(slaves) == len([hop for slaveIndex, hop in fn.hopsBySlaveIndex.iteritems() if hop == fn.hopNum]):
                                    fn.hopNum += 1
                                    if fn.hopNum <= fn.maxHops:
                                        fn.sendMessageToAllSlaves("/query/next", "queryType=%s&fromIP=%s&hop=%d" % (queryType, myIP, fn.hopNum) )
                                    else:
                                        utils.debug("self.hopNum, self.maxHops => %d, %d" % (fn.hopNum, fn.maxHops))
                                        fn.queryPctDone[queryType]["slaves"] = 100
                        except Exception, err:
                            stringError = "ERROR: %s" % str(err)
                            utils.debug(stringError)
                            self.write({"error": str(err)})

            elif args[0] == "queue":
                if thisIsMaster:
                    # Forward the query to all slaves
                    fn.sendMessageToAllSlaves( self.request.path, postdata )
                elif args[1] == "traverse":
                    # postdata is a list of vert IDs from another slave
                    verts = literal_eval(postdata.get("q", ["[]"])[0])
                    utils.debug("verts = %s" % str(verts))

                    sql = db.vertices.update().values(flag=0).\
                            where(
                                    sa.and_(
                                            db.vertices.c.id.in_(verts),
                                            db.vertices.c.flag != 0
                                    )
                            )
                    flagged = db.execute(sql)
                    utils.debug("verts flagged = %d" % flagged)


                    sql = db.triples.update().values(flag=0).\
                            where(
                                   sa.and_(
                                           sa.or_(
                                                    db.triples.c.s.in_(verts),
                                                    db.triples.c.o.in_(verts)
                                            ),
                                            db.triples.c.flag != 0
                                    )
                            )

                    flagged = db.execute(sql)
                    utils.debug("triples flagged = %d" % flagged)

                    fn.alertMasterThisSlaveIsDone("traverse")

                ret = fn.queryQueue[args[1]] # TODO: Is this correct????????
            elif args[0] == "results":
                if len(args) > 1 and args[1] == "sendtomaster":
                    # This is a slave that has been ordered to post results to the master.
                    # The only postdata rec'd from master is queryType.
                    try:
                        postdata = "compressed=1&fromIP=%s&queryType=%s&results=%s" % (myIP, queryType, urllib.quote_plus(zlib.compress(unicode(fn.getQueryResults(queryType)))) )
                        http.post(masterIP, port, "/query/results", postdata)
                    except Exception, err:
                        stringError = "ERROR: %s" % str(err)
                        utils.debug(stringError)
                        self.write({"error": str(err)})
                elif thisIsMaster:
                    # Master only.  Slave is posting results to the master.  Store them.
                    utils.debug("Slave %s posted results for '%s' query" % (fromIP, queryType))
                    try:
                        results = postdata.get("results", [None])[0]
                        if results is not None:
                            compressed = int(postdata.get("compressed", ["0"])[0])
                            if compressed == 1:
                                results = zlib.decompress(results)
                    except Exception, err:
                        results = [('ERROR: %s' % str(err))]

                    try:
                        if results is not None and str(results).startswith("[") and not str(results).startswith("[('ERROR"):
                            results = map(lambda x: list(x), literal_eval(results))
                        #utils.debug("results = %s" % str(results))

                        fn.results[queryType][fromIP] = results
                        if len(fn.results[queryType].keys()) == len(slaves):
                            fn.queryPctDone[queryType]["master"] = 100
                            utils.debug("All %d slaves have posted results for '%s' query" % (len(slaves), queryType))
                        else:
                            fn.queryPctDone[queryType]["master"] = int((len(fn.results[queryType].keys()) * 100.0) / len(slaves))
                    except Exception, err:
                        stringError = "ERROR: %s" % str(err)
                        utils.debug(stringError)
                        self.write({"error": str(err)})

            elif args[0] == "new":
                #utils.debug("Query received  *****************************************************")
                q = postdata.get("q", [None])[0]
                fn.initQuery(queryType)
                cmd = "/query/new"
                if queryType == "traverse":
                    try:
                        if q is None or len(q) == 0:
                            utils.debug("ERROR: no query")
                        else:
                            # e.g.: q = {'maxDist': '1', 'vertFrom': '33607',
                            #            'predicate': '*', 'vertTo': '30929'}
                            if str(q).startswith("["):
                                q = literal_eval(str(q))
                                q = q[0]
                            if str(q).startswith("{"):
                                q = literal_eval(str(q))
                            utils.debug("q = %s" % q)

                            fn.maxHops = int(q.get("maxDist", '1'))

                            fn.hopNum = 1
                            if thisIsMaster:
                                # Forward the query to all slaves
                                #utils.debug("Posting to all slaves: %s (%s)" % (cmd, q))
                                fn.sendMessageToAllSlaves( cmd, "queryType=%s&fromIP=%s&q=%s" % (queryType, myIP, urllib.quote_plus(str(q))) )
                            else:
                                sql = db.vertices.update().values(flag=0).where(db.vertices.c.vert == q["vertFrom"].replace("*", "%%"))
                                db.execute(sql)

                                if q["vertTo"] != "":
                                    fn.maxHops = int(round(fn.maxHops/2.0))
                                    sql = db.vertices.update().values(flag=0).where(db.vertices.c.vert == q["vertTo"].replace("*", "%%"))
                                    db.execute(sql)

                                sql = db.triples.update().\
                                                values(flag=0).\
                                                where(
                                                        sa.or_(
                                                                db.triples.c.s.in_(
                                                                       sa.select([db.vertices.c.id]).\
                                                                                where(db.vertices.c.flag==0)
                                                                        ),
                                                                db.triples.c.o.in_(
                                                                       sa.select([db.vertices.c.id]).\
                                                                                where(db.vertices.c.flag==0)
                                                                        )
                                                                )
                                                        )

                                if q["predicate"] != "*" and q["predicate"] != "":
                                    predList = []
                                    append = predList.append # Faster than calling predList.append in every iteration
                                    for e in q["predicate"].split(","):
                                        append("'%s'" % e.replace("  ", " ").replace(" ", "_").strip())
                                    sql = sql.where(db.triples.c.p.in_(sa.select([db.predicates.c.id]).where(db.predicates.c.pred.in_(predList))))

                                utils.debug("sql = %s" % str(sql))

                                fn.traverseSQL = sql # Storing for use in subsequent hops
                                db.execute(sql)

                                fn.distributeIntermediateQueriesToOtherSlaves("traverse")
                                fn.alertMasterThisSlaveIsDone("traverse")

                    except Exception, err:
                        stringError = "ERROR: %s" % str(err)
                        utils.debug(stringError)
                        self.write({"error": str(err)})

                elif queryType in ["sql", "scan"]:
                    if thisIsMaster:
                        # Forward the query to all slaves
                        ret = {}

                        # In case the user sent a 'LIKE' clause with percent signs, double them up to escape
                        q = re.sub('%+', '%', q)
                        q = re.sub(r'(?<=[^\%])\%(?=[^\%]*)', '%%', q)
                        if q.startswith('%') and not q.startswith('%%'):
                            q = '%' + q

                        fn.sendMessageToAllSlaves( cmd, "queryType=%s&fromIP=%s&q=%s" % (queryType, myIP, urllib.quote_plus(str(q))) )
                    else:
                        # This is a slave that just received a query from the master, so
                        # execute the query and post the results back to the master
                        utils.debug("q = %s" % q)
                        if queryType == "sql":
                            sql = q
                        else: # scan
                            sub = db.vertices.alias()
                            obj = db.vertices.alias()

                            sql = sa.select([sub.c.vert, db.predicates.c.pred, obj.c.vert]).\
                                            select_from(db.triples.\
                                                    join(sub, sub.c.id==db.triples.c.s).\
                                                    join(obj, obj.c.id==db.triples.c.o).\
                                                    join(db.predicates, db.predicates.c.id==db.triples.c.p)\
                                            ).\
                                            where(
                                                   sa.or_(
                                                            sub.c.vert.like("%%" + q + "%%"),
                                                            obj.c.vert.like("%%" + q + "%%")
                                                    )
                                            ).limit(10)


                        # Store results and alert master
                        fn.results[queryType] = db.execute(sql)
                        fn.alertMasterThisSlaveIsDone(queryType)

                elif queryType == "traverse":
                    try:
                        if args[0] == "queue":
                            if str(q).startswith("["):
                                q = literal_eval(str(q))
                                fn.queryQueue["traverse"] += q
                                fn.queryQueue["traverse"] = utils.deDup(fn.queryQueue["traverse"], False)
                        elif q is None or len(q) == 0:
                            utils.debug("ERROR: no query")
                        else:
                            if str(q).startswith("["):
                                q = literal_eval(str(q))[0]
                            utils.debug("q = %s" % q)

                            if thisIsMaster:
                                # Forward the query to all slaves
                                fn.sendMessageToAllSlaves( cmd, "queryType=%s&fromIP=%s&q=%s" % (queryType, myIP, urllib.quote_plus(str(q))) )

                    except Exception, err:
                        stringError = "ERROR: %s" % str(err)
                        utils.debug(stringError)
                        self.write({"error": str(err)})

            elif args[0] == "next" and queryType == "traverse" and not thisIsMaster:
                try:
                    # Slave only - master is signaling next step.
                    fn.hopNum += 1

                    if len(fn.queryQueue["traverse"]) > 0:
                        utils.debug("Got %d vert IDs from other slaves" % len(fn.queryQueue["traverse"]))
                        # We got vert IDs from other slaves
                        sql = db.vertices.update().values(flag=0).where(db.vertices.c.id.in_(fn.queryQueue["traverse"]))
                        v = db.execute(sql)
                        if v > 0:
                            db.execute(fn.traverseSQL)
                        fn.queryQueue["traverse"] = []

                    s = db.execute(fn.traverseSQL)
                    if s > 0:
                        sql = db.vertices.update().values(flag=0).where(db.vertices.c.id.in_(sa.union(sa.select([db.triples.c.s]).where(db.triples.c.flag==0), sa.select([db.triples.c.o]).where(db.triples.c.flag==0))))
                        v = db.execute(sql)
                        if v > 0:
                            db.execute(fn.traverseSQL)

                    fn.distributeIntermediateQueriesToOtherSlaves("traverse")
                    fn.alertMasterThisSlaveIsDone("traverse")

                except Exception, err:
                    stringError = "ERROR: %s" % str(err)
                    utils.debug(stringError)
                    self.write({"error": str(err)})

            if queryType in ["sql", "scan"]:
                self.write({"response": ret})
            else:
                self.write(str(fn.queryQueue.get(queryType, [])))
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write({"error": str(err)})
            raise


class GraphHandler(web.RequestHandler):
    def get(self, action, thing=None, *args):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        utils.debug("REC'D from %s: GET %s" % (self.request.remote_ip, self.request.path))
        try:
            ret = self.respond(action, thing, args)
            try:
                self.write(literal_eval(str(ret)))
            except:
                self.write(str(ret))
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write({"error": str(err)})


    def post(self, action):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        postdata = self.request.arguments
        fromIP = postdata.get("fromIP", [self.request.remote_ip])[0]
        strPostdata = str(postdata)
        if len(strPostdata) > 80:
            strPostdata = strPostdata[:80] + " ..."
        utils.debug("REC'D from %s: POST %s (%s)" % (fromIP, self.request.path, strPostdata))

        ret = {}
        if action == "import" and thisIsMaster:
            self.import_called = True
            # Parse the file and post shards to all slaves
            source = str(postdata.get("source", [None])[0])
            if source.lower().startswith("http"):
                data = net.get(source)
                loader.parseAndShardTriples(data)
                self.write('%s imported' % source)
            elif os.path.isfile(source): # Assume the file exists on the master
                data = open(source, "r").read()
                #utils.debug("data = (%s)" % str(data))
                loader.parseAndShardTriples(data)
                self.write('%s imported' % source)
            else:
                message = "ERROR: Cannot import - '%s' does not exist" % source
                self.debug = message
                utils.debug(message)

        elif action == "add":
            compressed = int(postdata.get("compressed", ["0"])[0])
            if "triples" in postdata.keys():
                triples = postdata["triples"][0]
                if compressed == 1:
                    triples = zlib.decompress(triples)
                triples = literal_eval(triples)
                loader.load(triples)
            elif "raw" in postdata.keys():
                raw = postdata["raw"][0]
                if compressed == 1:
                    raw = zlib.decompress(raw)
                loader.parseAndShardTriples(raw)
            elif "file" in postdata.keys():
                file = postdata["file"][0]
                if os.path.exists(file):
                    lines = []
                    for line in fileinput.input([file]):
                        lines.append(line)
                        if len(lines) % 10000 == 0:
                            loader.parseAndShardTriples("".join(lines))
                            lines = []
                    if len(lines) > 0:
                        loader.parseAndShardTriples("".join(lines))
                        lines = []



    def respond(self, action, thing=None, args=[]):
        args = list(args)
        #utils.debug("args: %s" % str(args))
        table = None
        if thing is not None and thing in ["vertex", "predicate", "triple"]:
            table = {"vertex": "vertices", "predicate": "predicates", "triple": "triples"}[thing]
        if action == "query":
            ret = {"args": args, "step": thing}
        elif action == "add":
            if table == "triples":
                if len(args) == 5:
                    s = self.respond("add", "vertex", [args[0], args[1]]).get("id", -1)
                    p = self.respond("add", "predicate", [args[2]]).get("id", -1)
                    o = self.respond("add", "vertex", [args[3], args[4]]).get("id", -1)
                    args = [s, p, o]
                if args[0] == -1 or args[1] == -1 or args[2] == -1:
                    id = -1
                else:
                    id = utils.keyHash(args[0], args[1], args[2]) #e.g.: -4527674315566272839
                    sql = db.triples.insert()
                    db.execute(sql, [id] + args)
                ret = {"id": id}
            elif thing == "vertex":
                # /graph/add/vertex/person/George
                id = utils.keyHash(args[0], args[1]) #e.g.: -4527674315566272839
                sql = db.vertices.insert()
                db.execute(sql, [id] + args)
                ret = {"id": id}
            elif thing == "predicate":
                id = utils.keyHash(args[0])
                sql = db.predicates.insert()
                db.execute(sql, [id] + args)
                ret = {"id": id}
        elif action == "get":
            if table is not None:
                if args is None or len(args) == 0 or args[0] == "":
                    sql = "SELECT %s FROM `%s`" % table
                    ret = db.execute(sql)
                else:
                    if thisIsMaster:
                        ret = fn.getFromAllSlaves("/graph/%s/%s%s" % (action, thing, "/".join(args)), thing)
                    else:
                        ret = db.execute("SELECT %s FROM `%s` WHERE id = ?" % table, args[0])
                    if len(ret) == 0:
                        ret = None
                    else:
                        ret = ret[0]
            if len(args) == 2 and args[1] == "aslist":
                ret = literal_eval(str(ret))
            else:
                ret = {str(thing): ret}
        elif action == "join":
            predicates = {}
            verts = {}
            triples = []
            if thing is None:
                step = fn.stepNum
            else:
                step = int(str(thing))

            sub = db.vertices.alias()
            obj = db.vertices.alias()
            sql = sa.select([sub.c.vert, db.predicates.c.pred, obj.c.vert]).\
                            select_from(db.triples.join(sub, sub.c.id==db.triples.c.s).\
                                    join(obj, obj.c.id==db.triples.c.o).\
                                    join(db.predicates, db.predicates.c.id==db.triples.c.p)
                            ).\
                            where(db.triples.c.step==step).\
                            distinct()

            triples = db.execute(sql)
            db.execute("reindex")
            ret = {"triples": triples}
        elif action == "hash":
            ret = utils.keyHash(args[0], args[1]) #e.g.: -4527674315566272839
        return ret

class DBHandler(web.RequestHandler):
    def get(self, *args):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        utils.debug("REC'D from %s: GET %s" % (self.request.remote_ip, self.request.path))
        try:
            if args[0] == "rowcount" and len(args) >= 2:
                if len(args) == 2 or args[2] in masterNetIDs: # Local server
                    if thisIsMaster:
                        ret = 0
                        for ip in slaves:
                            raw = ""
                            try:
                                url = "http://%s:%d/db/rowcount/%s" % (ip, port, args[1])
                                raw = net.get(url)
                                if raw.startswith("{"):
                                    rowcount = int(literal_eval(raw).get(args[1], -1))
                                else:
                                    rowcount = 0
                                if rowcount >= 0:
                                    ret += rowcount
                            except Exception, err:
                                utils.debug("ERROR: %s (IP %s)" % (str(err), ip) )
                                ret += 0
                    else:
                        table = args[1].replace("'", "''")
                        sql = sa.select([sa.func.count(db.tables[table].c.id)])
                        raw = db.execute(sql)
                        try:
                            ret = raw[0][0]
                        except Exception, err:
                            ret = -1
                            utils.debug("ERROR: %s - %s" % (str(err), raw) )
                else: # Remote server
                    ret = net.get("http://%s:%d/db/rowcount/%s" % (args[2], port, args[1]))
                    try:
                        ret = literal_eval(ret).get(args[1], -1)
                    except:
                        ret = -1
            ret = {args[len(args)-1]: ret}
            self.write(literal_eval(str(ret)))
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write({"error": str(err)})


    def post(self, *args):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        try:
            ret = ""
            postdata = self.request.arguments
            fromIP = postdata.get("fromIP", [self.request.remote_ip])[0]
            utils.debug("REC'D from %s: POST %s (%s)" % (fromIP, self.request.path, self.request.arguments))
            sql = postdata.get("sql", [None])[0]
            if sql is None:
                sql = args[0] # Should be either 'reindex' or 'vacuum'
            utils.debug("sql: %s" % str(sql))
            if args[0] == "init":
                if thisIsMaster:
                    fn.sendMessageToAllSlaves("/db/init", "fromIP=" % myIP)
                    ret = "database initialization command sent to all slaves"
                else:
                    utils.debug("Creating tables...")
                    db.metadata.create_all(db.engine)
                    ret = "database initialized"
            else:
                if thisIsMaster:
                    ret = {}
                    # In case the user sent a 'LIKE' clause with percent signs, double them up to escape
                    sql = re.sub('%+', '%', sql)
                    sql = re.sub(r'(?<=[^\%])\%(?=[^\%]*)', '%%', sql)
                    fn.initQuery("sql")
                    postdata = "queryType=sql&fromIP=%s&sql=%s" % (myIP, sql)
                    fn.sendMessageToAllSlaves("/db/%s" % args[0], postdata)
                else:
                    try:
                        ret = db.execute(sql)
                        try:
                            error = ret["error"]
                            ret = ["ERROR: %s" % error]
                        except:
                            ret = sorted(ret)
                    except:
                        pass
            self.write(literal_eval(str({"response": ret})))
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write({"error": str(err)})


class APIHandler(web.RequestHandler):
    def get(self, *args):
        self.set_header('Connection', 'close')
        self.set_header('Cache-Control', 'no-cache')
        self.set_header('Accept-Charset', '*')
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        try:
            format = args[0]
            if format == "html":
                ret = rou.getRoutesHTML()
                self.set_header('Content-Type', 'text/html; charset=UTF-8')
            else:
                routes = rou.getRoutes()
                routes.sort()
                ret = {"api": []}
                for route in routes:
                    ret["api"].append({"route": str(route[0][0]), "method": route[1], "comments": route[2], "example": route[3], "link": route[4]})
                self.set_header('Content-Type', 'text/plain; charset=UTF-8')
            self.write(ret)
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)
            self.write({"error": str(err)})



#################################################################################
# Misc Functions
#--------------------------------------------------------------------------------
class MiscFunctions(object):
    def __init__(self):
        self.allpredicates = []
        self.allVertNames = {}
        self.allVertTypes = []
        self.queryQueue = {"traverse": []}
        self.stepsDone = []
        self.hopNum = -1
        self.maxHops = -1
        self.traverseSQL = ""
        self.stepNum = -1
        self.isDone = {"sql": [], "scan": [], "traverse": []}
        self.results = {"sql": {}, "scan": {}, "traverse": {}}
        self.queryTimeouts = {}
        self.hopsBySlaveIndex = {}
        self.queryPctDone = {"sql": {}, "scan": {}, "traverse": {}}

    def initQuery(self, type="traverse"):
        self.queryPctDone[type]["master"] = -1
        self.queryPctDone[type]["slaves"] = -1
        self.isDone[type] = []
        self.results[type] = {}
        if type == "traverse":
            self.queryQueue[type] = []
            self.hopNum = -1
            self.maxHops = -1
            self.traverseSQL = ""
            self.hopsBySlaveIndex = {}
            if not thisIsMaster:
                #db.execute("UPDATE vertices SET flag = -1 WHERE flag > -1")
                #db.execute("UPDATE triples SET flag = -1 WHERE flag > -1")
                for table in ['vertices', 'triples']:
                    db.execute(db.tables[table].update().values(flag=-1).where(db.tables[table].c.flag > -1))

    def sendMessageToAllSlaves(self, route, postdata=None):
        # TODO: Utilize 0MQ's "Dealer" to send to all slaves at once
        for ip in slaves:
            if ip not in masterNetIDs:
                try:
                    http.post(ip, port, route, postdata)
                except Exception, err:
                    utils.debug("Error posting to %s: %s" % (ip, str(err)))


    def getQueryResults(self, queryType):
        try:
            if queryType == "traverse":
                sub = db.vertices.alias()
                obj = db.vertices.alias()
                sql = sa.select([sub.c.vert, db.predicates.c.pred, obj.c.vert]).\
                                select_from(
                                        db.triples.\
                                                join(sub, sub.c.id==db.triples.c.s).\
                                                join(obj, obj.c.id==db.triples.c.o).\
                                                join(db.predicates, db.predicates.c.id==db.triples.c.p)
                                        ).\
                                where(db.triples.c.flag>=0).\
                                distinct()

                utils.debug("Joining tables to get %s query results..." % queryType)
                results = [list(row) for row in db.execute(sql)]
            else:
                results = fn.results[queryType]
            utils.debug("Returning %d %s query results..." % (len(results), queryType))
        except Exception, err:
            utils.debug("Error getting %s query results: %s" % (queryType, str(err)))

        """
        try:
                ignore = str(results)
        except:
                for i in range(len(results)):
                        row = results[i]
                        for j in range(len(row)):
                                val = results[i][j]
                                try:
                                        ignore = unicode(val) # This will bomb if the value contains weird characters
                                except:
                                        results[i][j] = val.decode("utf8")
        """
        return results

    def getFromAllSlaves(self, relURL, key=None, postdata=None):
        try:
            ret = {}
            for ip in slaves:
                if ip not in masterNetIDs:
                    if ip not in ret.keys():
                        ret[ip] = []
                    url = "http://%s:%d%s" % (ip, port, relURL)
                    utils.debug("Requesting %s" % url)
                    if postdata is None:
                        raw = net.get(url)
                    else:
                        raw = net.get(url, "POST", postdata)

                    utils.debug("raw = %s ..." % str(raw)[0:50])

                    if str(raw)[0] in ["{", "["]:
                        if db.dialect == "sqlite":
                            # SQLite may return null values.  Python doesn't understand 'null', so replace with 'None'
                            # This will no longer be necessary since the default values can't be null in new SQL script
                            try:
                                raw = literal_eval(raw)
                            except NameError, err:
                                while raw.find(", null") > 0:
                                    raw = raw.replace(", null", ", None")
                                while raw.find("[null, ") > 0:
                                    raw = raw.replace("[null, ", "[None, ")
                                raw = literal_eval(str(raw))

                        if key is not None and str(raw).strip().startswith("{"):
                            raw = raw.get(key, None)

                    if raw is not None and len(raw) > 0:
                        try:
                            # If this is a list, add its values
                            raw.sort()
                            ret[ip] += raw
                        except:
                            # Not a list, so append the value
                            ret[ip].append(raw)
                        utils.debug("ret = %s ..." % str(ret)[0:50])

            # Remove all blank lines
            for ip in ret.keys():
                while "" in ret[ip]:
                    ret[ip].remove("")
                ret[ip] = utils.deDup(ret[ip], True) #TODO this was originallhy self.dedup, so I don't know if it'll work
            return ret
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)

    def getAllpredicates(self):
        if len(self.allpredicates) == 0:
            cachefile = resource_filename(__name__, "cache/predicates")
            sql = sa.select([db.predicates.c.pred]).distinct()
            if thisIsMaster:
                if os.path.isfile(cachefile) and utils.fileAge(cachefile) < 3600*24:
                    self.allpredicates = literal_eval(open(cachefile, "r").read())
                else:
                    for predicates in self.getFromAllSlaves("/db/execute", "response", "sql=%s" % urllib.quote_plus(sql)):
                        for e in predicates:
                            try:
                                e.sort()
                                self.allpredicates += e
                            except:
                                self.allpredicates.append(e)
                    if len(self.allpredicates) > 0:
                        if len(self.allpredicates) > 1:
                            utils.debug("Deduping %d predicates" % len(self.allpredicates))
                            self.allpredicates = utils.deDup(self.allpredicates, True)  #TODO this was originallhy self.dedup, so I don't know if it'll work 
                        open(cachefile, "w").write(str(self.allpredicates))
            else:
                self.allpredicates = db.execute(sql)
        return self.allpredicates

    def alertMasterThisSlaveIsDone(self, queryType):
        try:
            postdata = "queryType=%s&fromIP=%s" % (queryType, myIP)
            if queryType in ["sql", "scan"]:
                utils.debug("Alerting master that '%s' query is complete" % queryType)
            else:
                triples = 0 #db.execute("SELECT COUNT(*) FROM triples WHERE flag >= 0")[0][0]
                verts = 0 #db.execute("SELECT COUNT(*) FROM vertices WHERE flag >= 0")[0][0]

                postdata += "&hop=%d&triples=%d&verts=%d" % (self.hopNum, triples, verts)
                utils.debug("Alerting master that hopNum %d is complete" % self.hopNum)

            http.post(masterIP, port, '/query/slaveisdone', postdata)
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)

    def startTornado(self):
        utils.debug("Graph server listening on port %d" % port)
        http_server = httpserver.HTTPServer(application)
        http_server.listen(port)
        ioloop.IOLoop.instance().start()

    def distributeIntermediateQueriesToOtherSlaves(self, queryType):
        try:
            if queryType == "traverse":
                # Group the IDs by destination IP
                verts = {}
                sql = sa.select([db.vertices.c.id]).where(db.vertices.c.flag==0)
                for vertID in [row[0] for row in db.execute(sql)]:
                    #utils.debug("vertID = %s" % str(vertID))
                    destIndex = vertID % len(slaves)
                    destIP = slaves[destIndex]
                    if destIP not in masterNetIDs:
                        if destIP not in verts:
                            verts[destIP] = []
                        verts[destIP].append(vertID)

                for destIP in verts:
                    http.post(destIP, port, '/query/queue/traverse', 'fromIP=%s&q=%s' % (myIP, str(verts[destIP]) ))
                    utils.debug("POST 'http://%s:%d/query/queue/traverse' (%d vert IDs)" % (destIP, port, len(verts[destIP])))
        except Exception, err:
            stringError = "ERROR: %s" % str(err)
            utils.debug(stringError)

class SlaveManager(object):
    def __init__(self, slaves, masterNetIDs):
        self.slaves = slaves
        self.masterNetIDs = masterNetIDs

    def get_ip_for_vert(self, vert):
        vertID = utils.keyHash(vert)
        destIndex = vertID % len(self.slaves)
        if destIndex >= len(self.slaves):
            destIndex = 0
        destIP = slaves[destIndex]
        return destIP

# Initialize objects
fn = MiscFunctions()
rou = Routes()

# Get all API routes and initialize Tornado
application = web.Application([route[0] for route in rou.getRoutes()])

masterNetIDs = commands.getoutput("ifconfig | grep 'inet addr' | grep -v '127.0.0.1' | awk -F ' ' '{print $2}' | awk -F ':' '{print $2}'").split("\n")
masterNetIDs += ['localhost', '127.0.0.1']
myIP = masterNetIDs[0]
thisServer = myIP + ":" + str(port)
thisIsMaster = masterIP in masterNetIDs
slave_manager = SlaveManager(slaves, masterNetIDs)

rou.getRoutesHTML(True)
db = pygrendb.DB(dbEngineURL)
loader = dataloader.DataLoader(slave_manager, http, port, myIP, db)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('command', default='daemon', nargs='?',
                        help='pygren action to take',
                        choices=['debug', 'daemon', 'import'])
    parser.add_argument('file', nargs='?', help='file to import')
    args = parser.parse_args()
    if args.command == 'daemon':
        global logFile
        logFile = open(logFile, "a+")
        utils.debug("Starting as daemon (working_directory=%s)" % rootDir)
        context = daemon.DaemonContext(stdout=logFile, stderr=logFile, working_directory=rootDir)
        context.open()
    elif args.command == 'debug':
        utils.debug("Starting in test mode")
    elif args.command == 'import':
        utils.debug("IMPORTING DATAS")
        data = open(args.file).read()
        loader.parseAndShardTriples(data)
        utils.debug('%s imported' % args.file)
        return
    utils.debug("Logging to '%s'" % logFile)
    utils.debug("There are %d slaves in the cluster" % len(slaves))
    utils.debug("Requesting this server's network identities...")
    utils.debug("rootDir = %s" % rootDir)
    utils.debug("rootDir = %s" % rootDir)
    utils.debug("confFile = %s" % confFile)
    utils.debug("Server PID = {0}, Parent PID = {1}".format(os.getpid(), os.getppid()))
    utils.debug("This server's IP: " + myIP)
    if thisIsMaster:
        utils.debug("This is the master")
    else:
        utils.debug("This is a slave.  The master is %s" % masterIP)
    global db
    fn.startTornado()

if __name__ == "__main__":
    main()
