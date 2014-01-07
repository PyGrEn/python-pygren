"""
dataloader.py

Created by David Arsenault, Brett Beaudoin and Ryan Freckleton, The MITRE Corporation
Approved for Public Release; Distribution Unlimited. 13-3052
 ©2014 - The MITRE Corporation. All rights reserved.
"""
import collections
import urllib
import zlib

import pygrenutils as utils

class DataLoader(object):
    """
    For importing triples.
    """
    def __init__(self, slave_manager, http, port, myIP, db):
        self.sent = []
        self.dataToInsert = {"triples": [], "vertices": [], "predicates": []}
        self.slave_manager = slave_manager
        self.http = http
        self.port = port
        self.myIP = myIP
        self.db = db

    def addVertex(self, vert=""):
        try:
            if str(vert) != "":
                id = utils.keyHash(vert)
                newData = {'id':id, 'vert':vert.replace("'", "''")}
                self.dataToInsert["vertices"].append(newData)
            else:
                id = 0
        except:
            utils.debug("Vertex rejected: %s" % vert)
            id = -1
        return id

    def addPredicate(self, pred=""):
        try:
            if str(pred) != "":
                id = utils.keyHash(pred)
                newData = {'id':id, 'pred':pred.replace("'", "''")}
                self.dataToInsert["predicates"].append(newData)
            else:
                id = 0
        except:
            utils.debug("Predicate rejected: %s" % pred)
            id = -1
        return id

    def determine_dataToInsert(self, tripleList):
        """
        Add triples.  This will also add vertices and predicates.
        """
        for s, p, o in tripleList:
            sID = self.addVertex(s)
            pID = self.addPredicate(p)
            oID = self.addVertex(o)
            if sID > 0 and pID > 0 and oID > 0:
                id = utils.keyHash(sID, pID, oID)
                newData = {'id':id, 's':sID, 'p':pID, 'o':oID }
                self.dataToInsert["triples"].append(newData)

    def dedupe_dataToInsert(self):
        for table in self.dataToInsert.keys():
            self.dataToInsert[table] = utils.deDup(self.dataToInsert[table])

    def insert_data(self):
        self.tripleCount = 0
        for table in self.dataToInsert.keys():
            self.tripleCount += len(self.dataToInsert[table])
            rows = len(self.dataToInsert[table])
            if rows:
                utils.debug("Attempting to add %d %s" % (rows, table))
                # Insert the new data
                sql = self.db.tables[table].insert()
                rowsAdded = self.db.execute(sql, self.dataToInsert[table])
                utils.debug("Added %s %s (%s ignored)" % (rowsAdded, table,
                                                          max(0,rows-rowsAdded)))
            else:
                utils.debug("No insert statements")
            self.dataToInsert[table] = []

    def load(self, tripleList):
        """
        This is a slave-only function.
        """
        self.determine_dataToInsert(tripleList)
        self.dedupe_dataToInsert()
        self.insert_data()

    def parseAndShardTriples(self, rawText):
        """
        This is a master-only function
        """
        triples = utils.parse(rawText)
        ip_triples = collections.defaultdict(list)
        self.tripleCount = 0 # count of triples for debugging
        for triple in triples:
            s, __, o = triple
            for vert in s, o:
                destIP = self.slave_manager.get_ip_for_vert(vert)
                ip_triples[destIP].append(triple)
        for destIP in ip_triples:
            if ip_triples[destIP]:
                self.tripleCount += len(ip_triples[destIP])
                self.sendTriplesToSlave(destIP, utils.deDup(ip_triples[destIP]))
                utils.debug("%d percent complete" % int(50.0*self.tripleCount / len(ip_triples)))
                ip_triples[destIP] = []
        utils.debug("100 percent complete")
        utils.debug("Sharded %d triples" % self.tripleCount)

    def sendTriplesToSlave(self, slaveIP, triples):
        # Be sure not to send them to the master
        self.sent.append((slaveIP, triples))
        if slaveIP not in self.slave_manager.masterNetIDs:
            utils.debug("Sending %d triples to %s" % (len(triples), slaveIP))
            # Compress the data before sending
            triples = urllib.quote_plus(zlib.compress(str(triples)))
            # TODO: Mock this
            self.http.post(slaveIP, self.port, '/graph/add', "compressed=1&fromIP=%s&triples=%s" % (self.myIP, triples) )
