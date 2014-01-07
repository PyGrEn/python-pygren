PyGREN -- Python Graph Engine
=============================
Approved for Public Release; Distribution Unlimited. 13-3052
Copyright ©2014 - The MITRE Corporation. All rights reserved.

PyGREN is a distributed graph database engine. It's based on top of Tornado and
SQLAlchemy and implements a Bulk Synchronous Parallel system.
http://en.wikipedia.org/wiki/Bulk_Synchronous_Parallel

Currently, it supports:
 - distributed SQL queries
 - Simple subject/predicate/object queries
 - Dynamic updates and addition of data
 - Processing jobs
 - SPARQL queries (work in progress)

Requirements
------------
 - (Python 2.7?)
 - Unix like operating system
 - SQLAlchemy
 - lockfile
 - python-daemon
 - tornado

Installation
------------
 - Installable via pip:

    pip install pygren

 TODO: Install on slaves using pybundles (possibly a script to do this


Using PyGREN
------------
### Starting ###

To start pygren, type

    pygren debug

at the command line. Use CTRL-C to stop it.

### Usage ###

PyGEN is accessible through a human readable HTTP interface as well as a
machine-friendly RESTful interface. When PyGREN starts up, it will present the
graphical user interface at 127.0.0.1:8080.

#### End Points ####
 - /api/
 - /db/
 - /graph/
 - /query/

### Importing Data ###
There are several ways to import data with pygren. The simplest is through the command line:

    pygren import <filename>

Terminology
----------
 - PyGREN stores triples in a graph
 - A query is a request of data in the graph
 - Bulk Synchronous Parallel requests
