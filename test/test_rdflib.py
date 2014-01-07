import pygren.pygrenutils as mod
import rdflib
from rdflib import Literal as L

class CheckRDFlib:
    def check_parse_triple(self):
        g = rdflib.Graph()
        with open('test/simple.nt') as f:
            raw = g.parse(f, format='n3').serialize(format='nt')
            f.seek(0)
            assert raw == f.read()

    def check_literal(self):
        assert L('foo') == L('foo')

    def check_triples(self):
        import rdflib
        g = rdflib.Graph()
        with open('test/simple.nt') as f:
            g.parse(f, format='n3')
        assert list(g) == [(L('subject'), L('predicate'), L('object'))]

class DescribeTripleParser:
    def should_return_tuple_of_strings(self):
        with open('test/simple.nt') as f:
            simple = f.read()
        assert mod.parse(simple).next() == ('subject', 'predicate', 'object')
