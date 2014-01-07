import dingus

import pygren.dataloader as mod

class DescribeParseAndShardTriples:
    def setup_method(self, method):
        self.slave_manager = dingus.Dingus("slave_manager")
        self.dl = mod.DataLoader(self.slave_manager, dingus.Dingus("http"),
                                 dingus.Dingus("port"), dingus.Dingus("myIP"),
                                 dingus.Dingus("db"))
        with open('test/simple.nt') as f:
            raw = f.read()
        self.dl.parseAndShardTriples(raw)

    def should_send_to_slaves(self):
        assert self.dl.sent == [(self.slave_manager.get_ip_for_vert(),
                                [('subject', 'predicate', 'object')])]

    def should_send_for_subject_and_object(self):
        assert self.dl.tripleCount == 2

class DescribeLoad:
    def should_load_triples(self):
        triples = [('subject', 'predicate', 'object')]
        slave_manager = dingus.Dingus("slave_manager")
        dl = mod.DataLoader(slave_manager, dingus.Dingus("http"),
                                 dingus.Dingus("port"), dingus.Dingus("myIP"),
                                 dingus.Dingus("db"))
        dl.load(triples)
        assert dl.tripleCount == 4
        assert dl.db.calls('execute')
