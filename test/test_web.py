import tornado.httpclient
import tornado.ioloop
import tornado.web
import tornado.testing
import dingus

import pygren

class TestApp(tornado.testing.AsyncHTTPTestCase):
    def get_app(self):
        return pygren.application

    def test_import_nt(self):
        response = self.fetch('/graph/import', method='POST',
                              body='source=test/simple.nt')
        assert 'simple.nt imported' in response.body
        assert not response.error

    def test_import_unicode(self):
        response = self.fetch('/graph/import', method='POST',
                              body='source=test/ntn.n3')
        assert 'ntn.n3 imported' in response.body
        assert not response.error


class TestImport:
    def test_import_nt(self):
        application = dingus.Dingus("application")
        request = dingus.Dingus("request")
        items = dingus.Dingus(), dingus.Dingus()
        application.ui_methods = dingus.Dingus(items__returns=[items])
        request.arguments =  dingus.Dingus(get__returns=["test/simple.nt"])
        g = pygren.GraphHandler(application, request)
        g.post('import')
        assert g.import_called
        assert not getattr(g, 'debug', None)

    def test_import_unicode(self):
        application = dingus.Dingus("application")
        request = dingus.Dingus("request")
        items = dingus.Dingus(), dingus.Dingus()
        application.ui_methods = dingus.Dingus(items__returns=[items])
        request.arguments =  dingus.Dingus(get__returns=["test/ntn.n3"])
        g = pygren.GraphHandler(application, request)
        g.post('import')
        assert g.import_called
        assert not getattr(g, 'debug', None)

class DescribeImport:
    pass
    # Should import from command line
    # Should import from POST
