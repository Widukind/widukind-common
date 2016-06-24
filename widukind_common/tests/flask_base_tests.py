# -*- coding: utf-8 -*-

from urllib.parse import urlparse
import base64
import logging

from flask import json, url_for, template_rendered

try:
    from flask_login import user_logged_in, user_logged_out
    HAVE_FLASK_LOGIN = True
except ImportError:
    HAVE_FLASK_LOGIN = False
    
from widukind_common.tests.base import BaseDBTestCase    

logger = logging.getLogger(__name__)

JSON_HEADERS = {
    'Content-Type': 'application/json',
    'Accept': 'application/json',
}

class ContextVariableDoesNotExist(Exception):
    pass

class BaseFlaskTestCase(BaseDBTestCase):
    
    CONFIG = None
    
    def _create_app(self):
        raise NotImplementedError
    
    def setUp(self):
        super().setUp()
        
        self.flask_app = self._create_app()
        self.client = self.flask_app.test_client()
        self._ctx = self.flask_app.test_request_context()
        self._ctx.push()

        self.json_mod = json
        self.templates = []
        self.current_user = None
        template_rendered.connect(self._add_template)
        if HAVE_FLASK_LOGIN:
            user_logged_in.connect(self._signal_login)
            user_logged_out.connect(self._signal_logout)

    def tearDown(self):
        super().tearDown()
        template_rendered.disconnect(self._add_template)
        if HAVE_FLASK_LOGIN:
            user_logged_in.disconnect(self._signal_login)
            user_logged_out.disconnect(self._signal_logout)        
        _ctx = getattr(self, '_ctx', None)
        if self._ctx:
            self._ctx.pop()

    def _signal_login(self, sender, user=None):
        self.current_user = user

    def _signal_logout(self, sender, user=None):
        self.current_user = None

    def login_basic_headers(self, username=None, password=None):
        headers = {}
        headers['Authorization'] = 'Basic ' + base64.b64encode(('%s:%s' % (username, password)).encode('latin1')).strip().decode('latin1')
        return headers
    
    def login(self, username=None, password=None, url=None, follow_redirects=True):
        url = url or url_for('login')
        data = {
            'username': username, 
            'password': password
        }
        return self.client.post(url, data=data, follow_redirects=follow_redirects)
    
    def logout(self, url=None):
        #FIXME:
        url = url or url_for('logout')
        return self.client.get(url)

    def _add_template(self, app, template, context, **kwargs):

        if len(self.templates) > 0:
            self.templates = []
        
        self.templates.append((template, context))
        
    def assertTemplateUsed(self, name, tmpl_name_attribute='name'):
        used_templates = []

        for template, context in self.templates:
            if getattr(template, tmpl_name_attribute) == name:
                return True

            used_templates.append(template)

        raise AssertionError("template %s not used. Templates were used: %s" % (name, ' '.join(used_templates)))

    assert_template_used = assertTemplateUsed

    def get_context_variable(self, name):
        for template, context in self.templates:
            if name in context:
                return context[name]
        raise ContextVariableDoesNotExist

    def assertInContext(self, name):
        try:
            self.get_context_variable(name)
        except ContextVariableDoesNotExist:
            self.fail("Context variable does not exist: %s" % name)

    def assertContext(self, name, value=None, message=None):
        try:
            if value:
                self.assertEqual(self.get_context_variable(name), value, message)
            else:
                self.assertIsNotNone(self.get_context_variable(name), message)
        except ContextVariableDoesNotExist:
            self.fail(message or "Context variable does not exist: %s" % name)

    assert_context = assertContext

    def assertStatusCode(self, response, status_code):
        self.assertEquals(status_code, response.status_code)
        return response

    def assertOk(self, response):
        return self.assertStatusCode(response, 200)
    assert_200 = assertOk

    def assertBadRequest(self, response):
        return self.assertStatusCode(response, 400)
    assert_400 = assertBadRequest

    def assertRequireAuth(self, response):
        return self.assertStatusCode(response, 401)
    assert_401 = assertRequireAuth

    def assertForbidden(self, response):
        return self.assertStatusCode(response, 403)
    assert_403 = assertForbidden

    def assertNotFound(self, response):
        return self.assertStatusCode(response, 404)
    assert_404 = assertNotFound

    def assertMethodNotAllowed(self, response):
        return self.assertStatusCode(response, 405)
    assert_405 = assertMethodNotAllowed

    def assertRedirects(self, response, location):
        
        # Doit être un des 2 codes
        self.assertTrue(response.status_code in (301, 302))
        
        # Ne doit pas être None
        self.assertIsNotNone(response.location)
        
        url = urlparse(response.location)
    
        path = url.path
    
        self.assertEqual(path, location)
        
    assert_redirects = assertRedirects

    def assertContentType(self, response, content_type):
        """Assert the content-type of a Flask test client response

        :param response: The test client response object
        :param content_type: The expected content type
        """
        self.assertEquals(content_type, response.headers['Content-Type'])
        return response

    def assertOkHtml(self, response):
        """Assert the response status code is 200 and an HTML response

        :param response: The test client response object
        """
        return self.assertOk(
            self.assertContentType(response, 'text/html; charset=utf-8'))
        
    def json_post(self, url=None, dict_data=None, headers={}, follow_redirects=True):
        data=self.json_mod.dumps(dict_data)
        headers.update(JSON_HEADERS)
        return self.client.post(url, headers=headers, data=data, follow_redirects=follow_redirects)

    def json_get(self, url=None, headers={}, follow_redirects=True):
        headers.update(JSON_HEADERS)
        return self.client.get(url, headers=headers, follow_redirects=follow_redirects)
    
    def response_json(self, response):
        return self.json_mod.loads(response.data)

    def assertJson(self, response):
        """Test that content returned is in JSON format

        :param response: The test client response object
        """
        try:
            self.json_mod.loads(response.data)
        except Exception as err:
            msg = "error: %s - data: %s" % (str(err), getattr(response, 'data', ''))
            return self.fail(msg)
        
        return self.assertContentType(response, 'application/json')
    
    def assertOkJson(self, response):
        """Assert the response status code is 200 and a JSON response

        :param response: The test client response object
        """
        return self.assertOk(self.assertJson(response))

    def assertBadJson(self, response):
        """Assert the response status code is 400 and a JSON response

        :param response: The test client response object
        """
        return self.assertBadRequest(self.assertJson(response))
        
    def get_current_user(self):
        #from flask_security import current_user
        #return current_user
        return self.current_user

    def assertIsAuthenticated(self, username=None):
        
        user = self.get_current_user()

        self.assertIsNotNone(user)
        
        if username:
            self.assertEqual(getattr(user, 'username'), username)

    def assertIsNotAuthenticated(self):
        
        #TODO: vérifier si Anonymous ?
        
        self.assertIsNone(self.get_current_user())
        