import apispec.ext.marshmallow.common
import cherrypy
import collections
from enum import Enum
from functools import wraps
import importlib
import inspect
import json
import logging
import marshmallow.schema
import marshmallow.fields
import os
import pkgutil
import re
from typing import Any, Dict, List, Optional
from urllib.parse import unquote

from .. import exceptions
from .. import security

ENDPOINT_MAP = collections.defaultdict(list)  # type: dict


logger = logging.getLogger(__name__)


def load():
    controllers_dir = os.path.dirname(os.path.realpath(__file__))
    logger.debug("controllers_dir=%s", controllers_dir)

    controllers = []
    mods = [mod for _, mod, _ in pkgutil.iter_modules([controllers_dir])]
    logger.debug("mods=%s", mods)
    for mod_name in mods:
        mod = importlib.import_module('.controllers.{}'.format(mod_name),
                                      package='ceph_nvmeof_gateway')
        for _, cls in mod.__dict__.items():
            if inspect.isclass(cls) and issubclass(cls, Controller) and \
                    hasattr(cls, '_cp_controller_'):
                if not cls._cp_path_.startswith('/'):
                    # invalid _cp_path_ value
                    logger.error("Invalid url prefix '%s' for controller '%s'",
                                 cls._cp_path_, cls.__name__)
                    continue
                controllers.append(cls)
    logger.debug("controllers={}".format(controllers))

    return controllers


def generate_controller_routes(endpoint, mapper):
    inst = endpoint.inst
    ctrl_class = endpoint.ctrl

    conditions = dict(method=[endpoint.method])

    parent_url = endpoint.url

    # parent_url might be of the form "/.../{...}" where "{...}" is a path parameter
    # we need to remove the path parameter definition
    parent_url = re.sub(r'(?:/\{[^}]+\})$', '', parent_url)
    if not parent_url:  # root path case
        parent_url = "/"

    url = endpoint.url

    logger.debug("Mapped [%s] to %s:%s restricted to %s",
                 url, ctrl_class.__name__, endpoint.action,
                 endpoint.method)

    ENDPOINT_MAP[endpoint.url].append(endpoint)

    name = ctrl_class.__name__ + ":" + endpoint.action
    mapper.connect(name, url, controller=inst, action=endpoint.action,
                   conditions=conditions)

    # adding route with trailing slash
    name += "/"
    url += "/"
    mapper.connect(name, url, controller=inst, action=endpoint.action,
                   conditions=conditions)

    return parent_url


def generate_routes():
    mapper = cherrypy.dispatch.RoutesDispatcher()
    ctrls = load()

    parent_urls = set()

    endpoint_list = []
    for ctrl in ctrls:
        inst = ctrl()
        for endpoint in ctrl.endpoints():
            endpoint.inst = inst
            endpoint_list.append(endpoint)

    endpoint_list = sorted(endpoint_list, key=lambda e: e.url)
    for endpoint in endpoint_list:
        parent_urls.add(generate_controller_routes(endpoint, mapper))

    logger.debug("list of parent paths: %s", parent_urls)
    return mapper, parent_urls


def json_error_page(status, message, traceback, version):
    cherrypy.response.headers['Content-Type'] = 'application/json'
    return json.dumps(dict(status=status, detail=message, traceback=traceback,
                           version=version))


def _get_json_request_model(request, schema):
    if request.method not in request.methods_with_bodies:
        raise cherrypy.HTTPError(400, 'Unexpected body')

    content_type = request.headers.get('Content-Type', '')
    if content_type not in ['application/json', 'text/javascript'] or \
            not hasattr(request, 'json'):
        raise cherrypy.HTTPError(400, 'Expected JSON body')
    if isinstance(request.json, str):
        return schema.loads(request.json, session=request.db)
    else:
        return schema.load(request.json, session=request.db)


def _get_request_body_params(request):
    """
    Helper function to get parameters from the request body.
    :param request The CherryPy request object.
    :type request: cherrypy.Request
    :return: A dictionary containing the parameters.
    :rtype: dict
    """
    params = {}  # type: dict
    if request.method not in request.methods_with_bodies:
        return params

    content_type = request.headers.get('Content-Type', '')
    if content_type in ['application/json', 'text/javascript']:
        if not hasattr(request, 'json'):
            raise cherrypy.HTTPError(400, 'Expected JSON body')
        if isinstance(request.json, str):
            params.update(json.loads(request.json))
        else:
            params.update(request.json)

    return params


def _getargspec(func):
    try:
        while True:
            func = func.__wrapped__
    except AttributeError:
        pass
    # pylint: disable=deprecated-method
    return inspect.getfullargspec(func)


def _get_function_params(func):
    """
    Retrieves the list of parameters declared in function.
    Each parameter is represented as dict with keys:
      * name (str): the name of the parameter
      * required (bool): whether the parameter is required or not
      * default (obj): the parameter's default value
    """
    fspec = _getargspec(func)

    func_params = []
    nd = len(fspec.args) if not fspec.defaults else -len(fspec.defaults)
    for param in fspec.args[1:nd]:
        func_params.append({'name': param, 'required': True})

    if fspec.defaults:
        for param, val in zip(fspec.args[nd:], fspec.defaults):
            func_params.append({
                'name': param,
                'required': False,
                'default': val
            })

    return func_params


def _to_snake_case(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


class ControllerRoute(object):
    def __init__(self, path, security_scope=None, secure=True):
        if security_scope and not security.Scope.valid_scope(security_scope):
            raise exceptions.ScopeNotValid(security_scope)
        self.path = path
        self.security_scope = security_scope
        self.secure = secure

        if self.path and self.path[0] != "/":
            self.path = "/" + self.path

    def __call__(self, cls):
        cls._cp_controller_ = True
        cls._cp_path_ = self.path
        cls._security_scope = self.security_scope

        config = {
            'tools.authenticate.on': False,  # TODO
        }
        if not hasattr(cls, '_cp_config'):
            cls._cp_config = {}
        cls._cp_config.update(config)
        return cls


class FrontendControllerRoute(ControllerRoute):
    def __init__(self, path, security_scope=None, secure=True):
        super(FrontendControllerRoute, self).__init__(path, security_scope, secure)


class BackendControllerRoute(ControllerRoute):
    def __init__(self, path):
        if path and path[0] != "/":
            path = "/" + path
        path = "/backend{}".format(path)

        super(BackendControllerRoute, self).__init__(path, security_scope=security.Scope.BACKEND,
                                                     secure=True)

    def __call__(self, cls):
        cls = super(BackendControllerRoute, self).__call__(cls)
        cls._backend_endpoint = True
        return cls


def Endpoint(method=None, path=None, path_params=None, query_params=None,
             json_response=True):
    if method is None:
        method = 'GET'
    elif not isinstance(method, str) or \
            method.upper() not in ['GET', 'POST', 'DELETE', 'PUT']:
        raise TypeError("Possible values for method are: 'GET', 'POST', "
                        "'DELETE', or 'PUT'")

    method = method.upper()

    if method in ['GET', 'DELETE']:
        if path_params is not None:
            raise TypeError("path_params should not be used for {} "
                            "endpoints. All function params are considered"
                            " path parameters by default".format(method))

    if path_params is None:
        if method in ['POST', 'PUT']:
            path_params = []

    if query_params is None:
        query_params = []

    def _wrapper(func):
        if method in ['POST', 'PUT']:
            func_params = _get_function_params(func)
            for param in func_params:
                if param['name'] in path_params and not param['required']:
                    raise TypeError("path_params can only reference "
                                    "non-optional function parameters")

        if func.__name__ == '__call__' and path is None:
            e_path = ""
        else:
            e_path = path

        if e_path is not None:
            e_path = e_path.strip()
            if e_path and e_path[0] != "/":
                e_path = "/" + e_path
            elif e_path == "/":
                e_path = ""

        func._endpoint = {
            'method': method,
            'path': e_path,
            'path_params': path_params,
            'query_params': query_params,
            'json_response': json_response,
        }
        return func
    return _wrapper


class Controller:
    class Endpoint(object):
        """
        An instance of this class represents an endpoint.
        """

        def __init__(self, ctrl, func):
            self.ctrl = ctrl
            self.inst = None
            self.func = func
            setattr(self.ctrl, func.__name__, self.function)

        @property
        def config(self):
            func = self.func
            while not hasattr(func, '_endpoint'):
                if hasattr(func, "__wrapped__"):
                    func = func.__wrapped__
                else:
                    return None
            return func._endpoint

        @property
        def function(self):
            return self.ctrl._request_wrapper(self.func, self.method,
                                              self.config['json_response'])

        @property
        def method(self):
            return self.config['method']

        @property
        def url(self):
            ctrl_path = self.ctrl.get_path()
            if ctrl_path == "/":
                ctrl_path = ""
            if self.config['path'] is not None:
                url = "{}{}".format(ctrl_path, self.config['path'])
            else:
                url = "{}/{}".format(ctrl_path, self.func.__name__)

            ctrl_path_params = self.ctrl.get_path_param_names(
                self.config['path'])
            path_params = [p['name'] for p in self.path_params
                           if p['name'] not in ctrl_path_params]
            path_params = ["{{{}}}".format(p) for p in path_params]
            if path_params:
                url += "/{}".format("/".join(path_params))

            return url

        @property
        def action(self):
            return self.func.__name__

        @property
        def path_params(self):
            ctrl_path_params = self.ctrl.get_path_param_names(
                self.config['path'])
            func_params = _get_function_params(self.func)

            if self.method in ['GET', 'DELETE']:
                assert self.config['path_params'] is None

                return [p for p in func_params if p['name'] in ctrl_path_params
                        or (p['name'] not in self.config['query_params']
                            and p['required'])]

            # elif self.method in ['POST', 'PUT']:
            return [p for p in func_params if p['name'] in ctrl_path_params
                    or p['name'] in self.config['path_params']]

        @property
        def query_params(self):
            if self.method in ['GET', 'DELETE']:
                func_params = _get_function_params(self.func)
                path_params = [p['name'] for p in self.path_params]
                return [p for p in func_params if p['name'] not in path_params]

            # elif self.method in ['POST', 'PUT']:
            func_params = _get_function_params(self.func)
            return [p for p in func_params
                    if p['name'] in self.config['query_params']]

        @property
        def body_params(self):
            func_params = _get_function_params(self.func)
            path_params = [p['name'] for p in self.path_params]
            query_params = [p['name'] for p in self.query_params]
            return [p for p in func_params
                    if p['name'] not in path_params
                    and p['name'] not in query_params]

        @property
        def is_backend_api(self):
            # changed from hasattr to getattr: some ui-based api inherit _api_endpoint
            return getattr(self.ctrl, '_backend_endpoint', False)

    @classmethod
    def get_path_param_names(cls, path_extension=None):
        if path_extension is None:
            path_extension = ""
        full_path = cls._cp_path_[1:] + path_extension  # type: ignore
        path_params = []
        for step in full_path.split('/'):
            param = None
            if not step:
                continue
            if step[0] == ':':
                param = step[1:]
            elif step[0] == '{' and step[-1] == '}':
                param, _, _ = step[1:-1].partition(':')
            if param:
                path_params.append(param)
        return path_params

    @classmethod
    def get_path(cls):
        return cls._cp_path_  # type: ignore

    @classmethod
    def endpoints(cls):
        """
        This method iterates over all the methods decorated with ``@endpoint``
        and creates an Endpoint object for each one of the methods.

        :return: A list of endpoint objects
        :rtype: list[Controller.Endpoint]
        """
        result = []
        for _, func in inspect.getmembers(cls, predicate=callable):
            if hasattr(func, '_endpoint'):
                result.append(cls.Endpoint(cls, func))
        return result

    @staticmethod
    def _request_wrapper(func, method, json_response):
        @wraps(func)
        def inner(*args, **kwargs):
            for key, value in kwargs.items():
                if isinstance(value, str):
                    kwargs[key] = unquote(value)

            if hasattr(func, 'body_params_schema'):
                kwargs[_to_snake_case(func.body_params_schema.__class__.__name__)] = \
                    _get_json_request_model(cherrypy.request, func.body_params_schema)
            else:
                # Process method arguments.
                params = _get_request_body_params(cherrypy.request)
                kwargs.update(params)

            ret = func(*args, **kwargs)
            if isinstance(ret, bytes):
                ret = ret.decode('utf-8')
            if json_response or hasattr(func, 'response_schema'):
                cherrypy.response.headers['Content-Type'] = 'application/json'
            if hasattr(func, 'response_schema'):
                ret = func.response_schema.dumps(ret).encode('utf8')
            elif json_response:
                ret = json.dumps(ret).encode('utf8')
            return ret
        return inner

    def __init__(self):
        logger.info('Initializing controller: %s -> %s',
                    self.__class__.__name__, self._cp_path_)  # type: ignore
        super(Controller, self).__init__()

    @property
    def db(self):
        return cherrypy.request.db


class RESTController(Controller):

    _method_mapping = collections.OrderedDict([
        ('list', {'method': 'GET', 'resource': False, 'status': 200}),
        ('create', {'method': 'POST', 'resource': False, 'status': 201}),
        ('bulk_set', {'method': 'PUT', 'resource': False, 'status': 200}),
        ('bulk_delete', {'method': 'DELETE', 'resource': False, 'status': 204}),
        ('get', {'method': 'GET', 'resource': True, 'status': 200}),
        ('delete', {'method': 'DELETE', 'resource': True, 'status': 204}),
        ('set', {'method': 'PUT', 'resource': True, 'status': 200}),
        ('singleton_set', {'method': 'PUT', 'resource': False, 'status': 200})
    ])

    @classmethod
    def infer_resource_id(cls):
        if hasattr(cls, 'RESOURCE_ID'):
            if cls.RESOURCE_ID is not None:
                return cls.RESOURCE_ID.split('/')

        for k, v in cls._method_mapping.items():
            func = getattr(cls, k, None)
            while hasattr(func, "__wrapped__"):
                func = func.__wrapped__
            if v['resource'] and func:
                path_params = cls.get_path_param_names()
                params = _get_function_params(func)
                return [p['name'] for p in params
                        if p['required'] and p['name'] not in path_params]
        return None

    @classmethod
    def endpoints(cls):
        result = super(RESTController, cls).endpoints()
        res_id_params = cls.infer_resource_id()

        for _, func in inspect.getmembers(cls, predicate=callable):
            no_resource_id_params = False
            status = 200
            method = None
            query_params = None
            path = ""

            if func.__name__ in cls._method_mapping:
                meth = cls._method_mapping[func.__name__]  # type: dict

                if meth['resource']:
                    if not res_id_params:
                        no_resource_id_params = True
                    else:
                        path_params = ["{{{}}}".format(p) for p in res_id_params]
                        path += "/{}".format("/".join(path_params))

                status = meth['status']
                method = meth['method']

            else:
                continue

            if no_resource_id_params:
                raise TypeError("Could not infer the resource ID parameters for"
                                " method {} of controller {}. "
                                "Please specify the resource ID parameters "
                                "using the RESOURCE_ID class property"
                                .format(func.__name__, cls.__name__))

            if method in ['GET', 'DELETE']:
                params = _get_function_params(func)
                if res_id_params is None:
                    res_id_params = []
                if query_params is None:
                    query_params = [p['name'] for p in params
                                    if p['name'] not in res_id_params]

            func = cls._status_code_wrapper(func, status)
            endp_func = Endpoint(method, path=path,
                                 query_params=query_params)(func)
            result.append(cls.Endpoint(cls, endp_func))

        return result

    @classmethod
    def _status_code_wrapper(cls, func, status_code):
        @wraps(func)
        def wrapper(*vpath, **params):
            cherrypy.response.status = status_code
            return func(*vpath, **params)

        return wrapper


class SchemaType(Enum):
    """
    Representation of the type property of a schema object:
    http://spec.openapis.org/oas/v3.0.3.html#schema-object
    """
    ARRAY = 'array'
    BOOLEAN = 'boolean'
    INTEGER = 'integer'
    NUMBER = 'number'
    OBJECT = 'object'
    STRING = 'string'

    def __str__(self):
        return str(self.value)


class Schema:
    """
    Representation of a schema object:
    http://spec.openapis.org/oas/v3.0.3.html#schema-object
    """

    def __init__(self, schema_type: SchemaType = SchemaType.OBJECT,
                 properties: Optional[Dict] = None, required: Optional[List] = None):
        self._type = schema_type
        self._properties = properties if properties else {}
        self._required = required if required else []

    def as_dict(self) -> Dict[str, Any]:
        schema: Dict[str, Any] = {'type': str(self._type)}

        if self._type == SchemaType.ARRAY:
            items = Schema(properties=self._properties)
            schema['items'] = items.as_dict()
        else:
            schema['properties'] = self._properties

        if self._required:
            schema['required'] = self._required

        return schema


class SchemaInput:
    """
    Simple DTO to transfer data in a structured manner for creating a schema object.
    """
    type: SchemaType


def EndpointDoc(description="", group="", parameters=None, responses=None):  # noqa: N802
    if not isinstance(description, str):
        raise Exception("%s has been called with a description that is not a string: %s"
                        % (EndpointDoc.__name__, description))
    if not isinstance(group, str):
        raise Exception("%s has been called with a groupname that is not a string: %s"
                        % (EndpointDoc.__name__, group))
    if parameters and not isinstance(parameters, dict) and \
            not isinstance(parameters, marshmallow.schema.Schema):
        raise Exception("%s has been called with parameters that is not a schema: %s"
                        % (EndpointDoc.__name__, parameters))
    if responses and not isinstance(responses, dict):
        raise Exception("%s has been called with responses that is not a schema: %s"
                        % (EndpointDoc.__name__, responses))

    if not parameters:
        parameters = {}

    def _split_param(name, p_type, description, optional=False, default_value=None, nested=False):
        param = {
            'name': name,
            'description': description,
            'required': not optional,
            'nested': nested,
        }
        if default_value:
            param['default'] = default_value
        if isinstance(p_type, type):
            param['type'] = p_type
        else:
            nested_params = _split_parameters(p_type, nested=True)
            if nested_params:
                param['type'] = type(p_type)
                param['nested_params'] = nested_params
            else:
                param['type'] = p_type
        return param

    #  Optional must be set to True in order to set default value and parameters format must be:
    # 'name: (type or nested parameters, description, [optional], [default value])'
    def _split_dict(data, nested):
        splitted = []
        for name, props in data.items():
            if isinstance(name, str) and isinstance(props, tuple):
                if len(props) == 2:
                    param = _split_param(name, props[0], props[1], nested=nested)
                elif len(props) == 3:
                    param = _split_param(name, props[0], props[1], optional=props[2],
                                         nested=nested)
                if len(props) == 4:
                    param = _split_param(name, props[0], props[1], props[2], props[3], nested)
                splitted.append(param)
            else:
                raise Exception(
                    """Parameter %s in %s has not correct format. Valid formats are:
                    <name>: (<type>, <description>, [optional], [default value])
                    <name>: (<[type]>, <description>, [optional], [default value])
                    <name>: (<[nested parameters]>, <description>, [optional], [default value])
                    <name>: (<{nested parameters}>, <description>, [optional], [default value])"""
                    % (name, EndpointDoc.__name__))
        return splitted

    def _split_list(data, nested):
        splitted = []  # type: List[Any]
        for item in data:
            splitted.extend(_split_parameters(item, nested))
        return splitted

    # nested = True means parameters are inside a dict or array
    def _split_parameters(data, nested=False, response=False):
        param_list = []  # type: List[Any]
        if isinstance(data, dict):
            param_list.extend(_split_dict(data, nested))
        elif isinstance(data, (list, tuple)):
            param_list.extend(_split_list(data, True))
        elif isinstance(data, marshmallow.schema.Schema):
            fields = apispec.ext.marshmallow.common.get_fields(data)
            for name, field in fields.items():
                if not response and field.dump_only:
                    continue

                param = {
                    'name': name,
                    'description': field.metadata.get('description', ''),
                    'required': field.required,
                    'nested': False,
                }

                field_mapping = {
                    marshmallow.fields.List: list,
                    marshmallow.fields.Boolean: bool,
                    marshmallow.fields.Integer: int,
                    marshmallow.fields.Number: float,
                    marshmallow.fields.String: str,
                }

                for field_class in type(field).__mro__:
                    if field_class in field_mapping:
                        param['type'] = field_mapping[field_class]
                        break

                if field.default:
                    param['default'] = field.default

                param_list.append(param)
        return param_list

    resp = {}
    resp_schema = None
    if responses:
        for status_code, response_body in responses.items():
            if isinstance(response_body, marshmallow.schema.Schema):
                resp_schema = response_body

            schema_input = SchemaInput()
            schema_input.type = SchemaType.ARRAY if \
                isinstance(response_body, list) else SchemaType.OBJECT
            schema_input.params = _split_parameters(response_body, response=True)

            resp[str(status_code)] = schema_input

    def _wrapper(func):
        func.doc_info = {
            'summary': description,
            'tag': group,
            'parameters': _split_parameters(parameters),
            'response': resp
        }

        if isinstance(parameters, marshmallow.schema.Schema):
            func.body_params_schema = parameters
        if resp_schema:
            func.response_schema = resp_schema

        return func

    return _wrapper


class ControllerDoc(object):
    def __init__(self, description="", group=""):
        self.tag = group
        self.tag_descr = description

    def __call__(self, cls):
        cls.doc_info = {
            'tag': self.tag,
            'tag_descr': self.tag_descr
        }
        return cls
