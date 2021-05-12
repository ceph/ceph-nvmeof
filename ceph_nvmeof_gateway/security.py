import inspect


class Scope(object):
    """
    List of API Security Scopes.
    """

    BACKEND = "backend"

    @classmethod
    def all_scopes(cls):
        return [val for scope, val in
                inspect.getmembers(cls,
                                   lambda memb: not inspect.isroutine(memb))
                if not scope.startswith('_')]

    @classmethod
    def valid_scope(cls, scope_name):
        return scope_name in cls.all_scopes()
