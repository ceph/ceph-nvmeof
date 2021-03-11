class ScopeNotValid(Exception):
    def __init__(self, name):
        super(ScopeNotValid, self).__init__(
            "Scope '{}' is not valid".format(name))
