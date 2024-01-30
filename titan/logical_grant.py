class LogicalGrant:
    def __init__(self, urn, priv):
        self.urn = urn
        self.priv = priv

    def __repr__(self):
        return f"LogicalGrant({self.urn}, {self.priv})"

    def __eq__(self, other):
        if isinstance(other, LogicalGrant):
            return self.urn == other.urn and self.priv == other.priv
        return False

    def __hash__(self):
        return hash((self.urn, self.priv))

    def __or__(self, other):
        return Or(self, other)

    def __and__(self, other):
        return And(self, other)


class LogicalExpression:
    def __init__(self, *args):
        self.args = args

    def __repr__(self):
        return f"{self.__class__.__name__}({', '.join(map(str, self.args))})"


class And(LogicalExpression):
    def __and__(self, other):
        return And(*self.args, other)


class Or(LogicalExpression):
    def __or__(self, other):
        return Or(*self.args, other)
