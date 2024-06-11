import pyparsing as pp

Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
FullyQualifiedIdentifier = (
    pp.delimited_list(Identifier, delim=".", min=4, max=4)
    ^ pp.delimited_list(Identifier, delim=".", min=3, max=3)
    ^ pp.delimited_list(Identifier, delim=".", min=2, max=2)
    ^ Identifier
)
