"""
    This represents a single property. The spec is very informal and adhoc.


    String prop

        COMMENT = '<string_literal>'

    Int prop

        DATA_RETENTION_TIME_IN_DAYS = <integer>

    Bool prop

        COPY GRANTS
        CHANGE_TRACKING = { TRUE | FALSE }
"""

import re

import pyparsing as pp

# Compile the regular expressions
OPEN_BRACKET = re.compile(r"\[")
CLOSE_BRACKET = re.compile(r"\]")

equals = pp.Suppress("=")
lbrace = pp.Suppress("{")
rbrace = pp.Suppress("}")
pipe = pp.Suppress("|")
prop_name = pp.Word(pp.alphas + "_", pp.alphanums + "_")
identifier = pp.Word(pp.alphas + "_", pp.alphanums + "_")

enum = pp.Word(pp.alphas.upper() + "-_")

int_value = (pp.Literal("<integer>") | pp.Literal("<num>")).set_parse_action(pp.replaceWith(int))
string_value = (pp.Suppress("'<") + identifier + pp.Suppress(">'")).set_parse_action(pp.replaceWith(str))
bool_value = (pp.Literal("{ TRUE | FALSE }") | pp.Literal("TRUE | FALSE")).set_parse_action(pp.replaceWith(bool))
# enum_value = pp.delimitedList(enum, delim="|").set_parse_action(...)
prop_value = int_value | string_value | bool_value  # | enum_value


simple_kv_prop = prop_name + equals + prop_value + pp.StringEnd()

flag_prop = pp.OneOrMore(pp.Word(pp.alphas)) + pp.StringEnd()


class EntityProp:
    def __init__(self, name, prop_type):
        self.name = name
        self.prop_type = prop_type

    def __repr__(self):
        return f"<EntityProp {self.name} = {self.prop_type.__name__}>"


def parse_prop(prop):
    matches = simple_kv_prop.searchString(prop)
    if matches and matches[0]:
        return EntityProp(*matches[0])

    matches = flag_prop.searchString(prop)
    if matches and matches[0]:
        return EntityProp("_".join(matches[0]), prop_type=bool)


def gen_props(props_str):
    props = []
    lines = props_str.split("\n")
    prop_def = None
    while lines:
        line, lines = lines[0], lines[1:]
        if prop_def is None:
            prop_def = line
        else:
            prop_def += " " + line.strip()

        def_is_complete = len(OPEN_BRACKET.findall(prop_def)) == len(CLOSE_BRACKET.findall(prop_def))
        if def_is_complete:
            print("prop_def:", prop_def)
            prop = parse_prop(prop_def[2:-2])
            if prop:
                props.append(prop)
            prop_def = None

    # This just needs to be test cases
    # for prop in props:
    #     print(prop)

    return dict([(prop.name.lower(), prop) for prop in props])
