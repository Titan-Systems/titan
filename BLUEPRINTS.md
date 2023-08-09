# Blueprints

## Parent-child relationships

```Python

# Read
schema = Schema(name="foo")
schema.parent # => Database
schema.children # => {Table, View, Function, ...}

# Update
schema.add(Table(name="tbl"))


# Read by resource type
tbl = schema.tables["tbl"]
for tbl in schema.tables:
    ...


# Stubs cannot read children
schema = Schema.stub("ANALYTICS") # this is a stub
for tbl in schema.tables: # so this fails
    print(tbl.name)

# Live schemas can read children
schema = Schema.find("ANALYTICS")
for tbl in schema.tables: # this works
    print(tbl.name)

```











Not sure what a solid pythonic way is to implement blueprints.

This seems fine

```
bp = titan.Blueprint("my-first-blueprint")
bp.add(
    titan.Database("foo"),
    titan.Schema("bar", database="foo"),
    titan.Table(...),
)
```

But I want something a little more automatic. I want to make the add(...) automatic to reduce effort

```
db = titan.Database("foo")
db.add(
    titan.Schema("bar"),
    titan.Table("tbl1", schema="bar"),
)
```

or

```
titan.Database("foo")
titan.Schema("bar", database="foo")
titan.Table("tbl1", schema="bar") # Should this be valid?
```

or

```
titan.Database("foo")
titan.Schema("bar", database="foo")
titan.Table("tbl1", schema=titan.Schema.all["bar"])
```


```
titan.Schema.all["bar"]
titan.Schema.get("bar")
titan.Schema.get("bar", id="foo.bar")
```


```
titan.Schema("bar", database="foo")

db = titan.Database("foo")
titan.Schema("bar", database=db)

db = titan.Database("foo")
db.add(titan.Schema("bar"))

db = titan.Database("foo")
sc = titan.Schema("bar")
sc.database = db
```


```
bp = titan.Blueprint("my-first-blueprint")
bp.Database("foo")
bp.Schema("bar", database="foo")
bp.Table(...)
```