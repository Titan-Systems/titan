# Vars

First pass proposal at vars behavior

## YAML/CLI

titan.yml
```yaml
vars:
  - name: foobar
    type: string
    default: some_default_value
    sensitive: true
databases:
  - name: "db_{{ var.foobar }}"
```

```sh
titan plan --config titan.yml --vars '{"foobar": "blimblam"}'
titan plan --config titan.yml --vars 'foobar: blimblam'
```

## Python
```Python
from titan.blueprint import Blueprint
from titan import var

# Deferred style
db = Database(name=var.foobar)
# Interpolation style
db = Database(name="db_{{var.foobar}}")
Blueprint(resources=[db], vars={"foobar": "blimblam"})
```