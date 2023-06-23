NOTES
1. I want add resources to a DAG
2. I want to enforce the semantics of the DAG at execution time. Most important, I want to refuse to add resources to the DAG that would create cycles.
3. All resources have a name. This is a global identifier
4. Resources can rely on other resources this is the directed edge in the DAG.
4. I want to enable lazy references for rely-on relationships.

For example:

```Python

d = DAG()

a = A(name="a")
b = B(name="b")
b.relies_on(a)

c = C(name="c")
c.relies_on("b")

```

Please give advice on how I should handle this.
