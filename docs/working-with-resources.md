# Working with Resources

In the Titan framework, anything in Snowflake that can be created with a `CREATE` statement has a corresponding Python class, such as `Warehouse`, `Database`, `Role`, etc. These act as simple wrappers around configuration with built-in runtime type checking.

## Introduction to Resources

Resources in Titan are designed to be intuitive and straightforward. They encapsulate the configuration of Snowflake objects, ensuring that the properties and relationships between these objects are clearly defined and maintained.

### Instantiation and Configuration

Resources can be instantiated directly in Python with their respective parameters. For example, creating a user or a warehouse involves simply passing the required parameters to the class constructor:


### Resource Interactions

- **Passing Resources**: Resources can be passed directly to other resources to establish relationships or configurations. This can be done by passing the resource instance itself or by referencing its name.

    - **Pass by Instance**: Directly passing the resource instance ensures that the reference is clear and direct.
    - **Pass by Name**: Sometimes it's more convenient or necessary to pass resources by their name, especially when dealing with serialization or configurations that require names as strings.

### Combining Resources

Combining resources refers to the practice of grouping multiple related resources into a coherent structure or configuration. This can be particularly useful in complex setups where multiple resources need to interact closely with each other.

### Containers

Resources can be organized into containers that reflect their hierarchical relationship in Snowflake, such as a `Database` containing multiple `Schemas`, which in turn contain other objects like `Tables` or `Views`.

- **Recommended Method**: Using keyword arguments (`kwargs`) to pass resources ensures clarity and readability.
- **Advanced Method**: The `.add` method or using nested identifiers like "database.schema.table" can be used for more complex or dynamic configurations.

### Quoted Identifiers

Titan attempts to infer the need for quoted identifiers in SQL statements. It is generally recommended to avoid manually quoting identifiers unless absolutely necessary, as Titan handles most of the common cases automatically.

## Advanced Configuration

- **Manual Registration of Dependencies**: While Titan manages dependencies between resources automatically, there are rare cases where manual intervention might be necessary.
    - **Avoid Circular Dependencies**: Design your resource dependencies to avoid circular references, which can lead to errors or undefined behaviors.
    - **Using `Resource.requires(...)`**: This method can be used to explicitly define dependencies if needed, though it is typically not required.

- **Name Qualification**: In complex setups, fully qualifying resource names can help avoid ambiguity and ensure that SQL operations are performed on the correct objects.

By understanding and utilizing these concepts, you can effectively manage and orchestrate Snowflake resources using the Titan framework, making your data infrastructure robust, scalable, and maintainable.
