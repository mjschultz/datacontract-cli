from pyiceberg.schema import Schema
from pyiceberg import types

from datacontract.model.data_contract_specification import (
    DataContractSpecification,
    Model,
    Field,
)
from datacontract.export.exporter import Exporter


# TODO: make a class
field_id = 0


class IcebergExporter(Exporter):
    """
    Exporter class for exporting data contracts to Iceberg schemas.
    """

    def export(
        self,
        data_contract: DataContractSpecification,
        model,
        server,
        sql_server_type,
        export_args,
    ) -> dict[str, types.StructType]:
        """
        Export the given data contract to Iceberg schemas.

        Args:
            data_contract (DataContractSpecification): The data contract specification.
            model: Not used in this implementation.
            server: Not used in this implementation.
            sql_server_type: Not used in this implementation.
            export_args: Additional arguments for export.

        Returns:
            dict[str, types.StructType]: A dictionary mapping model names to their corresponding Iceberg schemas.
        """
        return to_iceberg(data_contract)


def to_iceberg(contract: DataContractSpecification) -> str:
    """
    Converts a DataContractSpecification into a Iceberg schema string.

    Args:
        contract (DataContractSpecification): The data contract specification containing models.

    Returns:
        str: A string representation of the Iceberg schema for each model in the contract.
    """
    ddl = []
    partitions = []
    location = ''
    for model_name, model in contract.models.items():
        schema = to_iceberg_schema(model)
        stmt_parts = [f'CREATE TABLE "{model_name}" ({schema_str(schema)})']
        if partitions:
            stmt_parts.append(f'PARTITIONED BY ({", ".join(partitions)}) ')
        if location:
            stmt_parts.append(f"LOCATION '{location}' ")
        stmt_parts.append("TBLPROPERTIES ( 'table_type' = 'ICEBERG' )")
        ddl.append(' '.join(stmt_parts))
    return '\n\n'.join(ddl)


def to_iceberg_schema(model: Model) -> types.StructType:
    """
    Convert a model to a Iceberg schema.

    Args:
        model (Model): The model to convert.

    Returns:
        types.StructType: The corresponding Iceberg schema.
    """
    fields = []
    for field_name, field in model.fields.items():
        field = make_field(field_name, field)
        fields.append(field)
    schema = Schema(*fields)
    return schema


def make_field(field_name, field):
    global field_id
    field_id += 1
    field_type = get_field_type(field)
    return types.NestedField(
        field_id=field_id,
        name=field_name,
        field_type=field_type,
        required=field.required
    )


def to_struct_type(fields: dict[str, Field]) -> types.StructType:
    """
    Convert a dictionary of fields to a Iceberg StructType.

    Args:
        fields (dict[str, Field]): The fields to convert.

    Returns:
        types.StructType: The corresponding Iceberg StructType.
    """
    struct_fields = []
    for field_name, field in fields.items():
        struct_field = make_field(field_name, field)
        struct_fields.append(struct_field)
    return types.StructType(*struct_fields)


def get_field_type(field: Field) -> types.IcebergType:
    """
    Convert a field to a Iceberg IcebergType.

    Args:
        field (Field): The field to convert.

    Returns:
        types.IcebergType: The corresponding Iceberg IcebergType.
    """
    field_type = field.type
    if field_type is None or field_type in ["null"]:
        return types.NullType()
    if field_type == "array":
        return types.ArrayType(to_data_type(field.items))
    if field_type in ["object", "record", "struct"]:
        return to_struct_type(field.fields)
    if field_type in ["string", "varchar", "text"]:
        return types.StringType()
    if field_type in ["number", "decimal", "numeric"]:
        return types.DecimalType()
    if field_type in ["integer", "int"]:
        return types.IntegerType()
    if field_type == "long":
        return types.LongType()
    if field_type == "float":
        return types.FloatType()
    if field_type == "double":
        return types.DoubleType()
    if field_type == "boolean":
        return types.BooleanType()
    if field_type in ["timestamp", "timestamp_tz"]:
        return types.TimestampType()
    if field_type == "timestamp_ntz":
        return types.TimestampNTZType()
    if field_type == "date":
        return types.DateType()
    if field_type == "bytes":
        return types.BinaryType()
    return types.BinaryType()


def schema_str(schema: Schema) -> str:
    """
    Converts a PyIceberg IcebergType schema to its equivalent code representation.

    Args:
        dtype (types.IcebergType): The PyIceberg IcebergType schema to be converted.

    Returns:
        str: The code representation of the PyIceberg IcebergType schema.
    """

    def indent(text: str, level: int) -> str:
        """
        Indents each line of the given text by a specified number of levels.

        Args:
            text (str): The text to be indented.
            level (int): The number of indentation levels.

        Returns:
            str: The indented text.
        """
        return "\n".join([f'{"    " * level}{line}' for line in text.split("\n")])

    def handle_field_type(field):
        field_type = field.field_type
        if isinstance(field_type, types.StructType):
            things = schema_str(field_type)
            return f'struct<{things}>'
        elif isinstance(field_type, types.ListType):
            things = '???'
            return f'array<{things}>'
        elif isinstance(field_type, types.DecimalType):
            return f"decimal({dtype.precision}, {dtype.scale})"
        else:
            dtype_str = str(field_type)
            return dtype_str

    top_level = []
    for field in schema.fields:
        field_type = handle_field_type(field)
        top_level.append(f'{field.name} {field_type}')

    _str = ',\n'.join(indent(t, 1) for t in top_level)
    return f'\n{_str}\n'

