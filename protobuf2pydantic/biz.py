from os import linesep
from typing import List, Set


import logging
from google.protobuf.reflection import GeneratedProtocolMessageType
from google.protobuf.descriptor import Descriptor, FieldDescriptor, EnumDescriptor

logging.basicConfig()

logger = logging.getLogger(__name__)

tab = " " * 4
one_line, two_lines = linesep * 2, linesep * 3
type_mapping = {
    FieldDescriptor.TYPE_DOUBLE: float,
    FieldDescriptor.TYPE_FLOAT: float,
    FieldDescriptor.TYPE_INT64: int,
    FieldDescriptor.TYPE_UINT64: int,
    FieldDescriptor.TYPE_INT32: int,
    FieldDescriptor.TYPE_FIXED64: float,
    FieldDescriptor.TYPE_FIXED32: float,
    FieldDescriptor.TYPE_BOOL: bool,
    FieldDescriptor.TYPE_STRING: str,
    FieldDescriptor.TYPE_BYTES: str,
    FieldDescriptor.TYPE_UINT32: int,
    FieldDescriptor.TYPE_SFIXED32: float,
    FieldDescriptor.TYPE_SFIXED64: float,
    FieldDescriptor.TYPE_SINT32: int,
    FieldDescriptor.TYPE_SINT64: int,
}


def m(field: FieldDescriptor) -> str:
    return type_mapping[field.type].__name__


def convert_field(level: int, field: FieldDescriptor, class_names: Set[str],
                  class_name_prefix: str) -> str:
    level += 1
    field_type = field.type
    field_label = field.label
    extra = None
    is_part_of_oneof = field.containing_oneof is not None

    if field_type == FieldDescriptor.TYPE_ENUM:
        enum_type: EnumDescriptor = field.enum_type
        type_statement = enum_type.name
        class_statement = f"{tab * level}class {enum_type.name}(IntEnum):"
        field_statements = map(
            lambda value: f"{tab * (level + 1)}{value.name} = {value.index}",
            enum_type.values,
        )
        extra = linesep.join([class_statement, *field_statements])
        factory = "int"
    elif field_type == FieldDescriptor.TYPE_MESSAGE:
        type_statement: str = field.message_type.name
        if type_statement.endswith("Entry"):
            key, value = field.message_type.fields  # type: FieldDescriptor
            type_statement = f"Dict[{m(key)}, {m(value)}]"
            factory = "dict"
        elif type_statement == "Struct":
            type_statement = "Dict[str, Any]"
            factory = "dict"
        else:
            if field.message_type.name not in class_names:
                extra = msg2pydantic(level, field.message_type, class_names,
                                     class_name_prefix)
            factory = type_statement
    else:
        type_statement = m(field)
        factory = type_statement

    if field_label == FieldDescriptor.LABEL_REPEATED:
        type_statement = f"List[{type_statement}]"
        factory = "list"

    default_statement = f" = Field(default_factory={factory})"
    if field_label == FieldDescriptor.LABEL_REQUIRED:
        default_statement = ""
    if is_part_of_oneof:
        type_statement = f"Optional[{type_statement}]"
    field_statement = f"{tab * level}{field.name}: {type_statement}"
    if not is_part_of_oneof:
        field_statement += default_statement
    if not extra:
        return field_statement
    return linesep + extra + one_line + field_statement


def msg2pydantic(level: int, msg: Descriptor, class_names: Set[str],
                 class_name_prefix: str = "",
                 skip_name_check: bool = False) -> str:
    prefixed_class_name = f"{class_name_prefix}{msg.name}"
    if prefixed_class_name in class_names and not skip_name_check:
        return ""
    class_names.add(prefixed_class_name)

    class_statement = f"{tab * level}class {msg.name}(BaseModel):"
    field_statements = [
        convert_field(level, field, class_names, f"{prefixed_class_name}-")
        for field in msg.fields
    ]

    if len(field_statements) == 0:
        field_statements.append(f'{tab * (level + 1)}pass')
    return linesep.join([class_statement, *field_statements])


def get_config(level: int):
    level += 1
    class_statement = f"{tab * level}class Config:"
    attribute_statement = f"{tab * (level + 1)}arbitrary_types_allowed = True"
    return linesep + class_statement + linesep + attribute_statement


def pb2_to_pydantic(module) -> str:
    pydantic_models: List[str] = []
    class_names: Set[str] = set()

    descriptors = [
        getattr(module, m).DESCRIPTOR for m in vars(module).keys()
        if isinstance(getattr(module, m), GeneratedProtocolMessageType)
    ]

    pydantic_models = [
        msg2pydantic(0, descriptor, class_names, skip_name_check=True)
        for descriptor in descriptors
    ]
    pydantic_models = [m for m in pydantic_models if m != ""]

    header = """from typing import List, Dict, Any, Optional
from enum import IntEnum

from pydantic import BaseModel, Field


"""
    return header + two_lines.join(pydantic_models)
