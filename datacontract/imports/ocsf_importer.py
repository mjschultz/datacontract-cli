import re

from json import load
from collections import ChainMap
from logging import getLogger
from typing import List

from datacontract.imports.importer import Importer
from datacontract.model.data_contract_specification import DataContractSpecification, Model, Field
from datacontract.model.exceptions import DataContractException


log = getLogger(__name__)

html_code = re.compile(r"</?code>")
html_all = re.compile(r"<[^>]+>")

PII_TYPES = {
    "email_t",
    "hostname_t",
    "ip_t",
    "mac_t",
    "username_t",
    #'file_name_t',
    #'process_name_t',
    #'url_t',
}
REQUIRED = "required"

# ADDITIONAL OBJECTS
INTERNAL_OBJECTS = {
    "_dns": {
        "caption": "DNS",
        "name": "_dns",
        "description": "The Domain Name System (DNS) object represents the shared information associated with the DNS query and answer objects.",
        "extends": "object",
        "attributes": {
            "class": {
                "description": "The class of resource records being queried. See <a target='_blank' href='https://www.rfc-editor.org/rfc/rfc1035.txt'>RFC1035</a>. For example: <code>IN</code>.",
                "caption": "Resource Record Class",
                "requirement": "recommended",
            },
            "packet_uid": {
                "description": "The DNS packet identifier assigned by the program that generated the query. The identifier is copied to the response.",
                "requirement": "recommended",
            },
            "type": {
                "description": "The type of resource records being queried. See <a target='_blank' href='https://www.rfc-editor.org/rfc/rfc1035.txt'>RFC1035</a>. For example: A, AAAA, CNAME, MX, and NS.",
                "caption": "Resource Record Type",
                "requirement": "recommended",
            },
        },
    },
    "_entity": {
        "caption": "Entity",
        "name": "_entity",
        "description": "The Entity object is an unordered collection of attributes, with a name and unique identifier. It serves as a base object that defines a set of attributes and default constraints available in all objects that extend it.",
        "extends": "object",
        "attributes": {
            "name": {"description": "The name of the entity.", "requirement": "recommended"},
            "uid": {"description": "The unique identifier of the entity.", "requirement": "recommended"},
        },
        "constraints": {"at_least_one": ["name", "uid"]},
    },
    "_resource": {
        "caption": "Resource",
        "description": "The Resource object contains attributes that provide information about a particular resource. It serves as a base object, offering attributes that help identify and classify the resource effectively.",
        "extends": "_entity",
        "name": "_resource",
        "profiles": ["data_classification"],
        "attributes": {
            "$include": ["profiles/data_classification.json"],
            "data": {"description": "Additional data describing the resource.", "requirement": "optional"},
            "labels": {"description": "The list of labels/tags associated to a resource.", "requirement": "optional"},
            "name": {"description": "The name of the resource."},
            "type": {"description": "The resource type as defined by the event source.", "requirement": "optional"},
            "uid": {"description": "The unique identifier of the resource."},
        },
    },
}


def clean_html(html):
    html = html_code.sub("`", html)
    html = html_all.sub("", html)
    return html


class DeprecatedField(Exception):
    pass


class OcsfImporter(Importer):
    def import_source(
        self, data_contract_specification: DataContractSpecification, source: str, import_args: dict
    ) -> DataContractSpecification:
        ocsf_contract = OcsfToContract.from_json(source)
        ocsf_classes = import_args.get("ocsf_class")
        return import_ocsfschema(data_contract_specification, ocsf_contract, ocsf_classes)


class OcsfToContract:
    """
    Based on the information found in "Understanding OCSF[1]".

    Keys are "OCSF" names.
    Values are DataContract Spec.

    [1]: https://github.com/ocsf/ocsf-docs/blob/main/Understanding%20OCSF.md
    """

    types: dict  # from scalar data types and objects
    models: dict  # from classes

    __base_types = {
        "boolean_t": "boolean",
        "float_t": "float",
        "integer_t": "integer",
        "json_t": "string",
        "long_t": "long",
        "string_t": "string",
    }

    @classmethod
    def from_json(cls, source: str):
        try:
            with open(source, "r") as file:
                schema = load(file)
        except Exception as e:
            raise DataContractException(
                type="schema",
                name="Parse json/ocsf schema",
                reason=f"Failed to parse json/ocsf schema from {source}",
                engine="datacontract",
                original_exception=e,
            )
        return cls(schema)

    def __init__(self, base_schema):
        self.base_schema = base_schema
        self.base_schema["objects"] = dict(
            ChainMap(
                self.base_schema["objects"],
                INTERNAL_OBJECTS,  # why doesn't ocsf-lib-py include?
            )
        )
        self.ocsf_types = {None: {}}
        self.ocsf_objects = {None: {}}

    def _clean_dict(self, d):
        return {k: v for k, v in d.items() if v is not None}

    def _build_scalar_type(self, name):
        spec = self.base_schema["types"][name]
        true_type = spec.get("type") or name
        description = clean_html(spec.get("description"))
        min_len = spec.get("min_len")
        max_len = spec.get("min_len")
        pattern = spec.get("regex")
        range_ = spec.get("range") or [None, None]
        observable_id = spec.get("observable")

        type_ = {
            "type": self.__base_types.get(true_type),
            "description": description,
            "minLength": min_len,
            "maxLength": max_len,
            # "pattern": pattern,  # TODO: yaml formatting is messed up
            "minimum": range_[0],
            "maximum": range_[1],
            "observable_id": observable_id,
            "pii": name in PII_TYPES,
        }
        return self._clean_dict(type_)

    def get_scalar_type(self, name):
        if name not in self.ocsf_types:
            self.ocsf_types[name] = self._build_scalar_type(name)
        return self.ocsf_types[name]

    def _build_object(self, name):
        spec = self.base_schema["objects"][name]
        extends = spec.get("extends")
        base_object = self.get_object(extends)
        this_object = {
            "name": name,
            "title": spec.get("caption"),
            "description": clean_html(spec.get("description")),
            "type": "object",
            "fields": self.get_fields(spec["attributes"]),
        }
        this_object = self._clean_dict(this_object)
        return dict(ChainMap(this_object, base_object))

    def get_object(self, name):
        if name not in self.ocsf_objects:
            self.ocsf_objects[name] = self._build_object(name)
        return self.ocsf_objects[name]

    def _get_field_args(self, name, spec):
        kwargs = {
            "title": spec.get("caption") or name,
            "description": clean_html(spec.get("description")),
            "tags": [],
        }

        if spec.get("deprecated"):
            raise DeprecatedField()

        if name == "$include":
            # Need an example of this
            log.warning(f"not handling `$include`: {name} for {spec}")
            return kwargs

        group = spec.get("group")
        if group:
            kwargs["tags"].append(group)

        profile = spec.get("profile")
        if group:
            kwargs["tags"].append(f"profile:{profile}")

        observable_id = spec.get("observable")
        if observable_id:
            kwargs["observable"] = observable_id

        # determine type
        obj_type = spec.get("object_type")
        base_type = spec.get("type")
        if obj_type:
            base = {
                'type': 'object',
                '$ref': f"#/definitions/{obj_type}",
            }
        else:
            base = self.get_scalar_type(base_type)

        is_array = spec.get("is_array")
        if is_array:
            # array should put the type info into the `items` field
            base = {
                'type': 'array',
                'items': base,
            }

        kwargs = dict(ChainMap(kwargs, base))

        enum = spec.get("enum")
        if enum:
            # DataContract requires str type here
            # TODO: disagree with that
            kwargs['enum'] = [str(k) for k in enum.keys()]

        # requirement(s)
        requirement = spec.get("requirement")
        if requirement == REQUIRED:
            kwargs["required"] = True
        elif requirement:
            kwargs["tags"].append(requirement)

        return kwargs

    def get_fields(self, attributes):
        fields = {}
        for name, spec in attributes.items():
            try:
                fields[name] = Field(**self._get_field_args(name, spec))
            except DeprecatedField:
                log.warning(f"{name}: is deprecated, skipping")

        return fields

    def get_definitions(self, model_fields, known_definitions=None):
        known_definitions = known_definitions or {}
        for field_name, field in model_fields.items():
            ref = field.ref
            if not (ref and ref.startswith("#/definitions/")):
                continue

            obj_to_include = ref.split("/")[-1]
            if obj_to_include in known_definitions:
                continue

            defn = self.get_object(obj_to_include)
            known_definitions[obj_to_include] = defn
            known_definitions = self.get_definitions(defn["fields"], known_definitions)
        return known_definitions


def import_ocsfschema(
    data_contract_specification: DataContractSpecification, ocsf_contract: OcsfToContract, ocsf_classes: List[str]
) -> DataContractSpecification:
    if data_contract_specification.models is None:
        data_contract_specification.models = {}

    if ocsf_classes:
        ocsf_classes = [c.replace("-", "_") for c in ocsf_classes]
        input_models = {c: ocsf_contract.base_schema["classes"][c] for c in ocsf_classes}
    else:
        input_models = ocsf_contract.base_schema["classes"]

    for name, spec in input_models.items():
        fields = ocsf_contract.get_fields(spec["attributes"])
        kwargs = {
            "title": spec.get("name", name),
            "description": clean_html(spec.get("description")),
            "type": spec.get("type", "table"),
            "fields": fields,
        }
        # would be nice to have profiles and category/category uid
        # associations/constraints/deprecated
        # need to use "extends" somehow"

        model = Model(**kwargs)
        data_contract_specification.models[name] = model

    definitions = {}
    for model_name, model in data_contract_specification.models.items():
        definitions = ocsf_contract.get_definitions(model.fields, definitions)
    data_contract_specification.definitions = definitions

    return data_contract_specification
