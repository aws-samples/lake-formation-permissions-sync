"""Convert CloudTrail requestParameters to boto3 API format.

CloudTrail logs parameter names in all-lowercase (e.g. 'databasename'),
while boto3 expects PascalCase (e.g. 'DatabaseName'). Rather than
maintaining a hand-coded lookup table, we build the mapping automatically
from the botocore service model definitions for Glue and Lake Formation.

This gives us 945+ parameter mappings that stay current with the SDK,
instead of the ~94 that were previously hard-coded.
"""

import logging

from botocore.loaders import Loader
from botocore.model import ServiceModel

logger = logging.getLogger(__name__)


def _build_name_map(*service_names):
    """Build a lowercase -> PascalCase name mapping from botocore service models.

    Walks every operation's input shape tree for the given services,
    collecting all member names. This covers every parameter the API accepts.
    """
    loader = Loader()
    mapping = {}
    for service in service_names:
        try:
            api_def = loader.load_service_model(service, "service-2")
            model = ServiceModel(api_def)
            visited = set()
            for op_name in model.operation_names:
                op = model.operation_model(op_name)
                if op.input_shape:
                    _collect_shape_names(op.input_shape, mapping, visited)
        except Exception as e:
            logger.warning("Failed to load service model for %s: %s", service, e)
    return mapping


def _collect_shape_names(shape, mapping, visited):
    """Recursively collect member names from a shape tree."""
    if shape.name in visited:
        return
    visited.add(shape.name)

    if hasattr(shape, "members"):
        for name, member_shape in shape.members.items():
            mapping[name.lower()] = name
            _collect_shape_names(member_shape, mapping, visited)

    if shape.type_name == "list" and hasattr(shape, "member"):
        _collect_shape_names(shape.member, mapping, visited)

    if shape.type_name == "map" and hasattr(shape, "value"):
        _collect_shape_names(shape.value, mapping, visited)


# Build the mapping once at module load time.
# botocore is already a Lambda dependency — no extra packages needed.
NAME_MAP = _build_name_map("glue", "lakeformation")


def cloudtrail_to_boto3_converter(obj):
    """Recursively convert CloudTrail request parameters to boto3 format.

    Transforms all dictionary keys from CloudTrail's lowercase format
    to the PascalCase format expected by boto3 API calls.

    Args:
        obj: A dict, list, or scalar value from CloudTrail requestParameters.

    Returns:
        The same structure with all dict keys converted to PascalCase.
    """
    if isinstance(obj, dict):
        return {NAME_MAP.get(k.lower(), k): cloudtrail_to_boto3_converter(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [cloudtrail_to_boto3_converter(item) for item in obj]
    return obj
