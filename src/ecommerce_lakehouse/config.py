import re


def quote_identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_-]*", value):
        raise ValueError(f"Invalid catalog, schema, or table identifier: {value!r}")
    return f"`{value}`"


def qualified_name(*parts):
    return ".".join(quote_identifier(part) for part in parts)


def pipeline_namespace(spark, layer):
    return qualified_name(
        spark.conf.get("ecommerce.catalog"),
        spark.conf.get(f"ecommerce.{layer}_schema"),
    )