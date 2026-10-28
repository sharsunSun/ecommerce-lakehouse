from importlib.resources import files

from ecommerce_lakehouse.config import quote_identifier


def render_sql(filename, *, alter_metric_view=False, **identifiers):
    if filename not in {"initialize.sql", "revenue_metrics.sql"}:
        raise ValueError(f"Unknown SQL resource: {filename!r}")
    template = files("ecommerce_lakehouse").joinpath("sql", filename)
    return template.read_text(encoding="utf-8").format(
        view_operation="ALTER VIEW" if alter_metric_view else "CREATE VIEW IF NOT EXISTS",
        view_options="" if alter_metric_view else "WITH METRICS LANGUAGE YAML",
        **{name: quote_identifier(value) for name, value in identifiers.items()})



