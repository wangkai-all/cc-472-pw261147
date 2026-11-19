"""Rule definitions, config validation and the five checkers.

Rule types
----------
primary_key   config: {"fields": ["id", ...]}
              Fails when a key is null/blank, duplicated inside the batch, or
              collides with an already-persisted valid record.
unique        config: {"fields": ["email", ...]}
              Fails on duplicate value-combinations inside the batch; nulls are
              ignored (a unique constraint never fires on null).
range         config: {"field": "age", "min": 0, "max": 120,
                       "min_inclusive": true, "max_inclusive": true}
              Fails when a non-null value falls outside the interval or is not
              numeric; null fails only when "allow_null": false.
cross_table   config: {"field": "customer_id",
                       "ref_dataset": "customers", "ref_field": "id"}
              Fails when a value has no matching record in the reference
              dataset (referential integrity); null fails only when
              "allow_null": false.
expression    config: {"expr": "price * quantity >= 0 and is_null(note) ..."}
              Fails when the (AST-evaluated) expression is false or raises.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .expr import ExpressionError, evaluate, validate_expression

RULE_TYPES = ("primary_key", "unique", "range", "cross_table", "expression")


class RuleConfigError(Exception):
    pass


def validate_config(rule_type: str, config: dict[str, Any]) -> None:
    if rule_type not in RULE_TYPES:
        raise RuleConfigError(f"unknown rule type {rule_type!r}")
    if not isinstance(config, dict):
        raise RuleConfigError("config must be an object")

    def require_fields(names: list[str]) -> None:
        for name in names:
            if name not in config:
                raise RuleConfigError(f"{rule_type} rule requires config field {name!r}")

    if rule_type in ("primary_key", "unique"):
        require_fields(["fields"])
        fields = config["fields"]
        if not isinstance(fields, list) or not fields or not all(
            isinstance(f, str) and f for f in fields
        ):
            raise RuleConfigError("'fields' must be a non-empty list of column names")

    elif rule_type == "range":
        require_fields(["field"])
        if not isinstance(config["field"], str) or not config["field"]:
            raise RuleConfigError("'field' must be a non-empty string")
        if "min" not in config and "max" not in config:
            raise RuleConfigError("range rule requires at least one of 'min'/'max'")
        for bound in ("min", "max"):
            if bound in config and config[bound] is not None and not isinstance(
                config[bound], (int, float)
            ):
                raise RuleConfigError(f"'{bound}' must be numeric or null")

    elif rule_type == "cross_table":
        require_fields(["field", "ref_dataset", "ref_field"])
        for name in ("field", "ref_dataset", "ref_field"):
            if not isinstance(config[name], str) or not config[name]:
                raise RuleConfigError(f"'{name}' must be a non-empty string")

    elif rule_type == "expression":
        require_fields(["expr"])
        if not isinstance(config["expr"], str):
            raise RuleConfigError("'expr' must be a string")
        validate_expression(config["expr"])


def make_key(row: dict[str, Any], fields: list[str]) -> str:
    return "~".join("" if row.get(f) is None else str(row.get(f)) for f in fields)


@dataclass
class CheckFailure:
    record_key: str
    reason: str


class Checker:
    """Stateless per-execution checker built from one published rule version."""

    def __init__(self, rule_id: str, rule_type: str, config: dict[str, Any]):
        self.rule_id = rule_id
        self.rule_type = rule_type
        self.config = config
        self._seen: dict[str, int] = {}

    # -- helpers -----------------------------------------------------------
    @staticmethod
    def _is_blank(value: Any) -> bool:
        return value is None or (isinstance(value, str) and value.strip() == "")

    def check(
        self,
        row: dict[str, Any],
        record_key: str,
        existing_keys: set[str] | None = None,
        ref_values: set[str] | None = None,
    ) -> CheckFailure | None:
        existing_keys = existing_keys or set()
        try:
            if self.rule_type == "primary_key":
                return self._check_primary_key(row, record_key, existing_keys)
            if self.rule_type == "unique":
                return self._check_unique(row, record_key)
            if self.rule_type == "range":
                return self._check_range(row, record_key)
            if self.rule_type == "cross_table":
                return self._check_cross_table(row, record_key, ref_values or set())
            return self._check_expression(row, record_key)
        except ExpressionError as exc:
            return CheckFailure(record_key, f"expression error: {exc}")
        except (TypeError, ValueError) as exc:
            return CheckFailure(record_key, f"value error: {exc}")

    # -- the five checkers -------------------------------------------------
    def _check_primary_key(
        self, row: dict[str, Any], record_key: str, existing_keys: set[str]
    ) -> CheckFailure | None:
        fields = self.config["fields"]
        blanks = [f for f in fields if self._is_blank(row.get(f))]
        if blanks:
            return CheckFailure(record_key, f"primary key field(s) {blanks} are null/blank")
        key = make_key(row, fields)
        if key in existing_keys:
            return CheckFailure(record_key, f"primary key {key!r} conflicts with an existing record")
        seen = self._seen.get(key, 0)
        self._seen[key] = seen + 1
        if seen:
            return CheckFailure(record_key, f"duplicate primary key {key!r} within batch")
        return None

    def _check_unique(self, row: dict[str, Any], record_key: str) -> CheckFailure | None:
        fields = self.config["fields"]
        values = [row.get(f) for f in fields]
        if any(v is None for v in values):
            return None  # SQL-style: unique constraints ignore nulls
        combo = "~".join(str(v) for v in values)
        seen = self._seen.get(combo, 0)
        self._seen[combo] = seen + 1
        if seen:
            return CheckFailure(
                record_key, f"duplicate unique value {combo!r} on {fields}"
            )
        return None

    def _check_range(self, row: dict[str, Any], record_key: str) -> CheckFailure | None:
        field = self.config["field"]
        value = row.get(field)
        allow_null = self.config.get("allow_null", True)
        if value is None:
            if not allow_null:
                return CheckFailure(record_key, f"{field!r} is null but not nullable")
            return None
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return CheckFailure(record_key, f"{field}={value!r} is not numeric")
        lower, upper = self.config.get("min"), self.config.get("max")
        lo_ok = lower is None or value > lower or (
            self.config.get("min_inclusive", True) and value == lower
        )
        hi_ok = upper is None or value < upper or (
            self.config.get("max_inclusive", True) and value == upper
        )
        if not (lo_ok and hi_ok):
            return CheckFailure(
                record_key,
                f"{field}={value} outside range [{lower}, {upper}]",
            )
        return None

    def _check_cross_table(
        self, row: dict[str, Any], record_key: str, ref_values: set[str]
    ) -> CheckFailure | None:
        field, ref_dataset, ref_field = (
            self.config["field"],
            self.config["ref_dataset"],
            self.config["ref_field"],
        )
        value = row.get(field)
        allow_null = self.config.get("allow_null", True)
        if value is None:
            if not allow_null:
                return CheckFailure(record_key, f"{field!r} is null but not nullable")
            return None
        if str(value) not in ref_values:
            return CheckFailure(
                record_key,
                f"{field}={value!r} has no match in {ref_dataset}.{ref_field}",
            )
        return None

    def _check_expression(self, row: dict[str, Any], record_key: str) -> CheckFailure | None:
        passed = evaluate(self.config["expr"], row)
        if not passed:
            return CheckFailure(record_key, f"expression false: {self.config['expr']}")
        return None
