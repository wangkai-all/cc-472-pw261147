"""Safe restricted Python-expression evaluator for custom expression rules.

Only literals, names (resolved against the record row), comparisons, boolean
and arithmetic operators, and a small whitelist of builtins are permitted.
No attribute access, no calls outside the whitelist, no names starting with
underscores. Evaluation runs on the AST — ``eval`` is never called.
"""
from __future__ import annotations

import ast
import builtins
import operator
from typing import Any

_SAFE_BINOPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_SAFE_CMPOPS = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda a, b: a in b,
    ast.NotIn: lambda a, b: a not in b,
}
_SAFE_UNARY = {ast.Not: operator.not_, ast.USub: operator.neg, ast.UAdd: operator.pos}
_SAFE_BUILTINS = {
    "len": len,
    "abs": abs,
    "min": min,
    "max": max,
    "round": round,
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "sum": sum,
    "any": any,
    "all": all,
    "lower": lambda s: str(s).lower(),
    "upper": lambda s: str(s).upper(),
    "is_null": lambda v: v is None,
}
# Only list/tuple/set/dict containers are allowed as call arguments.
_SAFE_CONTAINERS = (list, tuple, set, dict)


class ExpressionError(Exception):
    """Raised when an expression is invalid or cannot be evaluated."""


class _Evaluator(ast.NodeVisitor):
    def __init__(self, names: dict[str, Any]):
        self.names = names

    def visit_Expression(self, node: ast.Expression) -> Any:
        return self.visit(node.body)

    def visit_Constant(self, node: ast.Constant) -> Any:
        return node.value

    def visit_Name(self, node: ast.Name) -> Any:
        if node.id.startswith("_"):
            raise ExpressionError(f"name {node.id!r} is not allowed")
        if node.id in _SAFE_BUILTINS:
            return _SAFE_BUILTINS[node.id]
        try:
            return self.names[node.id]
        except KeyError:
            raise ExpressionError(f"unknown field/name {node.id!r}")

    def visit_List(self, node: ast.List) -> list:
        return [self.visit(e) for e in node.elts]

    def visit_Tuple(self, node: ast.Tuple) -> tuple:
        return tuple(self.visit(e) for e in node.elts)

    def visit_Set(self, node: ast.Set) -> set:
        return {self.visit(e) for e in node.elts}

    def visit_Dict(self, node: ast.Dict) -> dict:
        return {self.visit(k): self.visit(v) for k, v in zip(node.keys, node.values)}

    def visit_BinOp(self, node: ast.BinOp) -> Any:
        op = _SAFE_BINOPS.get(type(node.op))
        if op is None:
            raise ExpressionError(f"operator {type(node.op).__name__} is not allowed")
        return op(self.visit(node.left), self.visit(node.right))

    def visit_UnaryOp(self, node: ast.UnaryOp) -> Any:
        op = _SAFE_UNARY.get(type(node.op))
        if op is None:
            raise ExpressionError(f"unary operator {type(node.op).__name__} is not allowed")
        return op(self.visit(node.operand))

    def visit_BoolOp(self, node: ast.BoolOp) -> bool:
        values = [self.visit(v) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        return any(values)

    def visit_Compare(self, node: ast.Compare) -> bool:
        left = self.visit(node.left)
        for op_node, comparator in zip(node.ops, node.comparators):
            right = self.visit(comparator)
            cmp = _SAFE_CMPOPS.get(type(op_node))
            if cmp is None:
                raise ExpressionError(
                    f"comparison {type(op_node).__name__} is not allowed"
                )
            try:
                ok = cmp(left, right)
            except TypeError as exc:
                raise ExpressionError(f"type mismatch in comparison: {exc}")
            if not ok:
                return False
            left = right
        return True

    def visit_Call(self, node: ast.Call) -> Any:
        if not isinstance(node.func, ast.Name):
            raise ExpressionError("only whitelisted function calls are allowed")
        fn = _SAFE_BUILTINS.get(node.func.id)
        if fn is None:
            raise ExpressionError(f"function {node.func.id!r} is not allowed")
        args = [self.visit(a) for a in node.args]
        kwargs = {kw.arg: self.visit(kw.value) for kw in node.keywords}
        for value in [*args, *kwargs.values()]:
            if callable(value) or (
                not isinstance(value, (str, int, float, bool, type(None)))
                and not isinstance(value, _SAFE_CONTAINERS)
            ):
                raise ExpressionError("unsafe call argument")
        return fn(*args, **kwargs)

    def visit_IfExp(self, node: ast.IfExp) -> Any:
        return self.visit(node.body) if self.visit(node.test) else self.visit(node.orelse)

    def generic_visit(self, node: ast.AST) -> Any:  # blocks attributes, lambdas, ...
        raise ExpressionError(f"syntax {type(node).__name__} is not allowed")


def validate_expression(expr: str) -> None:
    """Static validation at rule-publish time so bad rules fail fast.

    Names that are not whitelisted functions are treated as record field
    references (their existence is checked at evaluation time). This rejects
    free-standing builtins such as ``open`` / ``eval`` / ``__import__`` while
    accepting arbitrary column names without needing a schema up front.
    """
    if not expr or not expr.strip():
        raise ExpressionError("expression is empty")
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"syntax error: {exc.msg}")
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            raise ExpressionError("attribute access is not allowed")
        if isinstance(node, (ast.Lambda, ast.FunctionDef, ast.AsyncFunctionDef,
                             ast.Import, ast.ImportFrom, ast.ClassDef, ast.GeneratorExp,
                             ast.ListComp, ast.SetComp, ast.DictComp, ast.Await,
                             ast.Yield, ast.YieldFrom, ast.Global, ast.Nonlocal)):
            raise ExpressionError(f"syntax {type(node).__name__} is not allowed")
        if isinstance(node, ast.Name):
            if node.id.startswith("_"):
                raise ExpressionError(f"name {node.id!r} is not allowed")
            if isinstance(node.ctx, ast.Load) and node.id not in _SAFE_BUILTINS:
                # field reference — allowed, but must not shadow a dangerous
                # Python builtin that a writer might expect to work
                if hasattr(builtins, node.id) and node.id not in _SAFE_BUILTINS:
                    raise ExpressionError(
                        f"name {node.id!r} is not allowed (use record fields or whitelisted functions)"
                    )


def evaluate(expr: str, row: dict[str, Any]) -> bool:
    """Return True when the row passes the expression."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ExpressionError(f"syntax error: {exc.msg}")
    result = _Evaluator(dict(row)).visit(tree)
    return bool(result)
