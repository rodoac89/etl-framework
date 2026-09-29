"""Analizador estático de scripts PySpark.

Usa el AST de Python. No importa pyspark ni ejecuta el script: revisa la
forma del código (lecturas, joins por sentencia, escrituras, cache y el
SQL embebido en spark.sql).
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

from etl_framework.config import Config
from etl_framework.sql_analyzer import HiveModel, analyze_sql


_READ_METHODS = {"table", "parquet", "orc", "csv", "json", "text", "load", "jdbc"}
_DF_METHODS = _READ_METHODS | {
    "filter",
    "where",
    "select",
    "selectExpr",
    "join",
    "crossJoin",
    "groupBy",
    "agg",
    "withColumn",
    "withColumnRenamed",
    "cache",
    "persist",
    "alias",
    "drop",
    "dropDuplicates",
    "distinct",
    "repartition",
    "coalesce",
    "union",
    "unionByName",
    "dropna",
    "fillna",
    "limit",
    "sample",
    "toDF",
    "hint",
    "subtract",
    "intersect",
    "sql",
    "createDataFrame",
}
_WRITE_METHODS = {"save", "saveAsTable", "insertInto", "parquet", "orc", "csv", "json", "text"}
_SQL_HINT = re.compile(r"(?i)\b(select|with|insert|create|from)\b")


@dataclass
class SparkRead:
    line: int
    source: str
    partition_filtered: bool


@dataclass
class SparkJoinStage:
    line: int
    count: int
    cross: bool
    variable: str | None


@dataclass
class SparkWrite:
    line: int
    method: str
    format: str | None
    coalesced: bool
    partitioned: bool
    insert_into: bool


@dataclass
class SparkReuse:
    variable: str
    line: int
    uses: int


@dataclass
class SparkAlias:
    variable: str
    line: int


@dataclass
class SparkAgg:
    line: int


@dataclass
class SparkStar:
    line: int
    qualified: bool


@dataclass
class EmbeddedSql:
    line: int
    sql: str
    partial: bool
    model: HiveModel


@dataclass
class SparkModel:
    reads: list[SparkRead] = field(default_factory=list)
    joins: list[SparkJoinStage] = field(default_factory=list)
    writes: list[SparkWrite] = field(default_factory=list)
    reuses: list[SparkReuse] = field(default_factory=list)
    bad_names: list[SparkAlias] = field(default_factory=list)
    unfiltered_aggs: list[SparkAgg] = field(default_factory=list)
    stars: list[SparkStar] = field(default_factory=list)
    rdd_lines: list[int] = field(default_factory=list)
    embedded: list[EmbeddedSql] = field(default_factory=list)
    dynamic_sql_lines: list[int] = field(default_factory=list)
    has_shuffle_work: bool = False
    has_aqe: bool = False
    has_shuffle_partitions: bool = False
    has_broadcast: bool = False
    syntax_error: str | None = None
    syntax_line: int | None = None


def analyze_spark(source: str, config: Config) -> SparkModel:
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return SparkModel(syntax_error=exc.msg or "error de sintaxis", syntax_line=exc.lineno or 1)
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node  # type: ignore[attr-defined]
    visitor = _SparkVisitor(source, config)
    visitor.visit(tree)
    return visitor.model


def _call_attr(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _const_str(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


def _config_value(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Constant):
        return None
    if isinstance(node.value, bool):
        return "true" if node.value else "false"
    if isinstance(node.value, (int, float)) and node.value in (0, 1):
        return "true" if node.value == 1 else "false"
    if isinstance(node.value, str):
        return node.value
    return None


def _chain(node: ast.AST) -> tuple[list[str], list[ast.Call]]:
    methods: list[str] = []
    calls: list[ast.Call] = []
    cur = node
    while True:
        if isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
            methods.append(cur.func.attr)
            calls.append(cur)
            cur = cur.func.value
            continue
        if isinstance(cur, ast.Attribute):
            methods.append(cur.attr)
            cur = cur.value
            continue
        break
    return methods, calls


def _down_has(node: ast.AST, names: set[str]) -> bool:
    methods, _ = _chain(node)
    return any(name in methods for name in names)


def _is_read_call(node: ast.Call) -> bool:
    attr = _call_attr(node)
    if attr not in _READ_METHODS:
        return False
    methods, _ = _chain(node)
    if attr == "table":
        return True
    return "read" in methods


def _outer_calls(node: ast.Call) -> list[ast.Call]:
    calls: list[ast.Call] = []
    cur: ast.AST = node
    while True:
        parent = getattr(cur, "parent", None)
        if not isinstance(parent, ast.Attribute):
            break
        grand = getattr(parent, "parent", None)
        if not isinstance(grand, ast.Call) or grand.func is not parent:
            break
        calls.append(grand)
        cur = grand
    return calls


def _root_name(node: ast.AST) -> str | None:
    cur = node
    while isinstance(cur, (ast.Call, ast.Attribute, ast.Subscript)):
        if isinstance(cur, ast.Call):
            cur = cur.func
        else:
            cur = cur.value
    if isinstance(cur, ast.Name):
        return cur.id
    return None


def _partition_pattern(columns: set[str]) -> re.Pattern[str]:
    ordered = sorted(columns, key=len, reverse=True)
    inner = "|".join(re.escape(col) for col in ordered)
    return re.compile(rf"(?i)(?<![\w.])(?:{inner})(?!\s*\()")


def _mentions_partition(node: ast.AST, columns: set[str], pattern: re.Pattern[str]) -> bool:
    for child in ast.walk(node):
        if isinstance(child, ast.Constant) and isinstance(child.value, str):
            if pattern.search(child.value):
                return True
        if isinstance(child, ast.Attribute) and child.attr.lower() in columns:
            parent = getattr(child, "parent", None)
            if isinstance(parent, ast.Call) and parent.func is child:
                continue
            return True
    return False


def _first_source(call: ast.Call) -> str:
    for arg in call.args:
        text = _const_str(arg)
        if text:
            return text
    return "<fuente>"


def _star_kind(arg: ast.AST) -> str | None:
    """'bare', 'qualified' o None."""
    text = _const_str(arg)
    if text is None and isinstance(arg, ast.Call):
        attr = _call_attr(arg)
        func_name = arg.func.id if isinstance(arg.func, ast.Name) else None
        if attr in {"col", "column"} or func_name in {"col", "column"}:
            if arg.args:
                return _star_kind(arg.args[0])
    if text is None:
        return None
    stripped = text.strip()
    if re.fullmatch(r"(?i)\*", stripped):
        return "bare"
    if re.fullmatch(r"(?i)[\w.]+\.\*", stripped):
        return "qualified"
    return None


def _join_is_cross(call: ast.Call) -> bool:
    if _call_attr(call) == "crossJoin":
        return True
    for kw in call.keywords:
        if kw.arg == "how":
            text = _const_str(kw.value)
            if text and text.lower() == "cross":
                return True
    for arg in call.args[1:]:
        text = _const_str(arg)
        if text and text.lower() == "cross":
            return True
    return False


def _assigned_names(node: ast.AST) -> list[ast.Name]:
    names: list[ast.Name] = []
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name):
                names.append(target)
    elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        names.append(node.target)
    return names


def _expr_is_filtered(node: ast.AST, env: dict[str, dict]) -> bool:
    if isinstance(node, ast.Name):
        record = env.get(node.id)
        if record is None:
            return True
        return bool(record.get("filtered"))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        attr = node.func.attr
        if attr in {"filter", "where"}:
            return True
        if attr in _READ_METHODS:
            methods, _ = _chain(node)
            return "filter" in methods or "where" in methods
        if attr == "join":
            left = _expr_is_filtered(node.func.value, env)
            right = any(_expr_is_filtered(arg, env) for arg in node.args[:1])
            return left or right
        return _expr_is_filtered(node.func.value, env)
    if isinstance(node, ast.Attribute):
        return _expr_is_filtered(node.value, env)
    if isinstance(node, ast.Await):
        return _expr_is_filtered(node.value, env)
    return False


class _Frame:
    def __init__(self) -> None:
        self.vars: dict[str, dict] = {}


class _SparkVisitor(ast.NodeVisitor):
    def __init__(self, source: str, config: Config) -> None:
        self.source = source
        self.config = config
        self.model = SparkModel()
        self.frames: list[_Frame] = []
        self._pattern = _partition_pattern(config.partition_set())
        self._seen_aqe_false = False
        self._seen_aqe_true = False

    @property
    def env(self) -> dict[str, dict]:
        merged: dict[str, dict] = {}
        for frame in self.frames:
            merged.update(frame.vars)
        return merged

    def visit_Module(self, node: ast.Module) -> None:
        self._scan_configs(node)
        self.frames.append(_Frame())
        for stmt in node.body:
            self.visit(stmt)
        self._finish_frame()
        self.frames.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        self._visit_function(node)

    def _visit_function(self, node: ast.AST) -> None:
        self.frames.append(_Frame())
        for stmt in node.body:  # type: ignore[attr-defined]
            self.visit(stmt)
        self._finish_frame()
        self.frames.pop()

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        for stmt in node.body:
            self.visit(stmt)

    def visit_Assign(self, node: ast.Assign) -> None:
        self._check_joins(node)
        self.visit(node.value)
        self._record_assignment(node, node.value)

    def visit_AnnAssign(self, node: ast.AnnAssign) -> None:
        if node.value is not None:
            self._check_joins(node)
            self.visit(node.value)
            self._record_assignment(node, node.value)

    def visit_Expr(self, node: ast.Expr) -> None:
        self._check_joins(node)
        self.visit(node.value)

    def visit_Return(self, node: ast.Return) -> None:
        if node.value is not None:
            self._check_joins(node)
            self.visit(node.value)

    def visit_Call(self, node: ast.Call) -> None:
        self._check_read(node)
        self._check_write(node)
        self._check_star(node)
        self._check_agg(node)
        self._check_cache_call(node)
        self._check_sql(node)
        self._check_broadcast(node)
        self.generic_visit(node)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        if node.attr == "rdd":
            self.model.rdd_lines.append(node.lineno)
        self.generic_visit(node)

    def visit_Name(self, node: ast.Name) -> None:
        if isinstance(node.ctx, ast.Load):
            for frame in reversed(self.frames):
                if node.id in frame.vars:
                    frame.vars[node.id]["loads"] += 1
                    break

    def _record_assignment(self, node: ast.AST, value: ast.AST) -> None:
        if not self.frames or not _down_has(value, _DF_METHODS):
            return
        frame = self.frames[-1]
        filtered = _expr_is_filtered(value, self.env)
        cached = _down_has(value, {"cache", "persist"})
        coalesced = _down_has(value, {"coalesce", "repartition"})
        has_join = _down_has(value, {"join", "crossJoin"})
        for name in _assigned_names(node):
            record = frame.vars.get(name.id)
            if record is None:
                record = {
                    "loads": 0,
                    "cached": False,
                    "filtered": False,
                    "coalesced": False,
                    "line": name.lineno,
                }
                frame.vars[name.id] = record
            record["line"] = name.lineno
            record["filtered"] = filtered
            record["coalesced"] = bool(record["coalesced"] or coalesced)
            record["cached"] = bool(record["cached"] or cached)
            if has_join and not self._prefixed(name.id):
                self.model.bad_names.append(SparkAlias(name.id, name.lineno))

    def _prefixed(self, name: str) -> bool:
        lowered = name.lower()
        return any(lowered.startswith(prefix.lower()) for prefix in self.config.spark_prefixes)

    def _finish_frame(self) -> None:
        frame = self.frames[-1]
        for name, record in frame.vars.items():
            if record["loads"] >= 2 and not record["cached"]:
                self.model.reuses.append(
                    SparkReuse(variable=name, line=record["line"], uses=record["loads"])
                )

    def _check_joins(self, node: ast.AST) -> None:
        joins = [
            child
            for child in ast.walk(node)
            if isinstance(child, ast.Call) and _call_attr(child) in {"join", "crossJoin"}
        ]
        if not joins:
            return
        self.model.has_shuffle_work = True
        variable = None
        if isinstance(node, (ast.Assign, ast.AnnAssign)):
            names = _assigned_names(node)
            variable = names[0].id if names else None
        self.model.joins.append(
            SparkJoinStage(
                line=joins[0].lineno,
                count=len(joins),
                cross=any(_join_is_cross(call) for call in joins),
                variable=variable,
            )
        )

    def _check_read(self, node: ast.Call) -> None:
        if not _is_read_call(node):
            return
        filtered = False
        for outer in _outer_calls(node):
            if _call_attr(outer) in {"filter", "where"} and _mentions_partition(
                outer, self.config.partition_set(), self._pattern
            ):
                filtered = True
                break
        self.model.reads.append(
            SparkRead(line=node.lineno, source=_first_source(node), partition_filtered=filtered)
        )

    def _check_write(self, node: ast.Call) -> None:
        attr = _call_attr(node)
        if attr not in _WRITE_METHODS:
            return
        methods, calls = _chain(node)
        if "write" not in methods:
            return
        fmt = None
        if attr in {"parquet", "orc", "csv", "json", "text"}:
            fmt = attr
        for call in calls:
            if _call_attr(call) == "format" and call.args:
                text = _const_str(call.args[0])
                if text:
                    fmt = text.lower()
        coalesced = "coalesce" in methods or "repartition" in methods
        if not coalesced:
            root = _root_name(node)
            record = self.env.get(root) if root else None
            if record and record.get("coalesced"):
                coalesced = True
        self.model.writes.append(
            SparkWrite(
                line=node.lineno,
                method=attr or "",
                format=fmt,
                coalesced=coalesced,
                partitioned="partitionBy" in methods,
                insert_into=attr == "insertInto",
            )
        )

    def _check_star(self, node: ast.Call) -> None:
        attr = _call_attr(node)
        if attr not in {"select", "selectExpr"}:
            return
        for arg in node.args:
            kind = _star_kind(arg)
            if kind:
                self.model.stars.append(SparkStar(line=node.lineno, qualified=kind == "qualified"))

    def _check_agg(self, node: ast.Call) -> None:
        attr = _call_attr(node)
        if attr not in {"groupBy", "agg"}:
            return
        self.model.has_shuffle_work = True
        methods, _ = _chain(node)
        if attr == "agg" and "groupBy" in methods:
            return
        receiver = node.func.value if isinstance(node.func, ast.Attribute) else None
        if receiver is not None and not _expr_is_filtered(receiver, self.env):
            self.model.unfiltered_aggs.append(SparkAgg(line=node.lineno))

    def _check_cache_call(self, node: ast.Call) -> None:
        if _call_attr(node) not in {"cache", "persist"}:
            return
        root = _root_name(node.func.value) if isinstance(node.func, ast.Attribute) else None
        if not root:
            return
        for frame in reversed(self.frames):
            if root in frame.vars:
                frame.vars[root]["cached"] = True
                break

    def _check_sql(self, node: ast.Call) -> None:
        if _call_attr(node) != "sql" or not node.args:
            return
        arg = node.args[0]
        partial = False
        sql: str | None = None
        anchor = node.lineno
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            sql = arg.value
            anchor = arg.lineno or node.lineno
        elif isinstance(arg, ast.JoinedStr):
            partial = True
            pieces: list[str] = []
            for value in arg.values:
                if isinstance(value, ast.Constant) and isinstance(value.value, str):
                    pieces.append(value.value)
                else:
                    pieces.append(" ")
            sql = "".join(pieces)
        elif not isinstance(arg, ast.Constant):
            self.model.dynamic_sql_lines.append(node.lineno)
            return
        if not sql or not _SQL_HINT.search(sql):
            return
        if partial:
            self.model.dynamic_sql_lines.append(node.lineno)
        model = analyze_sql(sql, self.config, max_joins=self.config.spark_max_joins)
        self.model.embedded.append(EmbeddedSql(line=anchor, sql=sql, partial=partial, model=model))

    def _check_broadcast(self, node: ast.Call) -> None:
        attr = _call_attr(node)
        if attr == "broadcast" or (isinstance(node.func, ast.Name) and node.func.id == "broadcast"):
            self.model.has_broadcast = True
        if attr == "hint":
            for arg in node.args:
                text = _const_str(arg)
                if text and text.lower() == "broadcast":
                    self.model.has_broadcast = True

    def _scan_configs(self, tree: ast.AST) -> None:
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            attr = _call_attr(node)
            func_name = node.func.id if isinstance(node.func, ast.Name) else None
            if attr not in {"config", "set"} and func_name not in {"config", "set"}:
                continue
            if not node.args:
                continue
            key = _const_str(node.args[0])
            if not key:
                continue
            value = _config_value(node.args[1]) if len(node.args) > 1 else None
            if key == "spark.sql.adaptive.enabled":
                if value is not None and value.lower() in {"false", "0"}:
                    self._seen_aqe_false = True
                else:
                    self._seen_aqe_true = True
            if key == "spark.sql.shuffle.partitions":
                self.model.has_shuffle_partitions = True
        self.model.has_aqe = self._seen_aqe_true and not self._seen_aqe_false
