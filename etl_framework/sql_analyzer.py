"""Analizador estático de HiveQL.

No ejecuta el SQL: tokeniza el script, separa cada SELECT (incluidas las CTE)
y deja un modelo que las reglas interpretan. Cubre el subconjunto que usan
las guías y los scripts de reingeniería (placeholders {param}, WiTH/where
en cualquier capitalización, INSERT OVERWRITE sin la palabra TABLE).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from etl_framework.config import Config


_MERGE_RE = re.compile(
    r"(?i)hive\.merge\.(mapfiles|mapredfiles|tezfiles|sparkfiles|smallfiles)"
)

_AGG_FUNCS = {
    "SUM",
    "COUNT",
    "AVG",
    "MIN",
    "MAX",
    "COLLECT_LIST",
    "COLLECT_SET",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "APPROX_COUNT_DISTINCT",
}

_NOT_A_TABLE = {
    "SELECT",
    "WITH",
    "WHERE",
    "ON",
    "JOIN",
    "LEFT",
    "RIGHT",
    "FULL",
    "INNER",
    "OUTER",
    "CROSS",
    "NATURAL",
    "SEMI",
    "ANTI",
    "GROUP",
    "ORDER",
    "HAVING",
    "LIMIT",
    "UNION",
    "LATERAL",
    "VIEW",
    "TABLESAMPLE",
    "VALUES",
    "SET",
    "PARTITION",
    "CLUSTER",
    "DISTRIBUTE",
    "SORT",
    "WINDOW",
    "QUALIFY",
    "AS",
    "AND",
    "OR",
    "USING",
    "FROM",
}

_JOIN_MODIFIERS = {
    "LEFT",
    "RIGHT",
    "FULL",
    "INNER",
    "OUTER",
    "NATURAL",
    "SEMI",
    "ANTI",
    "CROSS",
}

_CLAUSE_BREAKS = {
    "WHERE",
    "GROUP",
    "ORDER",
    "HAVING",
    "LIMIT",
    "UNION",
    "JOIN",
    "WINDOW",
    "QUALIFY",
    "CLUSTER",
    "DISTRIBUTE",
    "SORT",
    "SELECT",
}


@dataclass
class Token:
    kind: str
    text: str
    line: int


@dataclass
class TableRef:
    name: str
    base: str
    line: int
    role: str = "from"
    external: bool = True
    broken_cte: bool = False


@dataclass
class SelectScope:
    line: int
    depth: int
    start: int
    end: int = 0
    join_count: int = 0
    join_lines: list[int] = field(default_factory=list)
    cross_join: bool = False
    cross_line: int = 0
    bare_stars: list[int] = field(default_factory=list)
    qualified_stars: list[int] = field(default_factory=list)
    has_distinct: bool = False
    group_by: bool = False
    has_aggregate: bool = False
    has_where: bool = False
    partition_filtered: bool = False
    saw_from: bool = False
    tables: list[TableRef] = field(default_factory=list)

    @property
    def external_from(self) -> list[TableRef]:
        return [t for t in self.tables if t.role == "from" and t.external]


@dataclass
class TempTable:
    name: str
    base: str
    line: int


@dataclass
class CreateTable:
    name: str
    base: str
    line: int
    temporary: bool
    partitioned: bool
    storage_format: str | None


@dataclass
class InsertStmt:
    target: str
    base: str
    line: int
    has_partition: bool


@dataclass
class HiveModel:
    scopes: list[SelectScope]
    temp_tables: list[TempTable]
    creates: list[CreateTable]
    inserts: list[InsertStmt]
    has_merge_config: bool
    has_activity: bool
    max_joins: int


def analyze_sql(source: str, config: Config, *, max_joins: int | None = None) -> HiveModel:
    tokens = tokenize(source)
    statements = split_statements(tokens)
    known_temps: set[str] = set()
    previous_ctes: set[str] = set()
    scopes: list[SelectScope] = []
    temps: list[TempTable] = []
    creates: list[CreateTable] = []
    inserts: list[InsertStmt] = []
    has_activity = False
    limit = config.hive_max_joins if max_joins is None else max_joins

    for statement in statements:
        if not statement:
            continue
        ctes = extract_ctes(statement)
        found = walk_scopes(statement, ctes, known_temps, config.partition_set())
        for scope in found:
            for ref in scope.tables:
                if ref.external and ref.base.lower() in previous_ctes:
                    # Un ';' cortó el WITH en el statement anterior: el nombre
                    # ya no es una CTE y tampoco es una tabla de origen.
                    ref.broken_cte = True
                    ref.external = False
        previous_ctes = {name.lower() for name in ctes}
        scopes.extend(found)
        for created in parse_creates(statement):
            creates.append(created)
            has_activity = True
            if created.temporary:
                temps.append(TempTable(created.name, created.base, created.line))
                known_temps.add(created.base.lower())
                known_temps.add(created.name.lower())
        statement_inserts = parse_inserts(statement)
        if statement_inserts:
            has_activity = True
            inserts.extend(statement_inserts)
        if found:
            has_activity = True

    return HiveModel(
        scopes=scopes,
        temp_tables=temps,
        creates=creates,
        inserts=inserts,
        has_merge_config=bool(_MERGE_RE.search(source)),
        has_activity=has_activity,
        max_joins=limit,
    )


def tokenize(source: str) -> list[Token]:
    tokens: list[Token] = []
    i = 0
    n = len(source)
    line = 1
    while i < n:
        ch = source[i]
        if ch == "\n":
            line += 1
            i += 1
            continue
        if ch.isspace():
            i += 1
            continue
        if ch == "-" and i + 1 < n and source[i + 1] == "-":
            i += 2
            while i < n and source[i] != "\n":
                i += 1
            continue
        if ch == "/" and i + 1 < n and source[i + 1] == "*":
            i += 2
            while i < n:
                if source[i] == "\n":
                    line += 1
                    i += 1
                    continue
                if source[i] == "*" and i + 1 < n and source[i + 1] == "/":
                    i += 2
                    break
                i += 1
            continue
        if ch in {"'", '"'}:
            quote = ch
            i += 1
            while i < n:
                if source[i] == "\n":
                    line += 1
                    i += 1
                    continue
                if source[i] == "\\" and i + 1 < n:
                    i += 2
                    continue
                if source[i] == quote:
                    if i + 1 < n and source[i + 1] == quote:
                        i += 2
                        continue
                    i += 1
                    break
                i += 1
            tokens.append(Token("string", quote, line))
            continue
        if ch == "`":
            start_line = line
            i += 1
            buf = []
            while i < n:
                if source[i] == "\n":
                    line += 1
                    buf.append("\n")
                    i += 1
                    continue
                if source[i] == "`" and i + 1 < n and source[i + 1] == "`":
                    buf.append("`")
                    i += 2
                    continue
                if source[i] == "`":
                    i += 1
                    break
                buf.append(source[i])
                i += 1
            tokens.append(Token("quoted", "".join(buf), start_line))
            continue
        if ch == "{":
            start_line = line
            j = i + 1
            while j < n and source[j] != "}" and source[j] != "\n":
                j += 1
            if j < n and source[j] == "}":
                tokens.append(Token("placeholder", source[i : j + 1], start_line))
                i = j + 1
                continue
        if ch.isalpha() or ch == "_":
            start_line = line
            j = i + 1
            while j < n and (source[j].isalnum() or source[j] == "_"):
                j += 1
            tokens.append(Token("word", source[i:j], start_line))
            i = j
            continue
        if ch.isdigit():
            start_line = line
            j = i + 1
            while j < n and source[j].isdigit():
                j += 1
            if j < n and source[j] == "." and j + 1 < n and source[j + 1].isdigit():
                j += 1
                while j < n and source[j].isdigit():
                    j += 1
            tokens.append(Token("number", source[i:j], start_line))
            i = j
            continue
        if ch in {"<", ">", "!"} and i + 1 < n and source[i + 1] == "=":
            tokens.append(Token("symbol", ch + "=", line))
            i += 2
            continue
        if ch == "<" and i + 1 < n and source[i + 1] == ">":
            tokens.append(Token("symbol", "<>", line))
            i += 2
            continue
        tokens.append(Token("symbol", ch, line))
        i += 1
    return tokens


def split_statements(tokens: list[Token]) -> list[list[Token]]:
    statements: list[list[Token]] = []
    current: list[Token] = []
    for token in tokens:
        if token.text == ";":
            if current:
                statements.append(current)
                current = []
            continue
        current.append(token)
    if current:
        statements.append(current)
    return statements


def strip_ident(text: str) -> str:
    return text.strip("`").strip('"')


def word(token: Token | None) -> str | None:
    if token is None or token.kind != "word":
        return None
    return token.text.upper()


def skip_paren(tokens: list[Token], index: int) -> int:
    depth = 0
    i = index
    while i < len(tokens):
        if tokens[i].text == "(":
            depth += 1
        elif tokens[i].text == ")":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    return i


def extract_ctes(tokens: list[Token]) -> set[str]:
    names: set[str] = set()
    i = 0
    n = len(tokens)
    while i < n:
        if word(tokens[i]) != "WITH":
            i += 1
            continue
        i += 1
        while i < n:
            if word(tokens[i]) == "RECURSIVE":
                i += 1
                continue
            if tokens[i].kind not in {"word", "quoted"}:
                break
            names.add(strip_ident(tokens[i].text))
            i += 1
            while i < n and word(tokens[i]) != "AS":
                if tokens[i].text == "(":
                    i = skip_paren(tokens, i)
                    continue
                # Un SELECT aquí indica que el WITH ya terminó (SQL mal formado
                # o la palabra WITH usada en otro contexto).
                if word(tokens[i]) == "SELECT":
                    return names
                i += 1
            if i >= n:
                break
            i += 1
            while i < n and tokens[i].text != "(":
                if word(tokens[i]) == "SELECT":
                    return names
                i += 1
            if i >= n:
                break
            i = skip_paren(tokens, i)
            if i < n and tokens[i].text == ",":
                i += 1
                continue
            break
    return names


def read_table(tokens: list[Token], index: int) -> TableRef | None:
    if index >= len(tokens):
        return None
    token = tokens[index]
    if token.text == "(":
        return None
    if token.kind == "word" and token.text.upper() in _NOT_A_TABLE:
        return None
    if token.kind not in {"word", "quoted", "placeholder"}:
        return None
    parts = [strip_ident(token.text)]
    line = token.line
    j = index + 1
    while (
        j + 1 < len(tokens)
        and tokens[j].text == "."
        and tokens[j + 1].kind in {"word", "quoted", "placeholder"}
    ):
        parts.append(strip_ident(tokens[j + 1].text))
        j += 2
    return TableRef(name=".".join(parts), base=parts[-1], line=line)


def _comma_tables(tokens: list[Token], start: int, base_depth: int) -> list[TableRef]:
    extras: list[TableRef] = []
    depth = base_depth
    j = start
    while j < len(tokens):
        token = tokens[j]
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
            if depth < base_depth:
                break
        elif depth == base_depth and word(token) in _CLAUSE_BREAKS:
            break
        elif depth == base_depth and token.text == ",":
            ref = read_table(tokens, j + 1)
            if ref is not None:
                ref.role = "join"
                extras.append(ref)
        j += 1
    return extras


def _is_cross(tokens: list[Token], join_index: int) -> bool:
    j = join_index - 1
    while j >= 0 and word(tokens[j]) in _JOIN_MODIFIERS:
        if word(tokens[j]) == "CROSS":
            return True
        j -= 1
    return False


def _is_multiplication(tokens: list[Token], index: int) -> bool:
    prev = tokens[index - 1] if index > 0 else None
    nxt = tokens[index + 1] if index + 1 < len(tokens) else None
    if prev is None or nxt is None:
        return False
    if prev.text in {",", "(", "*"}:
        return False
    if word(prev) in {"SELECT", "DISTINCT", "ALL", "AS"}:
        return False
    prev_ok = prev.kind in {"word", "number", "quoted", "placeholder"} or prev.text in {")", "]"}
    next_ok = nxt.kind in {"word", "number", "quoted", "placeholder"} or nxt.text in {"(", "["}
    return prev_ok and next_ok


def _is_partition_token(tokens: list[Token], index: int, columns: set[str]) -> bool:
    token = tokens[index]
    if token.kind != "word" or token.text.lower() not in columns:
        return False
    nxt = tokens[index + 1] if index + 1 < len(tokens) else None
    if nxt is not None and nxt.text == "(":
        return False
    return True


def walk_scopes(
    tokens: list[Token],
    ctes: set[str],
    known_temps: set[str],
    partition_columns: set[str],
) -> list[SelectScope]:
    scopes: list[SelectScope] = []
    stack: list[SelectScope] = []
    depth = 0
    i = 0
    n = len(tokens)
    while i < n:
        token = tokens[i]
        if token.text == "(":
            depth += 1
            i += 1
            continue
        if token.text == ")":
            depth = max(0, depth - 1)
            while stack and stack[-1].depth > depth:
                stack[-1].end = i
                stack.pop()
            i += 1
            continue
        upper = word(token)
        if upper == "SELECT":
            scope = SelectScope(line=token.line, depth=depth, start=i)
            if i + 1 < n and word(tokens[i + 1]) == "DISTINCT":
                scope.has_distinct = True
            stack.append(scope)
            scopes.append(scope)
            i += 1
            continue
        if upper == "UNION" and stack and depth == stack[-1].depth:
            stack[-1].end = i
            stack.pop()
            i += 1
            continue
        if stack and depth == stack[-1].depth and upper == "FROM":
            scope = stack[-1]
            scope.saw_from = True
            ref = read_table(tokens, i + 1)
            if ref is not None:
                ref.role = "from"
                scope.tables.append(ref)
            for extra in _comma_tables(tokens, i + 1, depth):
                scope.join_count += 1
                scope.join_lines.append(extra.line)
                scope.tables.append(extra)
            i += 1
            continue
        if stack and depth == stack[-1].depth and upper == "JOIN":
            scope = stack[-1]
            scope.saw_from = True
            scope.join_count += 1
            scope.join_lines.append(token.line)
            if _is_cross(tokens, i):
                scope.cross_join = True
                scope.cross_line = token.line
            ref = read_table(tokens, i + 1)
            if ref is not None:
                ref.role = "join"
                scope.tables.append(ref)
            i += 1
            continue
        if stack and upper == "DISTINCT":
            stack[-1].has_distinct = True
        if stack and upper in _AGG_FUNCS and i + 1 < n and tokens[i + 1].text == "(":
            stack[-1].has_aggregate = True
        if (
            stack
            and depth == stack[-1].depth
            and upper == "GROUP"
            and i + 1 < n
            and word(tokens[i + 1]) == "BY"
        ):
            stack[-1].group_by = True
        if stack and depth == stack[-1].depth and upper == "WHERE":
            stack[-1].has_where = True
        if token.text == "*" and stack and stack[-1].depth == depth and not stack[-1].saw_from:
            if i > 0 and tokens[i - 1].text == ".":
                stack[-1].qualified_stars.append(token.line)
            elif not _is_multiplication(tokens, i):
                stack[-1].bare_stars.append(token.line)
        i += 1
    while stack:
        stack[-1].end = n
        stack.pop()

    local = {name.lower() for name in ctes} | {name.lower() for name in known_temps}
    for scope in scopes:
        _mark_predicates(tokens, scope, partition_columns)
        for ref in scope.tables:
            key_base = ref.base.lower()
            key_full = ref.name.lower()
            ref.external = key_base not in local and key_full not in local
    return scopes


def _mark_predicates(tokens: list[Token], scope: SelectScope, columns: set[str]) -> None:
    depth = scope.depth
    mode: str | None = None
    i = scope.start
    while i < scope.end:
        token = tokens[i]
        if token.text == "(":
            depth += 1
        elif token.text == ")":
            depth -= 1
        else:
            upper = word(token)
            at_level = depth == scope.depth
            if at_level and upper == "WHERE":
                mode = "where"
                scope.has_where = True
            elif at_level and upper == "ON":
                mode = "on"
            elif at_level and upper == "JOIN" and mode == "on":
                mode = None
            elif at_level and upper in {"GROUP", "ORDER", "HAVING", "LIMIT", "WINDOW", "UNION", "QUALIFY"}:
                mode = None
            elif at_level and upper == "SELECT" and i != scope.start:
                mode = None
            elif mode and _is_partition_token(tokens, i, columns):
                scope.partition_filtered = True
        i += 1


def parse_creates(tokens: list[Token]) -> list[CreateTable]:
    found: list[CreateTable] = []
    depth = 0
    i = 0
    while i < len(tokens):
        if tokens[i].text == "(":
            depth += 1
        elif tokens[i].text == ")":
            depth = max(0, depth - 1)
        elif depth == 0 and word(tokens[i]) == "CREATE":
            header = _create_header(tokens, i)
            if header is not None:
                temporary, table_at = header
                name_at = _skip_if_not_exists(tokens, table_at + 1)
                ref = read_table(tokens, name_at)
                if ref is not None:
                    storage, partitioned = _table_options(tokens)
                    found.append(
                        CreateTable(
                            name=ref.name,
                            base=ref.base,
                            line=tokens[i].line,
                            temporary=temporary,
                            partitioned=partitioned,
                            storage_format=storage,
                        )
                    )
        i += 1
    return found


def _create_header(tokens: list[Token], create_at: int) -> tuple[bool, int] | None:
    temporary = False
    j = create_at + 1
    limit = min(len(tokens), create_at + 8)
    while j < limit:
        upper = word(tokens[j])
        if upper in {"TEMP", "TEMPORARY"}:
            temporary = True
            j += 1
            continue
        if upper in {"EXTERNAL", "MANAGED", "REMOTE", "OR", "REPLACE", "TRANSACTIONAL"}:
            j += 1
            continue
        if upper == "TABLE":
            return temporary, j
        return None
    return None


def _skip_if_not_exists(tokens: list[Token], index: int) -> int:
    if (
        index + 2 < len(tokens)
        and word(tokens[index]) == "IF"
        and word(tokens[index + 1]) == "NOT"
        and word(tokens[index + 2]) == "EXISTS"
    ):
        return index + 3
    return index


def _table_options(tokens: list[Token]) -> tuple[str | None, bool]:
    storage: str | None = None
    partitioned = False
    depth = 0
    i = 0
    while i < len(tokens):
        if tokens[i].text == "(":
            depth += 1
        elif tokens[i].text == ")":
            depth -= 1
        elif depth == 0 and word(tokens[i]) == "PARTITIONED" and i + 1 < len(tokens) and word(tokens[i + 1]) == "BY":
            partitioned = True
        elif depth == 0 and word(tokens[i]) == "STORED" and i + 2 < len(tokens) and word(tokens[i + 1]) == "AS":
            fmt = word(tokens[i + 2])
            if fmt:
                storage = fmt
        elif depth == 0 and word(tokens[i]) == "USING" and i + 1 < len(tokens):
            nxt = tokens[i + 1]
            if nxt.kind in {"word", "string", "quoted"}:
                storage = strip_ident(nxt.text).strip("'").upper()
        i += 1
    return storage, partitioned


def parse_inserts(tokens: list[Token]) -> list[InsertStmt]:
    found: list[InsertStmt] = []
    depth = 0
    i = 0
    skip = {"INTO", "OVERWRITE", "TABLE", "IF", "NOT", "EXISTS", "PARTITION", "SELECT"}
    while i < len(tokens):
        if tokens[i].text == "(":
            depth += 1
            i += 1
            continue
        if tokens[i].text == ")":
            depth = max(0, depth - 1)
            i += 1
            continue
        if depth != 0 or word(tokens[i]) != "INSERT":
            i += 1
            continue
        has_partition = False
        target: TableRef | None = None
        nested = 0
        j = i + 1
        while j < len(tokens):
            if tokens[j].text == "(":
                nested += 1
            elif tokens[j].text == ")":
                nested -= 1
            elif nested == 0 and word(tokens[j]) in {"SELECT", "VALUES"}:
                break
            elif nested == 0 and word(tokens[j]) == "PARTITION":
                has_partition = True
            elif nested == 0 and word(tokens[j]) == "TABLE" and j + 1 < len(tokens):
                target = read_table(tokens, j + 1) or target
            elif (
                nested == 0
                and target is None
                and tokens[j].kind in {"word", "quoted", "placeholder"}
                and (word(tokens[j]) or "") not in skip
            ):
                target = read_table(tokens, j)
            j += 1
        if target is not None:
            found.append(
                InsertStmt(
                    target=target.name,
                    base=target.base,
                    line=tokens[i].line,
                    has_partition=has_partition,
                )
            )
        i = max(j, i + 1)
    return found
