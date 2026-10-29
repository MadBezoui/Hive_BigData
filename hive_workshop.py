#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mini-Hive pédagogique : le workshop « Manipulation et analyse de données avec Hive »
(CESI · Big Data · FISA A4) rejoué sans Hadoop, avec Python seul.

  * HDFS est simulé par un vrai dossier (mini_hdfs/hdfs/...) : on voit les répertoires
    de partitions et les fichiers de buckets dans l'explorateur de fichiers.
  * Le Metastore est un fichier JSON (mini_hdfs/metastore.json).
  * Les requêtes HiveQL sont traduites puis exécutées par SQLite (bibliothèque standard),
    avec les règles de lecture de Hive : colonnes manquantes à NULL, CAST qui renvoie
    NULL au lieu d'échouer, en-têtes ignorées, partitions, buckets (hash % N)...

Aucune dépendance : Python 3.8 ou plus récent.

Utilisation :
  python3 hive_workshop.py                 # interface web (http://localhost:8765)
  python3 hive_workshop.py serve --sans-corriges   # version élèves (corrections masquées)
  python3 hive_workshop.py rejouer         # rejoue tout le sujet dans le terminal
  python3 hive_workshop.py console         # invite « hive> » interactive
  python3 hive_workshop.py verifier        # vérifie les résultats attendus
  python3 hive_workshop.py reset           # vide le HDFS simulé et le Metastore
Options communes : --donnees DOSSIER (défaut : ./donnees) · --hdfs DOSSIER (défaut : ./mini_hdfs)
"""

import argparse
import datetime as _dt
import inspect
import json
import math
import os
import re
import shlex
import shutil
import sqlite3
import sys
import threading
import time

HOST_URI = "hdfs://quickstart.cloudera:8020"
DEFAULT_CONF = {
    "hive.exec.dynamic.partition": "true",
    "hive.exec.dynamic.partition.mode": "strict",
    "hive.enforce.bucketing": "false",
    "mapred.input.dir.recursive": "false",
    "hive.mapred.supports.subdirectories": "false",
    "hive.cli.print.current.db": "false",
    "hive.cli.print.header": "true",
    "hive.conf.validation": "true",
}


class HiveError(Exception):
    """Erreur renvoyée à l'utilisateur, formulée comme dans Hive."""


# ======================================================================
#  Types et conversions « à la Hive »
# ======================================================================

_INT_RE = re.compile(r"^[+-]?\d+(\.\d*)?$")
_NUM_RE = re.compile(r"^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$")
_DATE_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})")
_INT_RANGES = {"TINYINT": 2 ** 7, "SMALLINT": 2 ** 15, "INT": 2 ** 31, "INTEGER": 2 ** 31, "BIGINT": 2 ** 63}


def base_type(t):
    return re.split(r"[\s(<]", t.strip().upper(), maxsplit=1)[0]


def hive_str(v):
    """Représentation texte d'une valeur, comme Hive l'écrit."""
    if v is None:
        return None
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float):
        if v.is_integer() and abs(v) < 1e16:
            return "%.1f" % v
        return repr(v)
    return str(v)


def hive_cast(v, t):
    """CAST(v AS t) avec la sémantique de Hive : un échec donne NULL, jamais une erreur."""
    if v is None:
        return None
    bt = base_type(t)
    if bt in _INT_RANGES:
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, (int, float)):
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                return None
            r = int(v)
        else:
            s = str(v)
            if not _INT_RE.match(s):   # Hive n'enlève pas les espaces, n'accepte pas « 20000euro »
                return None
            r = int(s.split(".")[0])
        lim = _INT_RANGES[bt]
        return r if -lim <= r < lim else None
    if bt in ("DOUBLE", "FLOAT", "REAL"):
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            return float(v)
        s = str(v)
        return float(s) if _NUM_RE.match(s) else None
    if bt in ("DECIMAL", "NUMERIC"):
        m = re.search(r"\(\s*\d+\s*,\s*(\d+)\s*\)", t)
        scale = int(m.group(1)) if m else 0
        if isinstance(v, (int, float)) and not isinstance(v, bool):
            x = float(v)
        else:
            s = str(v)
            if not _NUM_RE.match(s):
                return None
            x = float(s)
        return round(x, scale)
    if bt == "BOOLEAN":
        if isinstance(v, (int, float)):
            return 1 if v else 0
        s = str(v).lower()
        return 1 if s == "true" else 0 if s == "false" else None
    if bt == "DATE":
        m = _DATE_RE.match(str(v))
        return m.group(0) if m else None
    if bt == "TIMESTAMP":
        s = str(v)
        return s if _DATE_RE.match(s) else None
    return hive_str(v)   # STRING, VARCHAR, CHAR...


def sqlite_affinity(t):
    bt = base_type(t)
    if bt in _INT_RANGES or bt == "BOOLEAN":
        return "INTEGER"
    if bt in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC"):
        return "REAL"
    return "TEXT"


def hive_hash(v, t):
    """Fonction de hachage de Hive 1.x pour le bucketing (ObjectInspectorUtils.hashCode)."""
    if v is None:
        return 0
    bt = base_type(t)
    if bt in ("INT", "INTEGER", "SMALLINT", "TINYINT"):
        return int(v)
    if bt == "BIGINT":
        v = int(v) & 0xFFFFFFFFFFFFFFFF
        h = (v ^ (v >> 32)) & 0xFFFFFFFF
        return h - (1 << 32) if h >= (1 << 31) else h
    if bt == "BOOLEAN":
        return 1 if v else 0
    h = 0
    for b in hive_str(v).encode("utf-8"):
        b = b - 256 if b > 127 else b   # octets signés en Java
        h = (h * 31 + b) & 0xFFFFFFFF
    return h - (1 << 32) if h >= (1 << 31) else h


def bucket_of(v, t, n):
    return (hive_hash(v, t) & 0x7FFFFFFF) % n


def fmt_value(v):
    """Affichage d'une valeur dans un résultat (NULL, nombres lisibles)."""
    if v is None:
        return "NULL"
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return "NaN" if math.isnan(v) else ("Infinity" if v > 0 else "-Infinity")
        if v.is_integer() and abs(v) < 1e16:
            return "%.1f" % v
        s = "%.6f" % v
        return s.rstrip("0").rstrip(".")
    return str(v)


# ======================================================================
#  Fonctions HiveQL ajoutées à SQLite
# ======================================================================

def _java_to_py_repl(rep):
    rep = rep.replace("\\", "\\\\")
    return re.sub(r"\$(\d+)", r"\\g<\1>", rep)


def f_regexp_replace(s, pat, rep):
    if s is None or pat is None or rep is None:
        return None
    return re.sub(pat, _java_to_py_repl(rep), hive_str(s))


def f_regexp_extract(s, pat, idx=1):
    if s is None or pat is None:
        return None
    m = re.search(pat, hive_str(s))
    if not m:
        return ""
    try:
        return m.group(int(idx)) or ""
    except IndexError:
        return None


def f_regexp(pat, s):            # X REGEXP Y / X RLIKE Y
    if s is None or pat is None:
        return None
    return 1 if re.search(pat, hive_str(s)) else 0


def f_substr(s, pos, length=None):
    if s is None or pos is None:
        return None
    s = hive_str(s)
    pos = int(pos)
    if pos > 0:
        start = pos - 1
    elif pos < 0:
        start = max(len(s) + pos, 0)
    else:
        start = 0
    if length is None:
        return s[start:]
    length = int(length)
    return "" if length <= 0 else s[start:start + length]


def _to_date(v):
    if v is None:
        return None
    m = _DATE_RE.match(hive_str(v))
    if not m:
        return None
    try:
        return _dt.date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def f_year(v):
    d = _to_date(v)
    return d.year if d else None


def f_month(v):
    d = _to_date(v)
    return d.month if d else None


def f_day(v):
    d = _to_date(v)
    return d.day if d else None


def f_to_date(v):
    d = _to_date(v)
    return d.isoformat() if d else None


def f_datediff(a, b):
    da, db = _to_date(a), _to_date(b)
    return (da - db).days if da and db else None


def f_date_add(v, n):
    d = _to_date(v)
    return (d + _dt.timedelta(days=int(n))).isoformat() if d and n is not None else None


def f_unix_timestamp(*a):
    if not a:
        return int(time.time())
    if a[0] is None:
        return None
    try:
        return int(_dt.datetime.strptime(hive_str(a[0])[:19], "%Y-%m-%d %H:%M:%S").timestamp())
    except ValueError:
        return None


def f_from_unixtime(n, fmt=None):
    if n is None:
        return None
    return _dt.datetime.fromtimestamp(int(n)).strftime("%Y-%m-%d %H:%M:%S")


def f_concat(*a):
    if any(x is None for x in a):
        return None
    return "".join(hive_str(x) for x in a)


def f_concat_ws(sep, *a):
    if sep is None:
        return None
    return hive_str(sep).join(hive_str(x) for x in a if x is not None)


def f_nvl(a, b):
    return b if a is None else a


def _num(f):
    def g(x, *rest):
        if x is None:
            return None
        try:
            return f(float(x), *[float(r) for r in rest])
        except (ValueError, TypeError):
            return None
    return g


class _Corr:
    def __init__(self):
        self.p = []

    def step(self, x, y):
        if x is not None and y is not None:
            self.p.append((float(x), float(y)))

    def finalize(self):
        n = len(self.p)
        if n < 2:
            return None
        mx = sum(x for x, _ in self.p) / n
        my = sum(y for _, y in self.p) / n
        sxy = sum((x - mx) * (y - my) for x, y in self.p)
        sxx = sum((x - mx) ** 2 for x, _ in self.p)
        syy = sum((y - my) ** 2 for _, y in self.p)
        return None if sxx == 0 or syy == 0 else sxy / math.sqrt(sxx * syy)


class _Covar:
    def __init__(self):
        self.p = []

    def step(self, x, y):
        if x is not None and y is not None:
            self.p.append((float(x), float(y)))

    def finalize(self):
        n = len(self.p)
        if not n:
            return None
        mx = sum(x for x, _ in self.p) / n
        my = sum(y for _, y in self.p) / n
        return sum((x - mx) * (y - my) for x, y in self.p) / n


class _Var:
    sample = False
    root = False

    def __init__(self):
        self.v = []

    def step(self, x):
        if x is not None:
            self.v.append(float(x))

    def finalize(self):
        n = len(self.v) - (1 if self.sample else 0)
        if n <= 0:
            return None
        m = sum(self.v) / len(self.v)
        r = sum((x - m) ** 2 for x in self.v) / n
        return math.sqrt(r) if self.root else r


class _VarSamp(_Var):
    sample = True


class _Std(_Var):
    root = True


class _StdSamp(_Var):
    sample = True
    root = True


class _Collect:
    unique = True

    def __init__(self):
        self.v = []

    def step(self, x):
        if x is not None and (not self.unique or x not in self.v):
            self.v.append(x)

    def finalize(self):
        return json.dumps(self.v, ensure_ascii=False)


class _CollectList(_Collect):
    unique = False


def register_functions(conn):
    simple = {
        "regexp_replace": (3, f_regexp_replace), "regexp_extract": (-1, f_regexp_extract),
        "regexp": (2, f_regexp), "substr": (-1, f_substr), "substring": (-1, f_substr),
        "year": (1, f_year), "month": (1, f_month), "day": (1, f_day), "dayofmonth": (1, f_day),
        "to_date": (1, f_to_date), "datediff": (2, f_datediff), "date_add": (2, f_date_add),
        "unix_timestamp": (-1, f_unix_timestamp), "from_unixtime": (-1, f_from_unixtime),
        "concat": (-1, f_concat), "concat_ws": (-1, f_concat_ws), "nvl": (2, f_nvl),
        "hive_cast": (2, hive_cast),
        "floor": (1, _num(lambda x: int(math.floor(x)))), "ceil": (1, _num(lambda x: int(math.ceil(x)))),
        "ceiling": (1, _num(lambda x: int(math.ceil(x)))), "sqrt": (1, _num(lambda x: math.sqrt(x) if x >= 0 else None)),
        "pow": (2, _num(lambda x, y: x ** y)), "power": (2, _num(lambda x, y: x ** y)),
        "ln": (1, _num(lambda x: math.log(x) if x > 0 else None)),
        "log10": (1, _num(lambda x: math.log10(x) if x > 0 else None)), "exp": (1, _num(math.exp)),
    }
    for name, (n, f) in simple.items():
        conn.create_function(name, n, f)
    aggs = {"corr": (2, _Corr), "covar_pop": (2, _Covar), "variance": (1, _Var), "var_pop": (1, _Var),
            "var_samp": (1, _VarSamp), "stddev": (1, _Std), "stddev_pop": (1, _Std),
            "stddev_samp": (1, _StdSamp), "collect_set": (1, _Collect), "collect_list": (1, _CollectList)}
    for name, (n, c) in aggs.items():
        conn.create_aggregate(name, n, c)


# ======================================================================
#  Traduction HiveQL -> SQLite
# ======================================================================

_STR_RE = re.compile(r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\"")


def protect_strings(sql):
    """Remplace les chaînes par des jetons pour pouvoir réécrire le reste sans risque."""
    lits = []

    def keep(m):
        s = m.group(0)
        body = s[1:-1]
        if s[0] == '"':
            body = body.replace('\\"', '"')
        body = body.replace("\\'", "'").replace("''", "'")
        lits.append(body)
        return "\x00%d\x00" % (len(lits) - 1)

    return _STR_RE.sub(keep, sql), lits


def restore_strings(sql, lits):
    return re.sub(r"\x00(\d+)\x00", lambda m: "'" + lits[int(m.group(1))].replace("'", "''") + "'", sql)


def match_paren(s, i):
    """Index de la parenthèse fermante correspondant à s[i] == '('."""
    depth = 0
    for j in range(i, len(s)):
        if s[j] == "(":
            depth += 1
        elif s[j] == ")":
            depth -= 1
            if depth == 0:
                return j
    raise HiveError("FAILED: ParseException : parenthèse fermante manquante")


def split_top(s, sep=","):
    out, depth, cur = [], 0, []
    for ch in s:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == sep and depth == 0:
            out.append("".join(cur))
            cur = []
        else:
            cur.append(ch)
    if "".join(cur).strip():
        out.append("".join(cur))
    return [x.strip() for x in out]


def rewrite_casts(sql):
    while True:
        ms = list(re.finditer(r"\bCAST\s*\(", sql, re.I))
        if not ms:
            return sql
        m = ms[-1]
        op = m.end() - 1
        cl = match_paren(sql, op)
        inner = sql[op + 1:cl]
        depth, pos = 0, None
        for am in re.finditer(r"\(|\)|\s+AS\s+", inner, re.I):
            tok = am.group(0)
            if tok == "(":
                depth += 1
            elif tok == ")":
                depth -= 1
            elif depth == 0:
                pos = am
        if pos is None:
            raise HiveError("FAILED: ParseException : CAST sans « AS type »")
        expr, typ = inner[:pos.start()], inner[pos.end():].strip()
        sql = sql[:m.start()] + "hive_cast(%s, '%s')" % (expr, typ.upper()) + sql[cl + 1:]


def translate(sql):
    s, lits = protect_strings(sql)
    s = re.sub(r"\bCURRENT_DATE\s*\(\s*\)", "CURRENT_DATE", s, flags=re.I)
    s = re.sub(r"\bCURRENT_TIMESTAMP\s*\(\s*\)", "CURRENT_TIMESTAMP", s, flags=re.I)
    s = re.sub(r"\bRLIKE\b", "REGEXP", s, flags=re.I)
    s = re.sub(r"\bIF\s*\(", "iif(", s, flags=re.I)
    s = re.sub(r"\bSORT\s+BY\b", "ORDER BY", s, flags=re.I)
    s = re.sub(r"\bCLUSTER\s+BY\b", "ORDER BY", s, flags=re.I)
    s = re.sub(r"\bDISTRIBUTE\s+BY\s+[\w.]+(\s*,\s*[\w.]+)*", "", s, flags=re.I)
    s = re.sub(r"\bdefault\.", "hive_default.", s, flags=re.I)
    s = s.replace("`", '"')
    s = rewrite_casts(s)
    s = s.replace("/", "*1.0/")                       # Hive : « / » renvoie toujours un DOUBLE
    s = re.sub(r"\bDIV\b", "/", s, flags=re.I)        # Hive : DIV = division entière
    return restore_strings(s, lits)


# ======================================================================
#  Découpage d'un script en instructions
# ======================================================================

_DFS_RE = re.compile(r"^\s*(hdfs\s+dfs|hadoop\s+fs|dfs)\s", re.I)


def split_statements(text):
    stmts, cur, quote = [], [], None
    lines = text.replace("\r\n", "\n").split("\n")
    for line in lines:
        if not quote and not "".join(cur).strip() and _DFS_RE.match(line):
            stmts.append(line.strip().rstrip(";").strip())
            continue
        i = 0
        while i < len(line):
            ch = line[i]
            if quote:
                cur.append(ch)
                if ch == "\\" and i + 1 < len(line):
                    cur.append(line[i + 1])
                    i += 1
                elif ch == quote:
                    quote = None
            elif ch in "'\"`":
                quote = ch
                cur.append(ch)
            elif line.startswith("--", i):
                break
            elif ch == ";":
                st = "".join(cur).strip()
                if st:
                    stmts.append(st)
                cur = []
            else:
                cur.append(ch)
            i += 1
        cur.append("\n")
    st = "".join(cur).strip()
    if st:
        stmts.append(st)
    return stmts


# ======================================================================
#  Le moteur
# ======================================================================

class MiniHive:
    def __init__(self, data_dir, root):
        self.data_dir = os.path.abspath(data_dir)
        self.root = os.path.abspath(root)
        self.hdfs = os.path.join(self.root, "hdfs")
        self.ms_path = os.path.join(self.root, "metastore.json")
        self.lock = threading.RLock()
        self.new_session()
        self._init_storage()

    # ---------- session, stockage ----------
    def new_session(self):
        self.conf = dict(DEFAULT_CONF)
        self.db = "default"

    def _init_storage(self):
        for d in ("user/hive/warehouse", "user/cloudera", "tmp"):
            os.makedirs(os.path.join(self.hdfs, d), exist_ok=True)
        if os.path.exists(self.ms_path):
            with open(self.ms_path, encoding="utf-8") as f:
                self.ms = json.load(f)
        else:
            self.ms = {"databases": {"default": {"comment": "Default Hive database", "location": "/user/hive/warehouse",
                                                 "props": {}, "tables": {}}}}
            self._save()

    def _save(self):
        with open(self.ms_path, "w", encoding="utf-8") as f:
            json.dump(self.ms, f, ensure_ascii=False, indent=1)

    def reset(self):
        with self.lock:
            if os.path.isdir(self.root):
                shutil.rmtree(self.root)
            self.new_session()
            self._init_storage()

    # ---------- chemins ----------
    @staticmethod
    def norm_hpath(p):
        p = re.sub(r"^hdfs://[^/]*", "", p.strip())
        p = "/" + p.strip("/")
        parts = []
        for part in p.split("/"):
            if part in ("", "."):
                continue
            if part == "..":
                if parts:
                    parts.pop()
            else:
                parts.append(part)
        return "/" + "/".join(parts)

    def real(self, hpath):
        return os.path.join(self.hdfs, self.norm_hpath(hpath).lstrip("/"))

    def resolve_local(self, p):
        """Retrouve un fichier « local » de la VM (/home/cloudera/...) dans le dossier donnees/."""
        cands = [p, os.path.join(self.data_dir, os.path.basename(p))]
        base = os.path.basename(p)
        stem, ext = os.path.splitext(base)
        stem2 = re.sub(r"\d+$", "", stem)
        for st in dict.fromkeys([stem, stem2]):
            for e in dict.fromkeys([ext, ".txt", ".csv"]):
                cands.append(os.path.join(self.data_dir, st + e))
        for c in cands:
            if os.path.isfile(c):
                return c
        raise HiveError("copyFromLocal: `%s': No such file or directory (fichier introuvable, ni tel quel ni dans %s)"
                        % (p, self.data_dir))

    def warehouse_path(self, db, table):
        d = self.ms["databases"][db]
        return self.norm_hpath(d["location"] + "/" + table)

    # ---------- métadonnées ----------
    def split_name(self, name):
        name = name.strip("`").lower()
        if "." in name:
            db, t = name.split(".", 1)
            return db, t
        return self.db, name

    def get_table(self, name, must=True):
        db, t = self.split_name(name)
        meta = self.ms["databases"].get(db, {}).get("tables", {}).get(t)
        if meta is None and must:
            raise HiveError("FAILED: SemanticException [Error 10001]: Table not found '%s'" % name)
        return meta

    # ---------- point d'entrée ----------
    def run_script(self, text, stop_on_error=True):
        out = []
        with self.lock:
            for st in split_statements(text):
                r = self.execute(st)
                out.append(r)
                if r["kind"] == "error" and stop_on_error:
                    break
        return out

    def execute(self, stmt):
        t0 = time.time()
        with self.lock:
            try:
                r = self._dispatch(stmt)
            except HiveError as e:
                r = {"kind": "error", "message": str(e)}
            except sqlite3.Error as e:
                r = {"kind": "error", "message": self._sqlite_error(e)}
            except RecursionError:
                r = {"kind": "error", "message": "FAILED: requête trop imbriquée"}
        r["stmt"] = stmt
        r["elapsed"] = round(time.time() - t0, 3)
        r["db"] = self.db
        return r

    @staticmethod
    def _sqlite_error(e):
        msg = str(e)
        m = re.match(r"no such table: (?:main\.)?(\S+)", msg)
        if m:
            return "FAILED: SemanticException [Error 10001]: Table not found '%s'" % m.group(1)
        m = re.match(r"no such column: (\S+)", msg)
        if m:
            return "FAILED: SemanticException [Error 10004]: Invalid table alias or column reference '%s'" % m.group(1)
        m = re.match(r"ambiguous column name: (\S+)", msg)
        if m:
            return "FAILED: SemanticException Ambiguous column reference %s (préfixez par l'alias de la table)" % m.group(1)
        m = re.match(r"no such function: (\S+)", msg)
        if m:
            return "FAILED: SemanticException [Error 10011]: Invalid function %s" % m.group(1)
        return "FAILED: ParseException : %s" % msg

    def _dispatch(self, s):
        u = re.sub(r"\s+", " ", s.strip()).upper()
        if _DFS_RE.match(s):
            return self.dfs(s)
        if u == "SET" or u.startswith("SET "):
            return self.do_set(s)
        if re.match(r"^RESET\b", u):
            self.conf = dict(DEFAULT_CONF)
            return msg("Configuration réinitialisée.")
        if re.match(r"^CREATE (DATABASE|SCHEMA)\b", u):
            return self.create_database(s)
        if re.match(r"^DROP (DATABASE|SCHEMA)\b", u):
            return self.drop_database(s)
        if re.match(r"^ALTER (DATABASE|SCHEMA)\b", u):
            return self.alter_database(s)
        if re.match(r"^USE ", u):
            return self.use(s)
        if re.match(r"^SHOW (DATABASES|SCHEMAS)", u):
            return table(["database_name"], [[d] for d in sorted(self.ms["databases"])])
        if re.match(r"^SHOW TABLES", u):
            return self.show_tables(s)
        if re.match(r"^SHOW PARTITIONS ", u):
            return self.show_partitions(s)
        if re.match(r"^SHOW TBLPROPERTIES ", u):
            return self.show_tblproperties(s)
        if re.match(r"^(DESCRIBE|DESC) (DATABASE|SCHEMA) ", u):
            return self.describe_database(s)
        if re.match(r"^(DESCRIBE|DESC) ", u):
            return self.describe(s)
        if re.match(r"^CREATE (OR REPLACE )?VIEW ", u):
            return self.create_view(s)
        if re.match(r"^DROP VIEW ", u):
            return self.drop_view(s)
        if re.match(r"^CREATE (EXTERNAL )?TABLE ", u):
            return self.create_table(s)
        if re.match(r"^DROP TABLE ", u):
            return self.drop_table(s)
        if re.match(r"^LOAD DATA ", u):
            return self.load_data(s)
        if re.match(r"^INSERT (OVERWRITE|INTO) ", u):
            return self.insert(s)
        if re.match(r"^(SELECT|WITH|VALUES)\b", u) or u.startswith("("):
            cols, rows, info = self.query(s)
            return table(cols, rows, info)
        if re.match(r"^EXPLAIN\b", u):
            return msg("EXPLAIN n'est pas simulé : le mini-Hive exécute les requêtes avec SQLite, pas MapReduce.")
        raise HiveError("FAILED: instruction non prise en charge par le mini-Hive : %s" % s.split()[0])

    # ---------- SET ----------
    def do_set(self, s):
        body = s.strip()[3:].strip()
        if not body:
            return table(["propriété", "valeur"], [[k, v] for k, v in sorted(self.conf.items())])
        if "=" not in body:
            k = body.strip()
            return msg("%s=%s" % (k, self.conf.get(k, "<non défini>")))
        k, v = body.split("=", 1)
        k, v = k.strip(), v.strip()
        self.conf[k] = v
        return msg("%s=%s" % (k, v))

    # ---------- bases de données ----------
    def create_database(self, s):
        m = re.match(r"(?is)^CREATE\s+(?:DATABASE|SCHEMA)\s+(IF\s+NOT\s+EXISTS\s+)?(\w+)(.*)$", s.strip())
        if not m:
            raise HiveError("FAILED: ParseException : syntaxe CREATE DATABASE")
        ine, name, rest = m.group(1), m.group(2).lower(), m.group(3)
        if name in self.ms["databases"]:
            if ine:
                return msg("OK (la base %s existe déjà)" % name)
            raise HiveError("FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. "
                            "Database %s already exists" % name)
        cm = re.search(r"(?i)COMMENT\s+'([^']*)'", rest)
        lm = re.search(r"(?i)LOCATION\s+'([^']*)'", rest)
        loc = self.norm_hpath(lm.group(1)) if lm else "/user/hive/warehouse/%s.db" % name
        os.makedirs(self.real(loc), exist_ok=True)
        self.ms["databases"][name] = {"comment": cm.group(1) if cm else "", "location": loc, "props": {}, "tables": {}}
        self._save()
        return msg("OK · base %s créée, répertoire HDFS %s" % (name, loc))

    def alter_database(self, s):
        m = re.match(r"(?is)^ALTER\s+(?:DATABASE|SCHEMA)\s+(\w+)\s+SET\s+DBPROPERTIES\s*\((.*)\)\s*$", s.strip())
        if not m:
            raise HiveError("FAILED: seule la forme ALTER DATABASE x SET DBPROPERTIES (...) est simulée")
        name = m.group(1).lower()
        if name not in self.ms["databases"]:
            raise HiveError("FAILED: SemanticException [Error 10072]: Database does not exist: %s" % name)
        for k, v in re.findall(r"'([^']*)'\s*=\s*'([^']*)'", m.group(2)):
            self.ms["databases"][name]["props"][k] = v
        self._save()
        return msg("OK")

    def describe_database(self, s):
        m = re.match(r"(?is)^DESC(?:RIBE)?\s+(?:DATABASE|SCHEMA)\s+(EXTENDED\s+)?(\w+)", s.strip())
        name = m.group(2).lower()
        d = self.ms["databases"].get(name)
        if not d:
            raise HiveError("FAILED: SemanticException [Error 10072]: Database does not exist: %s" % name)
        row = [name, d["comment"], HOST_URI + d["location"], "cloudera", "USER"]
        cols = ["db_name", "comment", "location", "owner_name", "owner_type"]
        if m.group(1):
            cols.append("parameters")
            row.append("{" + ", ".join("%s=%s" % kv for kv in d["props"].items()) + "}")
        return table(cols, [row])

    def drop_database(self, s):
        m = re.match(r"(?is)^DROP\s+(?:DATABASE|SCHEMA)\s+(IF\s+EXISTS\s+)?(\w+)\s*(CASCADE|RESTRICT)?", s.strip())
        name = m.group(2).lower()
        if name not in self.ms["databases"]:
            if m.group(1):
                return msg("OK")
            raise HiveError("FAILED: SemanticException [Error 10072]: Database does not exist: %s" % name)
        if name == "default":
            raise HiveError("FAILED: Can not drop default database")
        d = self.ms["databases"][name]
        if d["tables"] and (m.group(3) or "").upper() != "CASCADE":
            raise HiveError("FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. "
                            "InvalidOperationException(message:Database %s is not empty. One or more tables exist.)" % name)
        for t in list(d["tables"]):
            self.drop_table("DROP TABLE %s.%s" % (name, t), quiet_view=True)
        shutil.rmtree(self.real(d["location"]), ignore_errors=True)
        del self.ms["databases"][name]
        if self.db == name:
            self.db = "default"
        self._save()
        return msg("OK · base %s supprimée" % name)

    def use(self, s):
        name = s.strip().split()[1].strip("`").lower()
        if name not in self.ms["databases"]:
            raise HiveError("FAILED: SemanticException [Error 10072]: Database does not exist: %s" % name)
        self.db = name
        return msg("OK · base courante : %s" % name)

    def show_tables(self, s):
        m = re.search(r"(?i)\bIN\s+(\w+)", s)
        db = m.group(1).lower() if m else self.db
        lm = re.search(r"(?i)LIKE\s+'([^']*)'", s)
        names = sorted(self.ms["databases"].get(db, {}).get("tables", {}))
        if lm:
            pat = "^" + lm.group(1).replace("*", ".*") + "$"
            names = [n for n in names if re.match(pat, n)]
        return table(["tab_name"], [[n] for n in names])

    # ---------- CREATE TABLE ----------
    def create_table(self, s):
        st = s.strip()
        m = re.match(r"(?is)^CREATE\s+(EXTERNAL\s+)?TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.`]+)\s*", st)
        external, ine, name = bool(m.group(1)), bool(m.group(2)), m.group(3)
        rest = st[m.end():]
        if not rest.startswith("("):
            am = re.search(r"(?is)\bAS\s+(SELECT|WITH)\b", rest)
            if am:
                return self.ctas(external, ine, name, rest[:am.start()], rest[am.start() + 2:].strip())
            lk = re.match(r"(?is)^LIKE\s+([\w.]+)", rest)
            if lk:
                src = self.get_table(lk.group(1))
                return self._register_table(name, external, ine, src["columns"], src["partitions"], src["bucket"],
                                            src["sep"], None, 0, "")
            raise HiveError("FAILED: ParseException : liste de colonnes attendue après le nom de la table")
        cl = match_paren(rest, 0)
        cols = self._parse_cols(rest[1:cl])
        tail = rest[cl + 1:]
        parts, bucket = [], None
        pm = re.search(r"(?is)PARTITIONED\s+BY\s*\(", tail)
        if pm:
            pc = match_paren(tail, pm.end() - 1)
            parts = self._parse_cols(tail[pm.end():pc])
            pnames = {c[0] for c in parts}
            dup = [c[0] for c in cols if c[0] in pnames]
            if dup:
                raise HiveError("FAILED: SemanticException [Error 10035]: Column repeated in partitioning columns "
                                "(« %s » ne doit pas figurer dans la liste des colonnes)" % dup[0])
        bm = re.search(r"(?is)CLUSTERED\s+BY\s*\(\s*([\w`]+)[^)]*\)(?:\s*SORTED\s+BY\s*\([^)]*\))?\s*INTO\s+(\d+)\s+BUCKETS", tail)
        if bm:
            bcol = bm.group(1).strip("`").lower()
            allc = {c[0] for c in cols}
            if bcol not in allc:
                raise HiveError("FAILED: SemanticException [Error 10002]: Invalid column reference %s" % bcol)
            bucket = {"col": bcol, "n": int(bm.group(2))}
        sep = "\x01"
        fm = re.search(r"(?is)FIELDS\s+TERMINATED\s+BY\s+('(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\")", tail)
        if fm:
            sep = unescape_delim(fm.group(1)[1:-1])
        lm = re.search(r"(?is)LOCATION\s+'([^']*)'", tail)
        skip = 0
        sm = re.search(r"(?is)[\"']skip\.header\.line\.count[\"']\s*=\s*[\"'](\d+)[\"']", tail)
        if sm:
            skip = int(sm.group(1))
        cm = re.search(r"(?is)^\s*COMMENT\s+'([^']*)'", tail)
        return self._register_table(name, external, ine, cols, parts, bucket, sep,
                                    lm.group(1) if lm else None, skip, cm.group(1) if cm else "")

    def _parse_cols(self, body):
        cols = []
        for c in split_top(body):
            c = re.sub(r"(?is)\s+COMMENT\s+'.*'$", "", c).strip()
            if not c:
                continue
            mm = re.match(r"(?s)^`?(\w+)`?\s+(.+)$", c)
            if not mm:
                raise HiveError("FAILED: ParseException : définition de colonne invalide « %s »" % c)
            cols.append([mm.group(1).lower(), re.sub(r"\s+", "", mm.group(2)).upper()])
        if not cols:
            raise HiveError("FAILED: ParseException : la table doit avoir au moins une colonne")
        return cols

    def _register_table(self, name, external, ine, cols, parts, bucket, sep, location, skip, comment):
        db, t = self.split_name(name)
        if db not in self.ms["databases"]:
            raise HiveError("FAILED: SemanticException [Error 10072]: Database does not exist: %s" % db)
        if t in self.ms["databases"][db]["tables"]:
            if ine:
                return msg("OK (la table %s existe déjà : rien n'est modifié)" % t)
            raise HiveError("FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. "
                            "AlreadyExistsException(message:Table %s already exists)" % t)
        loc = self.norm_hpath(location) if location else self.warehouse_path(db, t)
        os.makedirs(self.real(loc), exist_ok=True)
        self.ms["databases"][db]["tables"][t] = {
            "name": t, "db": db, "external": external, "view": None, "columns": cols, "partitions": parts,
            "bucket": bucket, "sep": sep, "location": loc, "skip_header": skip, "comment": comment,
            "created": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        self._save()
        kind = "externe" if external else "interne (gérée)"
        extra = ""
        if location:
            files = self._list_data_files(loc)
            extra = " · %d fichier(s) déjà présent(s) dans le répertoire : la table les lit immédiatement" % len(files) \
                if files else " · répertoire vide pour l'instant"
        return msg("OK · table %s %s.%s créée sur %s%s" % (kind, db, t, loc, extra))

    def ctas(self, external, ine, name, middle, select):
        if external:
            raise HiveError("FAILED: SemanticException [Error 10070]: CREATE-TABLE-AS-SELECT cannot create external table")
        if re.search(r"(?i)PARTITIONED\s+BY", middle):
            raise HiveError("FAILED: SemanticException [Error 10068]: CREATE-TABLE-AS-SELECT does not support "
                            "partitioning in the target table")
        db, t = self.split_name(name)
        if t in self.ms["databases"].get(db, {}).get("tables", {}):
            if ine:
                return msg("OK (la table %s existe déjà : rien n'est modifié)" % t)
            raise HiveError("FAILED: SemanticException [Error 10036]: Table already exists: %s" % t)
        cols, rows, info = self.query(select)
        types = []
        for i in range(len(cols)):
            vals = [r[i] for r in rows if r[i] is not None]
            if vals and all(isinstance(v, int) and not isinstance(v, bool) for v in vals):
                types.append("BIGINT" if any(abs(v) >= 2 ** 31 for v in vals) else "INT")
            elif vals and all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in vals):
                types.append("DOUBLE")
            else:
                types.append("STRING")
        names = []
        for i, c in enumerate(cols):
            c = c.lower()
            names.append(c if re.match(r"^[a-z_]\w*$", c) else "_c%d" % i)
        sep = "\x01"
        fm = re.search(r"(?is)FIELDS\s+TERMINATED\s+BY\s+'((?:[^'\\]|\\.)*)'", middle)
        if fm:
            sep = unescape_delim(fm.group(1))
        self._register_table(name, False, False, [[n, ty] for n, ty in zip(names, types)], [], None, sep, None, 0, "")
        meta = self.get_table(name)
        path = self._write_rows(meta, meta["location"], rows, overwrite=True)
        self._save()
        return msg("OK · table interne %s.%s créée par CTAS : %d ligne(s) écrite(s) dans %s"
                   % (db, t, len(rows), ", ".join(path)), info)

    # ---------- vues ----------
    def create_view(self, s):
        m = re.match(r"(?is)^CREATE\s+(OR\s+REPLACE\s+)?VIEW\s+(IF\s+NOT\s+EXISTS\s+)?([\w.`]+)\s*(?:\([^)]*\)\s*)?AS\s+(.*)$",
                     s.strip())
        if not m:
            raise HiveError("FAILED: ParseException : syntaxe CREATE VIEW nom AS SELECT ...")
        repl, ine, name, sql = bool(m.group(1)), bool(m.group(2)), m.group(3), m.group(4).strip()
        db, v = self.split_name(name)
        tables = self.ms["databases"][db]["tables"]
        if v in tables and not repl:
            if ine:
                return msg("OK (la vue %s existe déjà)" % v)
            raise HiveError("FAILED: SemanticException [Error 10036]: Table already exists: %s" % v)
        cols, _, _ = self.query(sql, limit_probe=True)   # vérifie que la requête est valide
        tables[v] = {"name": v, "db": db, "external": False, "view": sql, "columns": [[c.lower(), "STRING"] for c in cols],
                     "partitions": [], "bucket": None, "sep": None, "location": None, "skip_header": 0, "comment": "",
                     "created": _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
        self._save()
        return msg("OK · vue %s.%s enregistrée dans le Metastore (aucune donnée copiée dans HDFS)" % (db, v))

    def drop_view(self, s):
        m = re.match(r"(?is)^DROP\s+VIEW\s+(IF\s+EXISTS\s+)?([\w.`]+)", s.strip())
        meta = self.get_table(m.group(2), must=not m.group(1))
        if meta is None:
            return msg("OK")
        if not meta["view"]:
            raise HiveError("FAILED: SemanticException [Error 10131]: Cannot drop a base table with DROP VIEW")
        del self.ms["databases"][meta["db"]]["tables"][meta["name"]]
        self._save()
        return msg("OK · vue supprimée")

    # ---------- DROP TABLE ----------
    def drop_table(self, s, quiet_view=False):
        m = re.match(r"(?is)^DROP\s+TABLE\s+(IF\s+EXISTS\s+)?([\w.`]+)(\s+PURGE)?", s.strip())
        meta = self.get_table(m.group(2), must=not m.group(1))
        if meta is None:
            return msg("OK")
        if meta["view"] and not quiet_view:
            raise HiveError("FAILED: SemanticException [Error 10132]: Cannot drop a view with DROP TABLE")
        del self.ms["databases"][meta["db"]]["tables"][meta["name"]]
        self._save()
        if meta["view"]:
            return msg("OK")
        if meta["external"]:
            return msg("OK · table externe supprimée du Metastore. Les fichiers restent dans %s" % meta["location"])
        shutil.rmtree(self.real(meta["location"]), ignore_errors=True)
        return msg("OK · table interne supprimée : schéma ET données (%s effacé de HDFS)" % meta["location"])

    # ---------- LOAD DATA ----------
    def load_data(self, s):
        m = re.match(r"(?is)^LOAD\s+DATA\s+(LOCAL\s+)?INPATH\s+'([^']*)'\s+(OVERWRITE\s+)?INTO\s+TABLE\s+([\w.`]+)"
                     r"(?:\s+PARTITION\s*\((.*)\))?\s*$", s.strip())
        if not m:
            raise HiveError("FAILED: ParseException : syntaxe LOAD DATA [LOCAL] INPATH '...' [OVERWRITE] INTO TABLE t")
        local, src, overwrite, name, pspec = m.groups()
        meta = self.get_table(name)
        if meta["view"]:
            raise HiveError("FAILED: SemanticException [Error 10090]: A view cannot be used as target table for LOAD or INSERT")
        dest = meta["location"]
        if meta["partitions"]:
            if not pspec:
                raise HiveError("FAILED: SemanticException [Error 10062]: Need to specify partition columns because "
                                "the destination table is partitioned")
            spec = dict((k.strip().lower(), v.strip().strip("'\"")) for k, v in
                        (x.split("=", 1) for x in split_top(pspec)))
            dest = dest + "/" + "/".join("%s=%s" % (c[0], spec.get(c[0], "")) for c in meta["partitions"])
        rdest = self.real(dest)
        os.makedirs(rdest, exist_ok=True)
        if local:
            srcs = [self.resolve_local(src)]
        else:
            rsrc = self.real(src)
            if os.path.isdir(rsrc):
                srcs = [os.path.join(rsrc, f) for f in sorted(os.listdir(rsrc)) if os.path.isfile(os.path.join(rsrc, f))]
            elif os.path.isfile(rsrc):
                srcs = [rsrc]
            else:
                raise HiveError("FAILED: SemanticException Line 1:17 Invalid path ''%s'': No files matching path %s%s"
                                % (src, HOST_URI, self.norm_hpath(src)))
        if overwrite:
            for f in os.listdir(rdest):
                p = os.path.join(rdest, f)
                shutil.rmtree(p) if os.path.isdir(p) else os.remove(p)
        moved = []
        for f in srcs:
            base = os.path.basename(f)
            target, k = os.path.join(rdest, base), 1
            while os.path.exists(target):
                stem, ext = os.path.splitext(base)
                target = os.path.join(rdest, "%s_copy_%d%s" % (stem, k, ext))
                k += 1
            if local:
                shutil.copy2(f, target)
            else:
                shutil.move(f, target)
            moved.append(dest + "/" + os.path.basename(target))
        verb = "copié" if local else "DÉPLACÉ (il n'est plus à son emplacement d'origine)"
        return msg("Loading data to table %s.%s · fichier %s vers %s" % (meta["db"], meta["name"], verb, ", ".join(moved)))

    # ---------- lecture d'une table depuis HDFS ----------
    def _list_data_files(self, hloc, recursive=False):
        root = self.real(hloc)
        if not os.path.isdir(root):
            return []
        out = []
        for f in sorted(os.listdir(root)):
            if f.startswith((".", "_")):
                continue
            p = os.path.join(root, f)
            if os.path.isdir(p):
                if recursive:
                    out += self._list_data_files(hloc + "/" + f, True)
            else:
                out.append(hloc + "/" + f)
        return out

    def _subdirs(self, hloc):
        root = self.real(hloc)
        if not os.path.isdir(root):
            return []
        return [f for f in sorted(os.listdir(root)) if os.path.isdir(os.path.join(root, f)) and not f.startswith((".", "_"))]

    def _partition_dirs(self, meta):
        """Liste [(chemin_hdfs, {col: valeur})] des partitions existantes."""
        res = [(meta["location"], {})]
        for pc, _pt in meta["partitions"]:
            nxt = []
            for path, vals in res:
                for d in self._subdirs(path):
                    if "=" in d and d.split("=", 1)[0].lower() == pc:
                        v = d.split("=", 1)[1]
                        nxt.append((path + "/" + d, dict(vals, **{pc: v})))
            res = nxt
        return res

    def read_table(self, meta, with_file=False, prune=None):
        """Lit les fichiers d'une table comme le ferait Hive. Renvoie (lignes, fichiers_lus)."""
        cols = meta["columns"]
        rows, files = [], []
        if meta["partitions"]:
            for path, vals in self._partition_dirs(meta):
                if prune and any(vals.get(k) != v for k, v in prune.items()):
                    continue
                pv = [None if vals[pc] == "__HIVE_DEFAULT_PARTITION__" else hive_cast(vals[pc], pt)
                      for pc, pt in meta["partitions"]]
                for f in self._list_data_files(path):
                    files.append(f)
                    for r in self._read_file(f, cols, meta["sep"], meta["skip_header"]):
                        rows.append(r + pv + ([HOST_URI + f] if with_file else []))
        else:
            loc = meta["location"]
            if self._subdirs(loc):
                rec = self.conf.get("mapred.input.dir.recursive", "").lower() == "true" and \
                    self.conf.get("hive.mapred.supports.subdirectories", "").lower() == "true"
                if not rec:
                    raise HiveError("Failed with exception java.io.IOException:java.io.IOException: Not a file: %s%s/%s"
                                    "\n→ le répertoire LOCATION de la table %s contient un sous-répertoire. Parade : "
                                    "SET mapred.input.dir.recursive=true; et SET hive.mapred.supports.subdirectories=true; "
                                    "(mais la table lira alors aussi les fichiers des sous-répertoires)"
                                    % (HOST_URI, loc, self._subdirs(loc)[0], meta["name"]))
                flist = self._list_data_files(loc, recursive=True)
            else:
                flist = self._list_data_files(loc)
            for f in flist:
                files.append(f)
                for r in self._read_file(f, cols, meta["sep"], meta["skip_header"]):
                    rows.append(r + ([HOST_URI + f] if with_file else []))
        return rows, files

    def _read_file(self, hpath, cols, sep, skip):
        with open(self.real(hpath), "rb") as fh:
            data = fh.read().decode("utf-8", errors="replace")   # Hive lit en UTF-8 : « é » Latin-1 devient « � »
        lines = data.splitlines()
        out = []
        for line in lines[skip:]:
            fields = line.split(sep)
            row = []
            for i, (_c, t) in enumerate(cols):
                v = fields[i] if i < len(fields) else None
                row.append(None if v is None or v == "\\N" else hive_cast(v, t))
            out.append(row)
        return out

    # ---------- requêtes ----------
    def _referenced(self, sql, db):
        low = protect_strings(sql)[0].lower()
        toks = set(re.findall(r"[a-z_][\w]*(?:\.[a-z_][\w]*)?", low))
        found = []
        for dname, d in self.ms["databases"].items():
            for tname, meta in d["tables"].items():
                if (dname == db and tname in toks) or ("%s.%s" % (dname, tname)) in toks:
                    found.append(meta)
        return found

    def query(self, sql, limit_probe=False):
        conn = sqlite3.connect(":memory:")
        register_functions(conn)
        info = []
        try:
            with_file = "input__file__name" in sql.lower()
            todo = self._referenced(sql, self.db)
            seen, metas = set(), []
            while todo:
                meta = todo.pop()
                key = (meta["db"], meta["name"])
                if key in seen:
                    continue
                seen.add(key)
                metas.append(meta)
                if meta["view"]:
                    todo += self._referenced(meta["view"], meta["db"])
            attached = set()
            prune_hint = self._prune_hint(sql)
            for meta in [m for m in metas if not m["view"]]:
                schema = "main" if meta["db"] == self.db else ("hive_default" if meta["db"] == "default" else meta["db"])
                if schema != "main" and schema not in attached:
                    conn.execute('ATTACH DATABASE \':memory:\' AS "%s"' % schema)
                    attached.add(schema)
                cols = meta["columns"] + meta["partitions"]
                prune = None
                if meta["partitions"]:
                    pn = {c[0] for c in meta["partitions"]}
                    prune = {k: v for k, v in prune_hint.items() if k in pn} or None
                rows, files = self.read_table(meta, with_file)
                total = len(files)
                defs = ", ".join('"%s" %s' % (c, sqlite_affinity(t)) for c, t in cols)
                if with_file:
                    defs += ', "input__file__name" TEXT'
                conn.execute('CREATE TABLE "%s"."%s" (%s)' % (schema, meta["name"], defs))
                if rows:
                    conn.executemany('INSERT INTO "%s"."%s" VALUES (%s)' % (schema, meta["name"],
                                     ",".join("?" * len(rows[0]))), rows)
                note = "%s.%s : %d ligne(s) dans %d fichier(s)" % (meta["db"], meta["name"], len(rows), total)
                if prune:
                    kept = len(self.read_table(meta, False, prune)[1])
                    note += " · élagage de partitions : Hive ne lirait que %d fichier(s) sur %d (%s)" % (
                        kept, total, ", ".join("%s=%s" % kv for kv in prune.items()))
                elif meta["partitions"]:
                    note += " · aucune partition éliminée : tous les répertoires sont lus"
                info.append(note)
            # vues : création dans l'ordre des dépendances
            views = [m for m in metas if m["view"]]
            for _ in range(len(views)):
                rest = []
                for v in views:
                    try:
                        conn.execute('CREATE TEMP VIEW "%s" AS %s' % (v["name"], translate(v["view"])))
                    except sqlite3.Error:
                        rest.append(v)
                views = rest
                if not views:
                    break
            for v in views:   # dernière tentative : laisse remonter l'erreur SQLite
                conn.execute('CREATE TEMP VIEW "%s" AS %s' % (v["name"], translate(v["view"])))
            if [m for m in metas if m["view"]]:
                info.append("vue(s) recalculée(s) à la volée : " + ", ".join(m["name"] for m in metas if m["view"]))
            cur = conn.execute(translate(sql))
            cols = []
            for i, d in enumerate(cur.description or []):
                n = d[0]
                cols.append(n if re.match(r"^[A-Za-z_]\w*$", n) else "_c%d" % i)
            rows = [list(r) for r in (cur.fetchmany(1) if limit_probe else cur.fetchall())]
            return cols, rows, info
        finally:
            conn.close()

    def _partition_files(self, meta):
        out = []
        for path, _ in self._partition_dirs(meta):
            out += self._list_data_files(path)
        return out

    @staticmethod
    def _prune_hint(sql):
        s, lits = protect_strings(sql)
        wm = re.search(r"(?is)\bWHERE\b(.*)", s)
        if not wm:
            return {}
        hint = {}
        if re.search(r"(?i)\bOR\b", wm.group(1)):
            return {}
        for col, tok in re.findall(r"(?:\b\w+\.)?\b(\w+)\s*=\s*\x00(\d+)\x00", wm.group(1)):
            hint[col.lower()] = lits[int(tok)]
        return hint

    # ---------- INSERT ----------
    def insert(self, s):
        m = re.match(r"(?is)^INSERT\s+(OVERWRITE|INTO)\s+(?:TABLE\s+)?([\w.`]+)\s*(?:PARTITION\s*\((.*?)\)\s*)?"
                     r"(?:IF\s+NOT\s+EXISTS\s+)?(SELECT\b.*|WITH\b.*|VALUES\b.*|FROM\b.*)$", s.strip())
        if not m:
            raise HiveError("FAILED: ParseException : syntaxe INSERT OVERWRITE|INTO TABLE t [PARTITION (...)] SELECT ...")
        mode, name, pspec, body = m.groups()
        overwrite = mode.upper() == "OVERWRITE"
        meta = self.get_table(name)
        if meta["view"]:
            raise HiveError("FAILED: SemanticException [Error 10090]: A view cannot be used as target table for LOAD or INSERT")
        if body.upper().startswith("FROM"):
            raise HiveError("FAILED: la forme « FROM ... INSERT ... » n'est pas simulée : écrivez INSERT ... SELECT ...")
        if body.upper().startswith("VALUES"):
            body = "SELECT * FROM (%s)" % body
        cols, rows, info = self.query(body)
        static, dynamic = {}, []
        if meta["partitions"]:
            if not pspec:
                raise HiveError("FAILED: SemanticException [Error 10062]: Need to specify partition columns because "
                                "the destination table is partitioned")
            for item in split_top(pspec):
                if "=" in item:
                    k, v = item.split("=", 1)
                    static[k.strip().lower()] = v.strip().strip("'\"")
                else:
                    dynamic.append(item.strip().lower())
            if dynamic:
                if self.conf.get("hive.exec.dynamic.partition", "true").lower() != "true":
                    raise HiveError("FAILED: SemanticException [Error 10095]: Dynamic partition is disabled. Either "
                                    "enable it by setting hive.exec.dynamic.partition=true or specify partition column values")
                if not static and self.conf.get("hive.exec.dynamic.partition.mode", "strict").lower() == "strict":
                    raise HiveError("FAILED: SemanticException [Error 10096]: Dynamic partition strict mode requires at "
                                    "least one static partition column. To turn this off set "
                                    "hive.exec.dynamic.partition.mode=nonstrict")
        elif pspec:
            raise HiveError("FAILED: SemanticException [Error 10001]: Table %s is not partitioned but partition spec "
                            "exists" % meta["name"])
        expected = len(meta["columns"]) + len(dynamic)
        if len(cols) != expected:
            raise HiveError("FAILED: SemanticException [Error 10044]: Cannot insert into target table because column "
                            "number/types are different '%s': Table insclause-0 has %d columns, but query has %d columns."
                            % (meta["name"], expected, len(cols)))
        groups = {}
        for r in rows:
            data = [hive_cast(v, t) for v, (_c, t) in zip(r[:len(meta["columns"])], meta["columns"])]
            dvals = dict(zip(dynamic, r[len(meta["columns"]):]))
            pvals = []
            for pc, pt in meta["partitions"]:
                v = static.get(pc) if pc in static else hive_str(hive_cast(dvals.get(pc), pt))
                pvals.append(v if v not in (None, "") else "__HIVE_DEFAULT_PARTITION__")
            groups.setdefault(tuple(pvals), []).append(data)
        if meta["partitions"] and not groups and static and not dynamic:
            groups[tuple(static[pc] for pc, _ in meta["partitions"])] = []
        if not meta["partitions"]:
            groups = {(): [hive_row for hive_row in groups.get((), [])]}
        written = []
        for pvals, data in sorted(groups.items()):
            path = meta["location"]
            if pvals:
                path += "/" + "/".join("%s=%s" % (pc, pv.replace("/", "%2F")) for (pc, _), pv in zip(meta["partitions"], pvals))
            written += self._write_rows(meta, path, data, overwrite, cast=False)
        notes = []
        if meta["bucket"] and self.conf.get("hive.enforce.bucketing", "false").lower() != "true":
            notes.append("hive.enforce.bucketing n'est pas à true : Hive 1.x n'écrit qu'UN fichier au lieu de %d buckets"
                         % meta["bucket"]["n"])
        part_note = ""
        if meta["partitions"]:
            part_note = " · %d partition(s) : %s" % (len(groups), ", ".join(
                "/".join("%s=%s" % (pc, pv) for (pc, _), pv in zip(meta["partitions"], k)) for k in sorted(groups)))
        return msg("Loading data to table %s.%s · %d ligne(s)%s · fichiers écrits : %s"
                   % (meta["db"], meta["name"], len(rows), part_note, ", ".join(written)), info + notes)

    def _write_rows(self, meta, hpath, rows, overwrite, cast=True):
        rdir = self.real(hpath)
        os.makedirs(rdir, exist_ok=True)
        if overwrite:
            for f in os.listdir(rdir):
                p = os.path.join(rdir, f)
                if os.path.isfile(p):
                    os.remove(p)
        if cast:
            rows = [[hive_cast(v, t) for v, (_c, t) in zip(r, meta["columns"])] for r in rows]
        sep = meta["sep"]

        def line(r):
            return sep.join("\\N" if v is None else hive_str(v) for v in r) + "\n"

        def free(name):
            target, k = name, 1
            while os.path.exists(os.path.join(rdir, target)):
                target = "%s_copy_%d" % (name, k)
                k += 1
            return target

        b = meta.get("bucket")
        enforce = self.conf.get("hive.enforce.bucketing", "false").lower() == "true"
        out = []
        if b and enforce:
            idx = [c[0] for c in meta["columns"]].index(b["col"])
            btype = meta["columns"][idx][1]
            buckets = [[] for _ in range(b["n"])]
            for r in rows:
                buckets[bucket_of(r[idx], btype, b["n"])].append(r)
            for i, br in enumerate(buckets):
                fname = free("%06d_0" % i)
                with open(os.path.join(rdir, fname), "w", encoding="utf-8", newline="") as fh:
                    fh.writelines(line(r) for r in br)
                out.append("%s/%s (%d o)" % (hpath, fname, os.path.getsize(os.path.join(rdir, fname))))
        else:
            fname = free("000000_0")
            with open(os.path.join(rdir, fname), "w", encoding="utf-8", newline="") as fh:
                fh.writelines(line(r) for r in rows)
            out.append("%s/%s (%d o)" % (hpath, fname, os.path.getsize(os.path.join(rdir, fname))))
        return out

    # ---------- DESCRIBE, SHOW ----------
    def describe(self, s):
        m = re.match(r"(?is)^DESC(?:RIBE)?\s+(FORMATTED\s+|EXTENDED\s+)?([\w.`]+)\s*$", s.strip())
        if not m:
            raise HiveError("FAILED: ParseException : syntaxe DESCRIBE [FORMATTED] table")
        meta = self.get_table(m.group(2))
        rows = [[c, t.lower(), ""] for c, t in meta["columns"]]
        if meta["partitions"]:
            rows += [["", None, None], ["# Partition Information", None, None], ["# col_name", "data_type", "comment"]]
            rows += [[c, t.lower(), ""] for c, t in meta["partitions"]]
        if m.group(1):
            if meta["view"]:
                ttype = "VIRTUAL_VIEW"
            else:
                ttype = "EXTERNAL_TABLE" if meta["external"] else "MANAGED_TABLE"
            rows += [["", None, None], ["# Detailed Table Information", None, None],
                     ["Database:", meta["db"], None], ["Owner:", "cloudera", None],
                     ["CreateTime:", meta["created"], None]]
            if not meta["view"]:
                rows.append(["Location:", HOST_URI + meta["location"], None])
            rows.append(["Table Type:", ttype, None])
            params = []
            if meta["external"]:
                params.append(["", "EXTERNAL", "TRUE"])
            if meta["comment"]:
                params.append(["", "comment", meta["comment"]])
            if meta["skip_header"]:
                params.append(["", "skip.header.line.count", str(meta["skip_header"])])
            if params:
                rows.append(["Table Parameters:", None, None])
                rows += params
            if meta["view"]:
                rows += [["", None, None], ["# View Information", None, None], ["View Original Text:", meta["view"], None]]
            else:
                b = meta["bucket"]
                rows += [["", None, None], ["# Storage Information", None, None],
                         ["SerDe Library:", "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe", None],
                         ["InputFormat:", "org.apache.hadoop.mapred.TextInputFormat", None],
                         ["Num Buckets:", str(b["n"]) if b else "-1", None],
                         ["Bucket Columns:", "[%s]" % (b["col"] if b else ""), None],
                         ["Storage Desc Params:", None, None],
                         ["", "field.delim", printable_delim(meta["sep"])]]
        return table(["col_name", "data_type", "comment"], rows)

    def show_partitions(self, s):
        meta = self.get_table(s.strip().split()[2])
        if not meta["partitions"]:
            raise HiveError("FAILED: Execution Error, return code 1 from org.apache.hadoop.hive.ql.exec.DDLTask. "
                            "Table %s is not a partitioned table" % meta["name"])
        rows = [["/".join("%s=%s" % (pc, vals[pc]) for pc, _ in meta["partitions"])]
                for _, vals in self._partition_dirs(meta)]
        return table(["partition"], rows)

    def show_tblproperties(self, s):
        meta = self.get_table(s.strip().split()[2])
        rows = []
        if meta["external"]:
            rows.append(["EXTERNAL", "TRUE"])
        if meta["comment"]:
            rows.append(["comment", meta["comment"]])
        if meta["skip_header"]:
            rows.append(["skip.header.line.count", str(meta["skip_header"])])
        files = [] if meta["view"] else (self._partition_files(meta) if meta["partitions"] else self._list_data_files(meta["location"]))
        rows.append(["numFiles", str(len(files))])
        rows.append(["totalSize", str(sum(os.path.getsize(self.real(f)) for f in files))])
        rows.append(["transient_lastDdlTime", str(int(time.time()))])
        return table(["prpt_name", "prpt_value"], rows)

    # ---------- commandes HDFS ----------
    def dfs(self, s):
        try:
            args = shlex.split(s)
        except ValueError as e:
            raise HiveError("dfs : %s" % e)
        args = args[2:] if args[0].lower() in ("hdfs", "hadoop") else args[1:]
        if not args:
            raise HiveError("Usage: hdfs dfs [-ls] [-mkdir] [-copyFromLocal] [-put] [-cat] [-rm] [-mv] [-cp] [-du] [-chmod]")
        cmd, rest = args[0], args[1:]
        flags = [a for a in rest if a.startswith("-") and len(a) > 1]
        paths = [a for a in rest if not (a.startswith("-") and len(a) > 1)]
        c = cmd.lower()
        if c in ("-mkdir",):
            out = []
            for p in paths:
                rp = self.real(p)
                if os.path.exists(rp):
                    if "-p" in flags:
                        continue
                    raise HiveError("mkdir: `%s': File exists" % self.norm_hpath(p))
                if not os.path.isdir(os.path.dirname(rp)) and "-p" not in flags:
                    raise HiveError("mkdir: `%s': No such file or directory (ajoutez -p pour créer les parents)"
                                    % self.norm_hpath(p))
                os.makedirs(rp, exist_ok=True)
                out.append(self.norm_hpath(p))
            return msg("Répertoire(s) créé(s) : " + (", ".join(out) or "aucun (déjà présents)"))
        if c in ("-chmod", "-chown", "-chgrp", "-setrep"):
            for p in paths[1:]:
                if not os.path.exists(self.real(p)):
                    raise HiveError("%s: `%s': No such file or directory" % (c[1:], self.norm_hpath(p)))
            return msg("OK (droits simulés : %s %s)" % (cmd, " ".join(paths)))
        if c in ("-copyfromlocal", "-put"):
            if len(paths) < 2:
                raise HiveError("%s: il faut une source locale et une destination HDFS" % cmd)
            dst = paths[-1]
            out = []
            for src in paths[:-1]:
                lp = self.resolve_local(src)
                rd = self.real(dst)
                if os.path.isdir(rd):
                    target = os.path.join(rd, os.path.basename(src))
                    htarget = self.norm_hpath(dst + "/" + os.path.basename(src))
                else:
                    if not os.path.isdir(os.path.dirname(rd)):
                        raise HiveError("%s: `%s': No such file or directory" % (cmd[1:], self.norm_hpath(dst)))
                    target, htarget = rd, self.norm_hpath(dst)
                if os.path.exists(target) and "-f" not in flags:
                    raise HiveError("%s: `%s': File exists (ajoutez -f pour écraser)" % (cmd[1:], htarget))
                shutil.copyfile(lp, target)
                note = "" if os.path.basename(lp) == os.path.basename(src) else " (fichier local utilisé : %s)" % os.path.basename(lp)
                out.append("%s → %s%s" % (src, htarget, note))
            return msg("Copié dans HDFS : " + " ; ".join(out))
        if c in ("-ls",):
            return msg(self._ls(paths or ["/user/cloudera"], "-R" in flags or "-r" in flags))
        if c in ("-lsr",):
            return msg(self._ls(paths, True))
        if c in ("-cat", "-text"):
            out = []
            for p in paths:
                rp = self.real(p)
                if not os.path.isfile(rp):
                    raise HiveError("cat: `%s': No such file or directory" % self.norm_hpath(p))
                with open(rp, "rb") as fh:
                    out.append(fh.read().decode("utf-8", errors="replace").replace("\x01", "^A"))
            return msg("".join(out) or "(fichier vide)")
        if c in ("-rm", "-rmr", "-rmdir"):
            rec = c == "-rmr" or any(f.lower() == "-r" for f in flags)
            out = []
            for p in paths:
                rp = self.real(p)
                if not os.path.exists(rp):
                    if "-f" in flags:
                        continue
                    raise HiveError("rm: `%s': No such file or directory" % self.norm_hpath(p))
                if os.path.isdir(rp):
                    if not rec and c != "-rmdir":
                        raise HiveError("rm: `%s': Is a directory (ajoutez -r)" % self.norm_hpath(p))
                    shutil.rmtree(rp)
                else:
                    os.remove(rp)
                out.append("Deleted " + self.norm_hpath(p))
            return msg("\n".join(out) or "Rien à supprimer")
        if c in ("-mv", "-cp"):
            if len(paths) != 2:
                raise HiveError("%s: il faut une source et une destination" % cmd)
            src, dst = self.real(paths[0]), self.real(paths[1])
            if not os.path.exists(src):
                raise HiveError("%s: `%s': No such file or directory" % (c[1:], self.norm_hpath(paths[0])))
            if os.path.isdir(dst):
                dst = os.path.join(dst, os.path.basename(src))
            if c == "-mv":
                shutil.move(src, dst)
            elif os.path.isdir(src):
                shutil.copytree(src, dst)
            else:
                shutil.copyfile(src, dst)
            return msg("OK")
        if c in ("-du", "-dus"):
            out = []
            for p in paths or ["/"]:
                rp = self.real(p)
                if not os.path.exists(rp):
                    raise HiveError("du: `%s': No such file or directory" % self.norm_hpath(p))
                items = [p] if ("-s" in flags or c == "-dus" or os.path.isfile(rp)) else \
                    [self.norm_hpath(p + "/" + f) for f in sorted(os.listdir(rp))]
                for it in items:
                    out.append("%-10d  %s" % (dir_size(self.real(it)), self.norm_hpath(it)))
            return msg("\n".join(out))
        if c in ("-touchz",):
            for p in paths:
                open(self.real(p), "a").close()
            return msg("OK")
        if c in ("-test",):
            return msg("0" if os.path.exists(self.real(paths[-1])) else "1")
        raise HiveError("%s: Unknown command (commandes simulées : -ls [-R], -mkdir [-p], -copyFromLocal, -put, -cat, "
                        "-rm [-r], -mv, -cp, -du, -chmod)" % cmd)

    def _ls(self, paths, recursive):
        out = []
        for p in paths:
            rp = self.real(p)
            if not os.path.exists(rp):
                raise HiveError("ls: `%s': No such file or directory" % self.norm_hpath(p))
            if os.path.isfile(rp):
                out.append(ls_line(rp, self.norm_hpath(p)))
                continue
            entries = []

            def walk(hp, r):
                for f in sorted(os.listdir(r)):
                    fp = os.path.join(r, f)
                    entries.append(ls_line(fp, self.norm_hpath(hp + "/" + f)))
                    if recursive and os.path.isdir(fp):
                        walk(hp + "/" + f, fp)
            walk(p, rp)
            if not recursive:
                out.append("Found %d items" % len(entries))
            out += entries
        return "\n".join(out)

    # ---------- état pour l'interface ----------
    def hdfs_tree(self):
        def node(rp, hp, depth):
            n = {"name": os.path.basename(rp) or "/", "path": hp, "dir": os.path.isdir(rp)}
            if n["dir"]:
                n["children"] = [node(os.path.join(rp, f), self.norm_hpath(hp + "/" + f), depth + 1)
                                 for f in sorted(os.listdir(rp))] if depth < 12 else []
            else:
                n["size"] = os.path.getsize(rp)
            return n
        with self.lock:
            return node(self.hdfs, "/", 0)

    def metastore_view(self):
        with self.lock:
            out = {"current": self.db, "databases": []}
            for name, d in sorted(self.ms["databases"].items()):
                tabs = []
                for t, meta in sorted(d["tables"].items()):
                    tabs.append({"name": t, "kind": "vue" if meta["view"] else ("externe" if meta["external"] else "interne"),
                                 "location": meta["location"], "columns": meta["columns"], "partitions": meta["partitions"],
                                 "bucket": meta["bucket"], "skip_header": meta["skip_header"]})
                out["databases"].append({"name": name, "location": d["location"], "tables": tabs})
            out["conf"] = {k: self.conf.get(k) for k in ("hive.exec.dynamic.partition", "hive.exec.dynamic.partition.mode",
                                                          "hive.enforce.bucketing", "mapred.input.dir.recursive",
                                                          "hive.mapred.supports.subdirectories")}
            return out

    def cat(self, hpath, limit=400):
        rp = self.real(hpath)
        if not os.path.isfile(rp):
            raise HiveError("cat: `%s': No such file or directory" % hpath)
        with open(rp, "rb") as fh:
            data = fh.read(200000)
        txt = data.decode("utf-8", errors="replace").replace("\x01", "␁")
        lines = txt.splitlines()
        return {"path": self.norm_hpath(hpath), "size": os.path.getsize(rp), "lines": lines[:limit],
                "truncated": len(lines) > limit}


# ---------- petites aides ----------

def msg(text, info=None):
    return {"kind": "text", "message": text, "info": info or []}


def table(cols, rows, info=None):
    return {"kind": "table", "columns": cols, "rows": rows, "info": info or []}


def unescape_delim(d):
    if d.startswith("\\"):
        body = d[1:]
        if body == "t":
            return "\t"
        if body.startswith("u"):
            return chr(int(body[1:], 16))
        if body.isdigit():
            return chr(int(body, 8))
        return body[:1]
    return d[:1] if d else "\x01"


def printable_delim(d):
    return {"\x01": "\\u0001 (^A, défaut de Hive)", "\t": "\\t"}.get(d, d)


def dir_size(p):
    if os.path.isfile(p):
        return os.path.getsize(p)
    return sum(os.path.getsize(os.path.join(r, f)) for r, _, fs in os.walk(p) for f in fs)


def ls_line(rp, hp):
    st = os.stat(rp)
    when = _dt.datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M")
    if os.path.isdir(rp):
        return "drwxrwxrwx   - cloudera supergroup %10d %s %s" % (0, when, hp)
    return "-rw-r--r--   1 cloudera supergroup %10d %s %s" % (st.st_size, when, hp)


def render_text_table(cols, rows, max_rows=200):
    vals = [[fmt_value(v) for v in r] for r in rows[:max_rows]]
    widths = [len(c) for c in cols]
    for r in vals:
        for i, v in enumerate(r):
            widths[i] = min(max(widths[i], len(v)), 90)
    sepl = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    out = [sepl, "|" + "|".join(" %-*s " % (w, c[:w]) for w, c in zip(widths, cols)) + "|", sepl]
    for r in vals:
        out.append("|" + "|".join(" %-*s " % (w, v[:w]) for w, v in zip(widths, r)) + "|")
    out.append(sepl)
    n = len(rows)
    out.append("%d ligne(s)%s" % (n, " (affichage limité à %d)" % max_rows if n > max_rows else ""))
    return "\n".join(out)


def render_result(r):
    head = "%s> %s" % (r.get("db", "hive"), r["stmt"].strip().splitlines()[0][:110] +
                       (" …" if len(r["stmt"].strip().splitlines()) > 1 else ""))
    if r["kind"] == "error":
        body = r["message"]
    elif r["kind"] == "table":
        body = render_text_table(r["columns"], r["rows"])
    else:
        body = r["message"]
    info = "".join("\n  · " + i for i in r.get("info", []))
    return "%s\n%s%s\n" % (head, body, info)

# ======================================================================
#  Le scénario du workshop (sujet 2023, adapté aux fichiers de donnees/)
# ======================================================================

PARTS = [
    {"id": "p1", "title": "Partie 1 · Bases et tables", "short": "Tables"},
    {"id": "p2", "title": "Partie 2 · Partitions et buckets", "short": "Partitions"},
    {"id": "p3", "title": "Partie 3 · Analyse des salaires", "short": "Analyse"},
]

STEPS = [
    # ------------------------------------------------------------ Partie 1
    {"id": "1.1", "part": "p1", "title": "Créer la base htables",
     "text": "Une base Hive n'est qu'un répertoire HDFS plus une entrée dans le Metastore. Exécutez, puis regardez "
             "le panneau HDFS : /user/hive/warehouse/htables.db vient d'apparaître.",
     "sql": "SET hive.cli.print.current.db=true;\nCREATE DATABASE htables;\nUSE htables;\nSHOW DATABASES;",
     "watch": ["Le répertoire htables.db sous /user/hive/warehouse", "La base courante affichée à gauche de l'invite"]},
    {"id": "1.2", "part": "p1", "title": "Déposer emploi_population.txt dans HDFS",
     "text": "Le fichier « local » de la VM (/home/cloudera/...) est pris dans le dossier donnees/. "
             "On crée /user/hive/data, qui servira de zone de dépôt (staging).",
     "sql": "hdfs dfs -mkdir /user/hive/data\nhdfs dfs -chmod 777 /user/hive/data\n"
            "hadoop fs -copyFromLocal /home/cloudera/emploi_population.txt /user/hive/data\nhdfs dfs -ls /user/hive/data",
     "watch": ["Le fichier de 646 octets dans /user/hive/data"]},
    {"id": "1.3", "part": "p1", "title": "Table externe log_emploi_population",
     "text": "LOCATION pointe vers un répertoire : la table lit tous les fichiers qui s'y trouvent, sans rien copier. "
             "DESCRIBE FORMATTED affiche Table Type: EXTERNAL_TABLE.",
     "sql": "CREATE EXTERNAL TABLE IF NOT EXISTS log_emploi_population (\n  genre STRING,\n  date_naissance DATE,\n"
            "  fonction STRING,\n  salaire STRING,\n  code_postal STRING,\n  id INT\n)\nCOMMENT 'table intermediaire'\n"
            "ROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE\nLOCATION '/user/hive/data';\n\n"
            "DESCRIBE FORMATTED log_emploi_population;\nSELECT * FROM log_emploi_population;",
     "watch": ["12 lignes", "Table Type: EXTERNAL_TABLE", "Aucun nouveau fichier dans HDFS"]},
    {"id": "1.4", "part": "p1", "title": "DROP d'une table externe : le fichier reste",
     "text": "On supprime la table, on vérifie que le fichier est toujours là, puis on recrée la table : "
             "elle retrouve ses 12 lignes immédiatement.",
     "sql": "DROP TABLE log_emploi_population;\nhdfs dfs -ls /user/hive/data\n\n"
            "CREATE EXTERNAL TABLE IF NOT EXISTS log_emploi_population (\n  genre STRING, date_naissance DATE, fonction STRING,\n"
            "  salaire STRING, code_postal STRING, id INT\n)\nROW FORMAT DELIMITED FIELDS TERMINATED BY ';'\n"
            "STORED AS TEXTFILE\nLOCATION '/user/hive/data';\n\nSELECT COUNT(*) FROM log_emploi_population;",
     "watch": ["emploi_population.txt toujours présent après le DROP", "COUNT(*) = 12 après la recréation"]},
    {"id": "1.5", "part": "p1", "title": "CTAS : la table interne emploi_population",
     "text": "CREATE TABLE ... AS SELECT calcule les colonnes finales et écrit un nouveau fichier dans l'entrepôt. "
             "Le séparateur par défaut de Hive est le caractère invisible ^A (␁ dans le panneau HDFS).",
     "sql": "CREATE TABLE IF NOT EXISTS emploi_population AS\nSELECT\n  SUBSTR(genre, 1, 1) AS sexe,\n"
            "  ROUND(DATEDIFF(CURRENT_TIMESTAMP(), date_naissance) / 365.2425) AS age,\n  fonction,\n  salaire,\n"
            "  code_postal,\n  id AS Id_empl\nFROM log_emploi_population;\n\n"
            "SELECT * FROM emploi_population;\nDESCRIBE FORMATTED emploi_population;",
     "watch": ["sexe vaut F ou M, age est un nombre", "Table Type: MANAGED_TABLE",
               "Le fichier 000000_0 dans /user/hive/warehouse/htables.db/emploi_population"]},
    {"id": "1.6", "part": "p1", "title": "Table interne personne et LOAD DATA",
     "text": "LOAD DATA INPATH ne copie pas : il déplace le fichier dans le répertoire de la table. "
             "Comparez les deux listings de /user/hive/warehouse.",
     "sql": "hadoop fs -copyFromLocal /home/cloudera/personne.txt /user/hive/warehouse/\nhdfs dfs -ls /user/hive/warehouse\n\n"
            "CREATE TABLE IF NOT EXISTS personne (\n  id INT,\n  nom STRING\n)\nCOMMENT 'table interne'\n"
            "ROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE;\n\n"
            "LOAD DATA INPATH '/user/hive/warehouse/personne.txt' OVERWRITE INTO TABLE personne;\n"
            "SELECT * FROM personne;\nhdfs dfs -ls /user/hive/warehouse",
     "watch": ["personne.txt a disparu de /user/hive/warehouse", "Il est maintenant dans htables.db/personne",
               "Le fichier a 3 colonnes, la table 2 : la 3e est ignorée"]},
    {"id": "1.7", "part": "p1", "title": "DROP d'une table interne : le fichier disparaît",
     "text": "Table interne = Hive est propriétaire des données. Le DROP efface le schéma ET le répertoire.",
     "sql": "DROP TABLE personne;\nhdfs dfs -ls -R /user/hive/warehouse/htables.db",
     "watch": ["Plus de répertoire personne, plus de personne.txt nulle part"]},
    {"id": "1.8", "part": "p1", "title": "Bonus : un CTAS ne peut pas être externe",
     "text": "Sous Hive 1.x, CREATE EXTERNAL TABLE ... AS SELECT est refusé. Lisez le message d'erreur.",
     "sql": "CREATE EXTERNAL TABLE copie_population AS\nSELECT * FROM emploi_population;",
     "watch": ["Error 10070 : CREATE-TABLE-AS-SELECT cannot create external table"], "expect_error": True},
    # ------------------------------------------------------------ Partie 2
    {"id": "2.1", "part": "p2", "title": "Partition dynamique : le mode strict bloque",
     "text": "On crée la table partitionnée par sexe. Sans changer la configuration, l'INSERT est refusé : "
             "par défaut, Hive exige au moins une partition statique.",
     "sql": "CREATE EXTERNAL TABLE population_genre (\n  age INT,\n  fonction STRING,\n  salaire STRING,\n"
            "  code_postal STRING,\n  id INT\n)\nPARTITIONED BY (sexe STRING)\nROW FORMAT DELIMITED\n"
            "FIELDS TERMINATED BY ',';\n\nINSERT OVERWRITE TABLE population_genre\nPARTITION (sexe)\n"
            "SELECT age, fonction, salaire, code_postal, id_empl, sexe FROM emploi_population;",
     "watch": ["Error 10096 : Dynamic partition strict mode..."], "expect_error": True},
    {"id": "2.2", "part": "p2", "title": "Partition dynamique : nonstrict, puis INSERT",
     "text": "La colonne de partition vient en DERNIER dans le SELECT. Chaque valeur de sexe devient un répertoire.",
     "sql": "SET hive.exec.dynamic.partition=true;\nSET hive.exec.dynamic.partition.mode=nonstrict;\n\n"
            "INSERT OVERWRITE TABLE population_genre\nPARTITION (sexe)\n"
            "SELECT age, fonction, salaire, code_postal, id_empl, sexe FROM emploi_population;\n\n"
            "SHOW PARTITIONS population_genre;\nhdfs dfs -ls -R /user/hive/warehouse/htables.db/population_genre",
     "watch": ["Deux répertoires sexe=F et sexe=M", "Les fichiers ne contiennent plus la colonne sexe",
               "Séparateur « , » dans ces fichiers"]},
    {"id": "2.3", "part": "p2", "title": "Mesurer l'intérêt de la partition",
     "text": "Même requête sur les deux tables. Regardez la ligne d'information sous chaque résultat : "
             "sur la table partitionnée, Hive ne lit qu'un répertoire. Sur la VM, l'écart était d'environ 11 s contre moins d'1 s.",
     "sql": "SELECT * FROM emploi_population WHERE sexe = 'F';\nSELECT * FROM population_genre WHERE sexe = 'F';",
     "watch": ["Même résultat (6 lignes)", "« Hive ne lirait que 1 fichier(s) sur 2 » pour population_genre"]},
    {"id": "2.4", "part": "p2", "title": "Staging des internautes dans un répertoire dédié",
     "text": "Le fichier va dans /user/hive/data/internautes, pas dans /user/hive/data : sinon log_emploi_population le lirait aussi.",
     "sql": "hdfs dfs -mkdir /user/hive/data/internautes\nhdfs dfs -chmod 777 /user/hive/data/internautes\n"
            "hdfs dfs -copyFromLocal /home/cloudera/Desktop/internautes_log.txt /user/hive/data/internautes\n\n"
            "CREATE EXTERNAL TABLE log_internautes (\n  id_internaute INT,\n  prenom STRING,\n  site_consulte STRING,\n"
            "  date_consultation STRING,\n  pays STRING\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\n"
            "STORED AS TEXTFILE\nLOCATION '/user/hive/data/internautes';\n\n"
            "SELECT date_consultation, COUNT(*) AS nb FROM log_internautes GROUP BY date_consultation;",
     "watch": ["17 lignes, 3 dates distinctes"]},
    {"id": "2.5", "part": "p2", "title": "Le piège « Not a file »",
     "text": "/user/hive/data contient maintenant un sous-répertoire. Relire log_emploi_population échoue, comme sur la VM. "
             "La parade du forum fonctionne... mais la table lit alors aussi les fichiers des sous-répertoires : comptez les lignes.",
     "sql": "SELECT COUNT(*) FROM log_emploi_population;",
     "sql2": "SET mapred.input.dir.recursive=true;\nSET hive.mapred.supports.subdirectories=true;\n"
             "SELECT COUNT(*) FROM log_emploi_population;\nSELECT genre, fonction, id FROM log_emploi_population LIMIT 15;\n\n"
             "-- on remet la configuration par défaut avant la suite\nSET mapred.input.dir.recursive=false;\n"
             "SET hive.mapred.supports.subdirectories=false;",
     "watch": ["Erreur Not a file au 1er essai", "29 lignes au lieu de 12 après la parade : la table est polluée"],
     "expect_error": True},
    {"id": "2.6", "part": "p2", "title": "Partitionner par date et répartir en 4 buckets",
     "text": "Pour un INT, hash(id) = id : le bucket vaut id % 4. Le 27-04-2016, les ids 1001, 1009, 1012 et 1013 "
             "ne remplissent que les buckets 0 et 1 : les deux autres fichiers restent vides.",
     "sql": "SET hive.exec.dynamic.partition=true;\nSET hive.exec.dynamic.partition.mode=nonstrict;\n"
            "SET hive.conf.validation=false;\nSET hive.enforce.bucketing=true;\n\n"
            "CREATE TABLE log_internautes_buckets (\n  id_internaute INT,\n  prenom STRING,\n  site_consulte STRING,\n"
            "  pays STRING\n)\nPARTITIONED BY (date_consultation STRING)\nCLUSTERED BY (id_internaute) INTO 4 BUCKETS\n"
            "ROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE;\n\n"
            "INSERT OVERWRITE TABLE log_internautes_buckets PARTITION (date_consultation)\n"
            "SELECT id_internaute, prenom, site_consulte, pays, date_consultation FROM log_internautes;\n\n"
            "DESCRIBE FORMATTED log_internautes_buckets;\n"
            "hdfs dfs -ls /user/hive/warehouse/htables.db/log_internautes_buckets/date_consultation=27-04-2016",
     "watch": ["Num Buckets: 4", "3 partitions × 4 fichiers = 12 fichiers", "Deux fichiers de 0 octet le 27-04-2016"]},
    {"id": "2.7", "part": "p2", "title": "Buckets sans partition, sur le pays",
     "text": "6 pays pour 4 buckets : certains fichiers contiennent plusieurs pays. La colonne virtuelle INPUT__FILE__NAME "
             "de Hive dit dans quel fichier se trouve chaque ligne.",
     "sql": "CREATE TABLE log_internautes_buckets2 (\n  id_internaute INT,\n  prenom STRING,\n  site_consulte STRING,\n"
            "  date_consultation STRING,\n  pays STRING\n)\nCLUSTERED BY (pays) INTO 4 BUCKETS\nROW FORMAT DELIMITED\n"
            "FIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE;\n\nINSERT OVERWRITE TABLE log_internautes_buckets2\n"
            "SELECT id_internaute, prenom, site_consulte, date_consultation, pays FROM log_internautes;\n\n"
            "SELECT INPUT__FILE__NAME AS fichier, COLLECT_SET(pays) AS pays\nFROM log_internautes_buckets2\n"
            "GROUP BY INPUT__FILE__NAME;",
     "watch": ["Chaque pays est dans un seul fichier", "Un même fichier peut contenir plusieurs pays"]},
    # ------------------------------------------------------------ Partie 3 : mise en place
    {"id": "3.0", "part": "p3", "title": "Mise en place de la base analyse_salaires",
     "text": "Les fichiers fournis s'appellent .txt (le sujet écrit .csv : le mini-Hive accepte les deux). "
             "fonction_employes.txt et salaire_employes.txt ont une ligne d'en-tête, employes.txt non.",
     "sql": "CREATE DATABASE analyse_salaires;\nUSE analyse_salaires;\n\n"
            "hdfs dfs -mkdir /user/hive/data/employes\nhdfs dfs -mkdir /user/hive/data/salaire\n"
            "hdfs dfs -mkdir /user/hive/data/fonction\nhdfs dfs -chmod 777 /user/hive/data/employes\n"
            "hadoop fs -copyFromLocal /home/cloudera/employes.txt /user/hive/data/employes/\n"
            "hadoop fs -copyFromLocal /home/cloudera/salaire_employes.txt /user/hive/data/salaire/\n"
            "hadoop fs -copyFromLocal /home/cloudera/fonction_employes.txt /user/hive/data/fonction/\n\n"
            "CREATE EXTERNAL TABLE salaire_employes (\n  sexe STRING,\n  secteur STRING,\n  salaire STRING,\n"
            "  code_postal INT,\n  id_empl INT\n)\nROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE\n"
            "LOCATION '/user/hive/data/salaire'\nTBLPROPERTIES (\"skip.header.line.count\"=\"1\");\n\n"
            "CREATE EXTERNAL TABLE fonction_employes (\n  id_empl INT,\n  annee_embauche INT,\n  fonction STRING\n)\n"
            "ROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE\nLOCATION '/user/hive/data/fonction'\n"
            "TBLPROPERTIES (\"skip.header.line.count\"=\"1\");\n\n"
            "CREATE EXTERNAL TABLE employes (\n  id_empl INT,\n  nom STRING,\n  prenom STRING,\n  date_naissance STRING\n)\n"
            "ROW FORMAT DELIMITED\nFIELDS TERMINATED BY ';'\nSTORED AS TEXTFILE\nLOCATION '/user/hive/data/employes';\n\n"
            "SELECT COUNT(*) FROM employes;\nSELECT COUNT(*) FROM fonction_employes;\nSELECT COUNT(*) FROM salaire_employes;\n"
            "SELECT DISTINCT secteur FROM salaire_employes;",
     "watch": ["33 lignes dans chaque table", "Secteurs en minuscules, « priv� » : accent Latin-1 lu en UTF-8"]},
]

QUESTIONS = [
    {
        "n": 1,
        "title": "Combien d'embauches après 2019 ?",
        "question": "Combien d'employés ont été embauchés après 2019 ?",
        "tables": ["fonction_employes"],
        "notions": ["COUNT()", "WHERE"],
        "hint": ["Une seule table suffit.", "« Après 2019 » exclut 2019."]
    },
    {
        "n": 2,
        "title": "Qui gagne plus de 35 000 € par an ?",
        "question": "Quelles paires employé–secteur ont un salaire supérieur à 35 000 euros par an ?",
        "tables": ["employes", "salaire_employes"],
        "notions": ["JOIN", "REGEXP_REPLACE", "CAST"],
        "hint": ["salaire vaut par exemple '20000euro/an' : c'est du texte.", "Extraire les chiffres, CAST en INT, comparer."]
    },
    {
        "n": 3,
        "title": "Employés de l'éducation recrutés après 2010",
        "question": "Listez les employés du secteur éducation recrutés après 2010.",
        "tables": ["employes", "fonction_employes", "salaire_employes"],
        "notions": ["CREATE VIEW", "deux jointures"],
        "hint": ["Une vue vue1 = employes ⋈ fonction_employes, embauchés après 2010.", "Vérifiez l'orthographe exacte avec SELECT DISTINCT secteur."]
    },
    {
        "n": 4,
        "title": "Qui a le plus d'expérience ?",
        "question": "Quel employé a le plus d'expérience professionnelle ?",
        "tables": ["employes", "fonction_employes"],
        "notions": ["ORDER BY … DESC", "LIMIT"],
        "hint": ["Expérience = YEAR(CURRENT_DATE()) − annee_embauche.", "Trier puis garder la première ligne."]
    },
    {
        "n": 5,
        "title": "Quel est le salaire moyen ?",
        "question": "Quel est le salaire moyen des employés ?",
        "tables": ["salaire_employes"],
        "notions": ["AVG()", "REGEXP_REPLACE", "CAST(… AS DECIMAL)"],
        "hint": ["AVG() attend un nombre.", "Ne garder que les chiffres du salaire."]
    },
    {
        "n": 6,
        "title": "Fonctions des femmes par année d'embauche",
        "question": "Quelles sont les fonctions des employées femmes, de l'embauche la plus récente à la plus ancienne ?",
        "tables": ["fonction_employes", "salaire_employes"],
        "notions": ["JOIN", "WHERE sexe = 'F'", "ORDER BY DESC"],
        "hint": ["Le sexe n'est pas dans la même table que la fonction.", "Préfixez id_empl : il existe dans les deux tables."]
    },
    {
        "n": 7,
        "title": "Qui réside à Paris ?",
        "question": "Listez les paires ‹prénom, fonction› des employés qui résident à Paris (code postal 75000).",
        "tables": ["employes", "fonction_employes", "salaire_employes"],
        "notions": ["CREATE VIEW", "filtre code_postal"],
        "hint": ["Une vue vue4 = identité + fonction.", "Le code postal est dans salaire_employes."]
    },
    {
        "n": 8,
        "title": "Écart de salaire femmes / hommes",
        "question": "Y a-t-il une différence entre le salaire des femmes et celui des hommes ? Comparez le ratio somme des salaires ÷ somme des années d'expérience.",
        "tables": ["salaire_employes", "fonction_employes"],
        "notions": ["sous-requête", "SUM(CASE WHEN …)"],
        "hint": ["Nettoyer le salaire une fois dans une sous-requête.", "Puis un SUM(CASE WHEN sexe = 'F' …) par sexe."]
    },
    {
        "n": 9,
        "title": "L'expérience influence-t-elle le salaire ?",
        "question": "Le nombre d'années d'expérience influence-t-il le salaire ? Utilisez corr().",
        "tables": ["salaire_employes", "fonction_employes"],
        "notions": ["corr(X, Y)"],
        "hint": ["X = années d'expérience, Y = salaire numérique.", "r est compris entre −1 et +1."]
    },
    {
        "n": 10,
        "title": "Le secteur influence-t-il le salaire ?",
        "question": "Le secteur d'activité influence-t-il le salaire des employés ? Concluez.",
        "tables": ["salaire_employes", "fonction_employes"],
        "notions": ["GROUP BY", "AVG()", "COUNT()"],
        "hint": ["Salaire moyen par secteur, trié du plus haut au plus bas.", "Affichez aussi le nombre d'employés par secteur."]
    },
]


# ======================================================================
#  Les 10 questions en Python pur (sans SQL), pour comparer
# ======================================================================

def lire_fichier(chemin, colonnes, entete=False):
    """Lit un fichier « ; » comme Hive : chaque ligne devient un dictionnaire."""
    with open(chemin, "rb") as f:
        lignes = f.read().decode("utf-8", errors="replace").splitlines()
    if entete:
        lignes = lignes[1:]
    table_lue = []
    for ligne in lignes:
        champs = ligne.split(";")
        table_lue.append({col: (champs[i] if i < len(champs) else None) for i, col in enumerate(colonnes)})
    return table_lue


def salaire_en_euros(texte):
    """'20000euro/an' -> 20000 ; garde tous les chiffres, quel que soit le suffixe."""
    chiffres = re.sub(r"[^0-9]", "", texte or "")
    return int(chiffres) if chiffres else None


def charger_donnees(dossier):
    employes = lire_fichier(os.path.join(dossier, "employes.txt"), ["id_empl", "nom", "prenom", "date_naissance"])
    fonctions = lire_fichier(os.path.join(dossier, "fonction_employes.txt"), ["id_empl", "annee_embauche", "fonction"], entete=True)
    salaires = lire_fichier(os.path.join(dossier, "salaire_employes.txt"),
                            ["sexe", "secteur", "salaire", "code_postal", "id_empl"], entete=True)
    for e in employes:
        e["id_empl"] = int(e["id_empl"])
    for f in fonctions:
        f["id_empl"], f["annee_embauche"] = int(f["id_empl"]), int(f["annee_embauche"])
    for s in salaires:
        s["id_empl"], s["code_postal"] = int(s["id_empl"]), int(s["code_postal"])
        s["euros"] = salaire_en_euros(s["salaire"])
    return employes, fonctions, salaires



def _charger_corriges():
    """Les corrections vivent dans corriges.py, à côté du script, hors du dépôt public."""
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "corriges.py")
    if not os.path.isfile(p):
        return None
    import importlib.util
    spec = importlib.util.spec_from_file_location("corriges", p)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    for q in QUESTIONS:
        q.update(mod.CORRIGES.get(q["n"], {}))
    return mod


CORRIGES_MOD = _charger_corriges()
PY_FUNCS = CORRIGES_MOD.PY_FUNCS if CORRIGES_MOD else {}


def python_answer(data_dir, n):
    donnees = charger_donnees(data_dir)
    cols, rows = PY_FUNCS[n](*donnees)
    return {"kind": "table", "columns": cols, "rows": rows, "info": ["calculé en Python pur, sans SQL"],
            "source": inspect.getsource(PY_FUNCS[n]), "stmt": "python q%d" % n}


def python_helpers_source():
    return "\n\n".join(inspect.getsource(f) for f in (lire_fichier, salaire_en_euros, charger_donnees))

# ======================================================================
#  Rejouer, vérifier, console
# ======================================================================

def step_scripts(step):
    return [step["sql"]] + ([step["sql2"]] if step.get("sql2") else [])


def replay(engine, upto=None, out=None, include_questions=True):
    """Remet à zéro puis rejoue les étapes (jusqu'à l'étape `upto` exclue si elle est donnée)."""
    engine.reset()
    results = []
    for st in STEPS:
        if upto is not None and st["id"] == upto:
            return results
        for block in step_scripts(st):
            res = engine.run_script(block, stop_on_error=False)
            results.append((st, res))
            if out:
                out("\n" + "=" * 78 + "\n[%s] %s\n" % (st["id"], st["title"]) + "=" * 78)
                for r in res:
                    out(render_result(r))
    if include_questions and upto is None and CORRIGES_MOD:
        for q in QUESTIONS:
            for label, key in (("solution", "solution"), ("corrigé du sujet", "subject")):
                res = engine.run_script(q[key], stop_on_error=False)
                results.append((q, res))
                if out:
                    out("\n" + "=" * 78 + "\n[Q%d · %s] %s\n" % (q["n"], label, q["title"]) + "=" * 78)
                    for r in res:
                        out(render_result(r))
            if out:
                out("Attendu : " + q["expected"])
    return results


def verify(engine, data_dir, out=print):
    """Rejoue tout et contrôle les points clés du sujet. Renvoie le nombre d'échecs."""
    engine.reset()
    fails = []

    def check(label, cond):
        out(("  OK   " if cond else "  ÉCHEC ") + label)
        if not cond:
            fails.append(label)

    def run(sql):
        return engine.run_script(sql, stop_on_error=False)

    def last_rows(sql):
        r = run(sql)[-1]
        return r.get("rows", []), r

    by_id = {s["id"]: s for s in STEPS}
    for sid in ("1.1", "1.2", "1.3"):
        run(by_id[sid]["sql"])
    rows, _ = last_rows("SELECT COUNT(*) FROM log_emploi_population;")
    check("log_emploi_population : 12 lignes", rows == [[12]])
    run(by_id["1.4"]["sql"])
    check("DROP d'une table externe : le fichier reste", os.path.exists(engine.real("/user/hive/data/emploi_population.txt")))
    run(by_id["1.5"]["sql"])
    rows, _ = last_rows("SELECT sexe, COUNT(*) FROM emploi_population GROUP BY sexe ORDER BY sexe;")
    check("CTAS emploi_population : 6 F et 6 M", rows == [["F", 6], ["M", 6]])
    rows, _ = last_rows("SELECT age FROM emploi_population WHERE id_empl = 1;")
    check("CTAS : age calculé (DOUBLE)", bool(rows) and isinstance(rows[0][0], float) and 25 < rows[0][0] < 60)
    run(by_id["1.6"]["sql"])
    check("LOAD DATA INPATH déplace le fichier", not os.path.exists(engine.real("/user/hive/warehouse/personne.txt"))
          and os.path.exists(engine.real("/user/hive/warehouse/htables.db/personne/personne.txt")))
    rows, _ = last_rows("SELECT COUNT(*) FROM personne;")
    check("personne : 12 lignes", rows == [[12]])
    run(by_id["1.7"]["sql"])
    check("DROP d'une table interne efface les données", not os.path.exists(engine.real("/user/hive/warehouse/htables.db/personne")))
    r = run(by_id["1.8"]["sql"])[-1]
    check("CTAS externe refusé (Error 10070)", r["kind"] == "error" and "10070" in r["message"])
    r = run(by_id["2.1"]["sql"])[-1]
    check("Mode strict : INSERT dynamique refusé (Error 10096)", r["kind"] == "error" and "10096" in r["message"])
    run(by_id["2.2"]["sql"])
    rows, _ = last_rows("SHOW PARTITIONS population_genre;")
    check("Partitions sexe=F et sexe=M", rows == [["sexe=F"], ["sexe=M"]])
    r = run("SELECT * FROM population_genre WHERE sexe = 'F';")[-1]
    check("Élagage : 1 fichier sur 2 pour sexe = 'F'", len(r.get("rows", [])) == 6 and any("1 fichier(s) sur 2" in i for i in r["info"]))
    run(by_id["2.4"]["sql"])
    rows, _ = last_rows("SELECT COUNT(*) FROM log_internautes;")
    check("log_internautes : 17 lignes", rows == [[17]])
    r = run(by_id["2.5"]["sql"])[-1]
    check("« Not a file » sur log_emploi_population", r["kind"] == "error" and "Not a file" in r["message"])
    rows, _ = last_rows(by_id["2.5"]["sql2"].split(";")[0] + ";" + by_id["2.5"]["sql2"].split(";")[1] + ";"
                        "SELECT COUNT(*) FROM log_emploi_population;")
    check("Après la parade, la table est polluée : 29 lignes", rows == [[29]])
    run("SET mapred.input.dir.recursive=false;")
    run(by_id["2.6"]["sql"])
    base = engine.real("/user/hive/warehouse/htables.db/log_internautes_buckets")
    sizes = {}
    for d in sorted(os.listdir(base)):
        sizes[d] = [os.path.getsize(os.path.join(base, d, f)) for f in sorted(os.listdir(os.path.join(base, d)))]
    check("Buckets : 3 partitions × 4 fichiers", len(sizes) == 3 and all(len(v) == 4 for v in sizes.values()))
    check("27-04-2016 : buckets 2 et 3 vides", sizes.get("date_consultation=27-04-2016", [1, 1, 1, 1])[2:] == [0, 0]
          and all(s > 0 for s in sizes.get("date_consultation=27-04-2016", [0, 0])[:2]))
    r = run(by_id["2.6"]["sql"].split("DESCRIBE")[0] + "DESCRIBE FORMATTED log_internautes_buckets;")[-1]
    check("DESCRIBE FORMATTED : Num Buckets 4", any(row[0] == "Num Buckets:" and row[1] == "4" for row in r.get("rows", [])))
    r = run(by_id["2.7"]["sql"])[-1]
    pays = sorted(p for row in r.get("rows", []) for p in json.loads(row[1]))
    check("Buckets par pays : 6 pays répartis, chacun dans un seul fichier", len(pays) == 6 and len(r["rows"]) <= 4)
    r = run(by_id["3.0"]["sql"])
    counts = [x["rows"] for x in r if x["kind"] == "table" and x["columns"] and x["columns"][0] == "_c0"]
    check("analyse_salaires : 33 lignes dans chaque table", counts == [[[33]], [[33]], [[33]]])

    if CORRIGES_MOD:
        CORRIGES_MOD.verifier_questions(run, check, QUESTIONS, charger_donnees(data_dir))
    else:
        out("\ncorriges.py absent : les contrôles des questions Q1 à Q10 sont ignorés.")
    out("\n%d contrôle(s) en échec" % len(fails) if fails else "\nTous les contrôles passent.")
    return len(fails)


def console(engine):
    print("Mini-Hive · tapez vos instructions HiveQL terminées par « ; » (quit; pour sortir)")
    print("Base courante : %s · données locales : %s\n" % (engine.db, engine.data_dir))
    buf = []
    while True:
        try:
            prompt = ("hive (%s)> " % engine.db) if engine.conf.get("hive.cli.print.current.db") == "true" else "hive> "
            line = input("    > " if buf else prompt)
        except (EOFError, KeyboardInterrupt):
            print()
            break
        if not buf and line.strip().lower().rstrip(";") in ("quit", "exit"):
            break
        if not buf and _DFS_RE.match(line):
            print(render_result(engine.execute(line.strip().rstrip(";"))))
            continue
        buf.append(line)
        if line.rstrip().endswith(";"):
            for r in engine.run_script("\n".join(buf)):
                print(render_result(r))
            buf = []


# ======================================================================
#  Serveur web
# ======================================================================

import http.server
import socketserver
import urllib.parse


def interface_html():
    """interface/index.html s'il existe à côté du script (modifiable à chaud), sinon la copie intégrée."""
    p = os.path.join(os.path.dirname(os.path.abspath(__file__)), "interface", "index.html")
    if os.path.isfile(p):
        with open(p, encoding="utf-8") as fh:
            return fh.read()
    return INDEX_HTML


def display(r):
    """Formate les valeurs comme Hive les affiche (NULL, 38.0, décimales lisibles)."""
    if r.get("kind") == "table":
        r = dict(r, rows=[[None if v is None else fmt_value(v) for v in row] for row in r["rows"]])
    return r


def make_handler(engine, student_mode):
    class Handler(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _send(self, code, body, ctype="application/json; charset=utf-8"):
            data = body if isinstance(body, bytes) else body.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(data)

        def _json(self, obj, code=200):
            self._send(code, json.dumps(obj, ensure_ascii=False, default=str))

        def _body(self):
            n = int(self.headers.get("Content-Length") or 0)
            if not n:
                return {}
            try:
                return json.loads(self.rfile.read(n).decode("utf-8"))
            except ValueError:
                return {}

        def do_GET(self):
            u = urllib.parse.urlparse(self.path)
            q = urllib.parse.parse_qs(u.query)
            if u.path in ("/", "/index.html"):
                return self._send(200, interface_html(), "text/html; charset=utf-8")
            if u.path == "/api/state":
                return self._json({"hdfs": engine.hdfs_tree(), "meta": engine.metastore_view()})
            if u.path == "/api/content":
                qs = []
                for qq in QUESTIONS:
                    d = dict(qq)
                    if student_mode:
                        for k in ("solution", "subject", "expected"):
                            d.pop(k, None)
                    qs.append(d)
                return self._json({"parts": PARTS, "steps": STEPS, "questions": qs, "student": student_mode,
                                   "data_dir": engine.data_dir, "hdfs_dir": engine.hdfs,
                                   "helpers": "" if student_mode else python_helpers_source(),
                                   "local_files": sorted(f for f in os.listdir(engine.data_dir)
                                                         if os.path.isfile(os.path.join(engine.data_dir, f)))
                                   if os.path.isdir(engine.data_dir) else []})
            if u.path == "/api/cat":
                try:
                    return self._json(engine.cat(q.get("path", ["/"])[0]))
                except HiveError as e:
                    return self._json({"error": str(e)}, 404)
            if u.path == "/api/local":
                name = os.path.basename(q.get("name", [""])[0])
                p = os.path.join(engine.data_dir, name)
                if not os.path.isfile(p):
                    return self._json({"error": "fichier introuvable"}, 404)
                with open(p, "rb") as fh:
                    raw = fh.read(200000)
                txt = raw.decode("utf-8", errors="replace")
                return self._json({"name": name, "size": os.path.getsize(p), "lines": txt.splitlines()[:400],
                                   "crlf": b"\r\n" in raw})
            return self._send(404, "introuvable", "text/plain; charset=utf-8")

        def do_POST(self):
            u = urllib.parse.urlparse(self.path)
            b = self._body()
            if u.path == "/api/run":
                res = engine.run_script(b.get("sql", ""), stop_on_error=True)
                return self._json({"results": [display(r) for r in res]})
            if u.path == "/api/reset":
                engine.reset()
                return self._json({"ok": True})
            if u.path == "/api/replay":
                replay(engine, upto=b.get("upto"), include_questions=False)
                return self._json({"ok": True, "db": engine.db})
            if u.path == "/api/python":
                if student_mode:
                    return self._json({"error": "corrections masquées en mode élèves"}, 403)
                try:
                    return self._json(display(python_answer(engine.data_dir, int(b.get("n", 0)))))
                except (KeyError, ValueError, OSError) as e:
                    return self._json({"kind": "error", "message": "Erreur Python : %s" % e, "stmt": "python"})
            return self._send(404, "introuvable", "text/plain; charset=utf-8")

    return Handler


class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True


def serve(engine, port, student_mode, open_browser):
    if not CORRIGES_MOD and not student_mode:
        print("corriges.py absent : interface en mode élèves (corrections non disponibles).")
        student_mode = True
    httpd = None
    for p in range(port, port + 20):
        try:
            httpd = _Server(("127.0.0.1", p), make_handler(engine, student_mode))
            port = p
            break
        except OSError:
            continue
    if httpd is None:
        sys.exit("Impossible d'ouvrir un port entre %d et %d" % (port, port + 19))
    url = "http://localhost:%d" % port
    print("Mini-Hive · interface ouverte sur %s%s" % (url, "  (mode élèves : corrections masquées)" if student_mode else ""))
    print("Données : %s\nHDFS simulé : %s\nCtrl+C pour arrêter." % (engine.data_dir, engine.hdfs))
    if open_browser:
        threading.Timer(0.6, lambda: __import__("webbrowser").open(url)).start()
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nArrêt.")


def main(argv=None):
    here = os.path.dirname(os.path.abspath(__file__))
    default_data = os.path.join(here, "donnees")
    for cand in (os.path.join(os.getcwd(), "donnees"), os.path.join(os.path.dirname(here), "donnees")):
        if not os.path.isdir(default_data) and os.path.isdir(cand):
            default_data = cand
    ap = argparse.ArgumentParser(description="Workshop Hive rejoué en Python pur (CESI · Big Data · FISA A4)")
    ap.add_argument("commande", nargs="?", default="serve",
                    choices=["serve", "rejouer", "console", "verifier", "reset", "sql"],
                    help="serve (défaut) · rejouer · console · verifier · reset · sql \"...\"")
    ap.add_argument("requete", nargs="?", help="pour la commande sql : instructions HiveQL")
    ap.add_argument("--donnees", default=default_data, help="dossier des fichiers fournis (défaut : ./donnees)")
    ap.add_argument("--hdfs", default=os.path.join(here, "mini_hdfs"), help="dossier du HDFS simulé (défaut : ./mini_hdfs)")
    ap.add_argument("--port", type=int, default=8765)
    ap.add_argument("--sans-corriges", action="store_true", help="interface élèves : masque corrections et résultats attendus")
    ap.add_argument("--sans-navigateur", action="store_true", help="ne pas ouvrir le navigateur automatiquement")
    a = ap.parse_args(argv)
    if hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except (ValueError, OSError):
            pass
    if not os.path.isdir(a.donnees):
        sys.exit("Dossier de données introuvable : %s (utilisez --donnees)" % a.donnees)
    engine = MiniHive(a.donnees, a.hdfs)
    if a.commande == "serve":
        serve(engine, a.port, a.sans_corriges, not a.sans_navigateur)
    elif a.commande == "rejouer":
        replay(engine, out=print)
    elif a.commande == "console":
        console(engine)
    elif a.commande == "verifier":
        sys.exit(1 if verify(engine, a.donnees) else 0)
    elif a.commande == "reset":
        engine.reset()
        print("HDFS simulé et Metastore remis à zéro (%s)." % engine.root)
    elif a.commande == "sql":
        for r in engine.run_script(a.requete or ""):
            print(render_result(r))

INDEX_HTML = r"""<!doctype html>
<html lang="fr">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Mini-Hive · Workshop Hive</title>
<style>
:root{
  --bg:#f6f5f1; --panel:#ffffff; --soft:#efede6; --line:#e2dfd5; --ink:#1d1d1b; --ink2:#4a4a46; --grey:#7a7a74;
  --yellow:#fee131; --yellow-soft:#fff6c2; --red:#c2410c; --red-soft:#fdece3; --teal:#0f766e; --teal-soft:#e3f3f0;
  --blue:#1d4ed8; --code-bg:#1e1f22; --code-ink:#e8e6e3; --mono:ui-monospace,SFMono-Regular,Menlo,Consolas,"Liberation Mono",monospace;
}
@media (prefers-color-scheme: dark){
  :root{ --bg:#141413; --panel:#1d1d1b; --soft:#262624; --line:#34342f; --ink:#f1efe8; --ink2:#c9c6bc; --grey:#9a978d;
    --yellow-soft:#3a3412; --red-soft:#3a1e14; --teal-soft:#12302c; --code-bg:#0f0f0e; }
}
*{box-sizing:border-box}
html,body{margin:0;height:100%}
body{background:var(--bg);color:var(--ink);font:15px/1.5 system-ui,-apple-system,"Segoe UI",Roboto,Arial,sans-serif}
button{font:inherit;cursor:pointer}
#toggleAside,#toggleNav{display:none}
header{height:56px;display:flex;align-items:center;gap:14px;padding:0 18px;background:#1d1d1b;color:#fff;position:sticky;top:0;z-index:5}
header .logo{display:flex;align-items:center;gap:10px;font-weight:700;letter-spacing:.2px}
header .logo span.b{background:var(--yellow);color:#1d1d1b;border-radius:6px;padding:2px 8px;font-size:13px}
header .sp{flex:1}
header .db{font-family:var(--mono);font-size:13px;background:#2c2c29;border-radius:999px;padding:4px 12px}
header .db b{color:var(--yellow)}
header .mode{font-size:12px;border:1px solid #555;border-radius:999px;padding:3px 10px;color:#ddd}
.hbtn{background:transparent;border:1px solid #555;color:#eee;border-radius:8px;padding:5px 12px;font-size:13px}
.hbtn:hover{border-color:var(--yellow);color:var(--yellow)}
.layout{display:grid;grid-template-columns:250px minmax(0,1fr) 340px;height:calc(100vh - 56px)}
nav{border-right:1px solid var(--line);overflow:auto;padding:14px 10px 40px;background:var(--panel)}
nav h4{font-size:11px;letter-spacing:.8px;text-transform:uppercase;color:var(--grey);margin:16px 8px 6px}
nav a{display:flex;gap:8px;align-items:baseline;padding:6px 8px;border-radius:8px;color:var(--ink2);text-decoration:none;font-size:14px}
nav a:hover{background:var(--soft)}
nav a.on{background:var(--yellow);color:#1d1d1b;font-weight:600}
nav a .id{font-family:var(--mono);font-size:12px;color:var(--grey);min-width:30px}
nav a.on .id{color:#1d1d1b}
nav a .ok{margin-left:auto;color:var(--teal);font-size:12px}
main{overflow:auto;padding:26px 34px 80px}
aside{border-left:1px solid var(--line);overflow:auto;background:var(--panel);display:flex;flex-direction:column}
.kicker{display:inline-block;background:var(--yellow);color:#1d1d1b;font-size:11px;font-weight:700;letter-spacing:.8px;border-radius:999px;padding:3px 12px;text-transform:uppercase}
.kicker.dark{background:#1d1d1b;color:var(--yellow)}
h1{font-size:28px;line-height:1.2;margin:12px 0 10px}
.lead{color:var(--ink2);max-width:860px;margin:0 0 14px}
.watch{background:var(--teal-soft);border-radius:12px;padding:10px 16px;margin:14px 0;max-width:860px}
.watch b{font-size:13px;color:var(--teal)}
.watch ul{margin:4px 0 2px 18px;padding:0}
.cards{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px;margin:16px 0}
.card{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:12px 14px}
.card h5{margin:0 0 6px;font-size:13px}
.card ul{margin:0;padding-left:18px}
.card.hint{background:var(--yellow-soft);border-color:transparent}
.card.hint .blur{filter:blur(5px);cursor:pointer;transition:filter .2s}
.card.hint .blur.show{filter:none}
.qbig{display:flex;gap:16px;align-items:center;margin:8px 0 6px}
.qbig .q{width:64px;height:64px;border-radius:50%;background:var(--yellow);color:#1d1d1b;display:grid;place-items:center;font-weight:800;font-size:22px;flex:none}
.qbig p{font-size:20px;margin:0}
.editor{margin:16px 0 8px;border-radius:12px;overflow:hidden;border:1px solid var(--line);background:var(--code-bg)}
.editor .bar{display:flex;gap:8px;align-items:center;padding:8px 10px;background:#2a2b2e;color:#bbb;font-size:12px;flex-wrap:wrap}
.editor .bar .t{font-family:var(--mono);margin-right:auto}
textarea{display:block;width:100%;min-height:120px;resize:vertical;border:0;outline:0;background:var(--code-bg);color:var(--code-ink);font:14px/1.55 var(--mono);padding:14px 16px;tab-size:2}
.btn{border:0;border-radius:8px;padding:6px 14px;font-size:13px;font-weight:600}
.btn.run{background:var(--yellow);color:#1d1d1b}
.btn.ghost{background:transparent;color:#ddd;border:1px solid #555}
.btn.ghost:hover{border-color:var(--yellow);color:var(--yellow)}
.btn.light{background:var(--soft);color:var(--ink);border:1px solid var(--line)}
.row{display:flex;gap:10px;flex-wrap:wrap;align-items:center;margin:10px 0}
.expected{background:var(--teal-soft);border-left:4px solid var(--teal);border-radius:8px;padding:10px 14px;margin:10px 0;max-width:900px}
.expected.subject{background:var(--red-soft);border-left-color:var(--red)}
.results{display:flex;flex-direction:column;gap:12px;margin-top:12px}
.res{background:var(--panel);border:1px solid var(--line);border-radius:12px;overflow:hidden}
.res .h{font-family:var(--mono);font-size:12px;color:var(--grey);padding:7px 12px;background:var(--soft);display:flex;gap:10px;white-space:nowrap;overflow:hidden}
.res .h span.s{overflow:hidden;text-overflow:ellipsis;flex:1}
.res pre{margin:0;padding:10px 14px;font:13px/1.5 var(--mono);white-space:pre-wrap;word-break:break-word}
.res.err{border-color:var(--red)}
.res.err pre{background:var(--red-soft);color:var(--red)}
.res .info{padding:6px 12px 8px;font-size:12px;color:var(--teal);border-top:1px dashed var(--line)}
.tbl{overflow:auto;max-height:440px}
table.t{border-collapse:collapse;font:13px/1.4 var(--mono);width:max-content;min-width:100%}
table.t th{position:sticky;top:0;background:var(--panel);text-align:left;padding:6px 12px;border-bottom:2px solid var(--yellow);font-weight:700}
table.t td{padding:5px 12px;border-bottom:1px solid var(--line);white-space:pre}
table.t td.null{color:var(--grey);font-style:italic}
table.t td.num{text-align:right}
.count{font-size:12px;color:var(--grey);padding:6px 12px}
.tabs{display:flex;border-bottom:1px solid var(--line);position:sticky;top:0;background:var(--panel);z-index:2}
.tabs button{flex:1;background:none;border:0;padding:11px 6px;font-size:13px;font-weight:600;color:var(--grey);border-bottom:3px solid transparent}
.tabs button.on{color:var(--ink);border-bottom-color:var(--yellow)}
.pane{padding:12px 12px 40px;font-size:13px}
.tree{font-family:var(--mono);font-size:12.5px}
.tree ul{list-style:none;margin:0;padding-left:14px;border-left:1px dotted var(--line)}
.tree>ul{padding-left:0;border:0}
.tree li{margin:1px 0}
.tree .n{display:flex;gap:6px;align-items:center;padding:2px 4px;border-radius:5px;cursor:pointer;white-space:nowrap}
.tree .n:hover{background:var(--soft)}
.tree .n .sz{margin-left:auto;color:var(--grey);font-size:11px}
.tree .n .sz.zero{color:var(--red);font-weight:700}
.tree .n.new{background:var(--yellow-soft)}
.tree .ic{width:14px;text-align:center;color:var(--grey)}
.diff{background:var(--soft);border-radius:8px;padding:8px 10px;margin-bottom:10px;font-family:var(--mono);font-size:11.5px;word-break:break-all}
.diff .a{color:var(--teal)} .diff .d{color:var(--red)}
.meta .db{margin:0 0 14px}
.meta .db h6{font-size:13px;margin:0 0 6px;font-family:var(--mono)}
.meta .db h6 .cur{background:var(--yellow);color:#1d1d1b;border-radius:999px;font-size:10px;padding:1px 7px;margin-left:6px;font-family:system-ui}
.meta .tb{border:1px solid var(--line);border-radius:10px;padding:8px 10px;margin:6px 0}
.meta .tb .nm{font-family:var(--mono);font-weight:700}
.badge{font-size:10px;font-weight:700;border-radius:999px;padding:1px 7px;margin-left:6px;letter-spacing:.4px}
.badge.ext{background:var(--teal-soft);color:var(--teal)} .badge.int{background:var(--yellow-soft);color:#7a6400}
.badge.vue{background:var(--soft);color:var(--grey)}
@media (prefers-color-scheme: dark){ .badge.int{color:var(--yellow)} }
.meta .tb .loc,.meta .tb .cols{font-family:var(--mono);font-size:11px;color:var(--grey);word-break:break-all}
.conf div{display:flex;justify-content:space-between;gap:8px;font-family:var(--mono);font-size:11.5px;padding:4px 0;border-bottom:1px solid var(--line)}
.conf b.t{color:var(--teal)} .conf b.f{color:var(--red)}
.modal{position:fixed;inset:0;background:rgba(0,0,0,.45);display:none;align-items:center;justify-content:center;z-index:20;padding:20px}
.modal.on{display:flex}
.modal .box{background:var(--panel);border-radius:14px;max-width:900px;width:100%;max-height:85vh;display:flex;flex-direction:column;overflow:hidden}
.modal .box .h{display:flex;align-items:center;gap:10px;padding:12px 16px;border-bottom:1px solid var(--line);font-family:var(--mono);font-size:13px}
.modal .box .h .x{margin-left:auto}
.modal pre{margin:0;padding:14px 16px;overflow:auto;font:13px/1.5 var(--mono)}
.home .grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:14px;margin:20px 0}
.home .grid .card{cursor:pointer}
.home .grid .card:hover{border-color:var(--yellow)}
.home .grid .card .n{font-size:30px;font-weight:800}
code{font-family:var(--mono);background:var(--soft);border-radius:4px;padding:0 4px;font-size:.92em}
pre.src{background:var(--code-bg);color:var(--code-ink);border-radius:12px;padding:14px 16px;font:13px/1.5 var(--mono);overflow:auto;max-width:100%}
.next{margin-top:26px}
.files .f{display:flex;gap:10px;align-items:center;padding:8px 12px;border:1px solid var(--line);border-radius:10px;margin:6px 0;cursor:pointer;background:var(--panel);max-width:640px}
.files .f:hover{border-color:var(--yellow)}
.files .f .sz{margin-left:auto;color:var(--grey);font-family:var(--mono);font-size:12px}
.toast{position:fixed;bottom:18px;left:50%;transform:translateX(-50%);background:#1d1d1b;color:#fff;border-radius:10px;padding:8px 16px;font-size:13px;display:none;z-index:30}
@media (max-width:1100px){ .layout{grid-template-columns:220px minmax(0,1fr)} aside{display:none} aside.on{display:flex;position:fixed;right:0;top:56px;bottom:0;width:340px;z-index:6;box-shadow:-6px 0 20px rgba(0,0,0,.2)} #toggleAside{display:inline-block} }
@media (max-width:760px){ .layout{grid-template-columns:1fr} nav{display:none} nav.on{display:block;position:fixed;left:0;top:56px;bottom:0;width:260px;z-index:6} #toggleNav{display:inline-block} main{padding:18px 16px 60px} .cards,.home .grid{grid-template-columns:1fr} header .db,header .mode,header .lt{display:none} header{gap:8px;padding:0 10px} }
</style>
</head>
<body>
<header>
  <button class="hbtn" id="toggleNav" title="Menu">☰</button>
  <div class="logo"><span class="b">HIVE</span> Mini-Hive<span class="lt"> · Workshop FISA A4</span></div>
  <div class="sp"></div>
  <span class="mode" id="mode"></span>
  <span class="db" id="dbBadge">hive (<b>default</b>)&gt;</span>
  <button class="hbtn" id="resetBtn" title="Vider le HDFS simulé et le Metastore">Réinitialiser</button>
  <button class="hbtn" id="toggleAside" title="HDFS et Metastore">HDFS</button>
</header>
<div class="layout">
  <nav id="nav"></nav>
  <main id="main"></main>
  <aside id="aside">
    <div class="tabs"><button data-t="hdfs" class="on">HDFS</button><button data-t="meta">Metastore</button><button data-t="conf">Config</button></div>
    <div class="pane" id="pane"></div>
  </aside>
</div>
<div class="modal" id="modal"><div class="box"><div class="h"><span id="mTitle"></span><button class="btn light x" id="mClose">Fermer</button></div><pre id="mBody"></pre></div></div>
<div class="toast" id="toast"></div>
<script>
const $ = (s, el=document) => el.querySelector(s);
const esc = s => String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
let C = null, STATE = null, PREV = new Set(), NEWP = new Set(), DIFF = null, TAB = "hdfs", ROUTE = "home";
const done = new Set();
const OPEN = new Set(["/", "/user", "/user/hive", "/user/hive/warehouse", "/user/hive/data"]);

async function api(path, body){
  const r = await fetch(path, body === undefined ? {} : {method:"POST", headers:{"Content-Type":"application/json"}, body:JSON.stringify(body)});
  return r.json();
}
function toast(t){ const el=$("#toast"); el.textContent=t; el.style.display="block"; clearTimeout(el._t); el._t=setTimeout(()=>el.style.display="none",2600); }

function fmt(v){
  if (v === null || v === undefined) return "NULL";
  return String(v);
}
function renderResult(r){
  const head = `<div class="h"><span>${esc(r.db||"")}&gt;</span><span class="s">${esc((r.stmt||"").replace(/\s+/g," "))}</span><span>${r.elapsed!==undefined? r.elapsed+" s":""}</span></div>`;
  let body = "";
  if (r.kind === "error") body = `<pre>${esc(r.message)}</pre>`;
  else if (r.kind === "table") {
    const rows = r.rows || [];
    body = `<div class="tbl"><table class="t"><thead><tr>${r.columns.map(c=>`<th>${esc(c)}</th>`).join("")}</tr></thead><tbody>` +
      rows.slice(0,500).map(row => "<tr>"+row.map(v => `<td class="${v===null?"null":(/^-?\d+(\.\d+)?$/.test(v)?"num":"")}">${esc(fmt(v))}</td>`).join("")+"</tr>").join("") +
      `</tbody></table></div><div class="count">${rows.length} ligne(s)${rows.length>500?" · 500 affichées":""}</div>`;
  } else body = `<pre>${esc(r.message)}</pre>`;
  const info = (r.info||[]).length ? `<div class="info">${r.info.map(i=>"· "+esc(i)).join("<br>")}</div>` : "";
  const src = r.source ? `<pre class="src" style="border-radius:0;margin:0">${esc(r.source)}</pre>` : "";
  return `<div class="res ${r.kind==="error"?"err":""}">${head}${src}${body}${info}</div>`;
}

async function run(sql, out, stepId){
  out.innerHTML = `<div class="count">Exécution…</div>`;
  const d = await api("/api/run", {sql});
  out.innerHTML = d.results.map(renderResult).join("") || `<div class="count">Rien à exécuter.</div>`;
  if (stepId && d.results.length && !d.results.some(r=>r.kind==="error")) done.add(stepId);
  await refresh(true);
  renderNav();
}

function flatPaths(n, acc){ acc.add(n.path + (n.dir?"/":"")); (n.children||[]).forEach(c=>flatPaths(c, acc)); return acc; }
async function refresh(trackDiff){
  STATE = await api("/api/state");
  const cur = flatPaths(STATE.hdfs, new Set());
  if (trackDiff){
    const added = [...cur].filter(p=>!PREV.has(p)), removed = [...PREV].filter(p=>!cur.has(p));
    NEWP = new Set(added); DIFF = (added.length||removed.length) ? {added, removed} : null;
    added.forEach(p => { const parts=p.replace(/\/$/,"").split("/"); for(let i=1;i<parts.length;i++) OPEN.add(parts.slice(0,i).join("/")||"/"); });
  }
  PREV = cur;
  $("#dbBadge").innerHTML = `hive (<b>${esc(STATE.meta.current)}</b>)&gt;`;
  renderAside();
}

function renderAside(){
  document.querySelectorAll(".tabs button").forEach(b=>b.classList.toggle("on", b.dataset.t===TAB));
  const pane = $("#pane");
  if (!STATE) return;
  if (TAB === "hdfs"){
    let diff = "";
    if (DIFF){
      const f = p => esc(p.replace(/\/$/,""));
      diff = `<div class="diff"><b>Dernière exécution</b><br>${DIFF.added.slice(0,8).map(p=>`<span class="a">+ ${f(p)}</span>`).join("<br>")}${DIFF.added.length>8?`<br>… +${DIFF.added.length-8}`:""}${DIFF.added.length&&DIFF.removed.length?"<br>":""}${DIFF.removed.slice(0,8).map(p=>`<span class="d">− ${f(p)}</span>`).join("<br>")}</div>`;
    }
    pane.innerHTML = diff + `<div class="tree"><ul>${treeNode(STATE.hdfs)}</ul></div><p style="color:var(--grey);font-size:12px;margin-top:14px">Cliquez sur un fichier pour voir son contenu. Dossier réel : <code>${esc(C.hdfs_dir)}</code></p>`;
    pane.querySelectorAll("[data-p]").forEach(el => el.onclick = e => {
      e.stopPropagation(); const p = el.dataset.p;
      if (el.dataset.d === "1"){ OPEN.has(p) ? OPEN.delete(p) : OPEN.add(p); renderAside(); }
      else showFile(p);
    });
  } else if (TAB === "meta"){
    pane.innerHTML = `<div class="meta">` + STATE.meta.databases.map(db => `<div class="db"><h6>${esc(db.name)}${db.name===STATE.meta.current?'<span class="cur">courante</span>':""}</h6>
      <div style="font-family:var(--mono);font-size:11px;color:var(--grey);margin-bottom:4px">${esc(db.location)}</div>
      ${db.tables.length? db.tables.map(t => `<div class="tb"><span class="nm">${esc(t.name)}</span><span class="badge ${t.kind==="externe"?"ext":t.kind==="interne"?"int":"vue"}">${t.kind.toUpperCase()}</span>
        ${t.location?`<div class="loc">${esc(t.location)}</div>`:""}
        <div class="cols">${t.columns.map(c=>esc(c[0])).join(", ")}${t.partitions.length?` · <b>partition</b> ${t.partitions.map(c=>esc(c[0])).join(", ")}`:""}${t.bucket?` · <b>${t.bucket.n} buckets</b> sur ${esc(t.bucket.col)}`:""}${t.skip_header?" · en-tête ignorée":""}</div></div>`).join("") : `<div style="color:var(--grey)">aucune table</div>`}</div>`).join("") + `</div>`;
  } else {
    pane.innerHTML = `<div class="conf"><p style="margin-top:0;color:var(--grey)">Propriétés de session (SET) utiles au workshop :</p>` +
      Object.entries(STATE.meta.conf).map(([k,v]) => `<div><span>${esc(k)}</span><b class="${v==="true"||v==="nonstrict"?"t":"f"}">${esc(v)}</b></div>`).join("") + `</div>`;
  }
}
function treeNode(n){
  const isOpen = OPEN.has(n.path);
  const isNew = NEWP.has(n.path + (n.dir?"/":""));
  if (n.dir){
    const kids = (n.children||[]);
    return `<li><div class="n ${isNew?"new":""}" data-p="${esc(n.path)}" data-d="1"><span class="ic">${isOpen?"▾":"▸"}</span>📁 ${esc(n.name)}${kids.length?"":` <span class="sz">vide</span>`}</div>${isOpen && kids.length?`<ul>${kids.map(treeNode).join("")}</ul>`:""}</li>`;
  }
  return `<li><div class="n ${isNew?"new":""}" data-p="${esc(n.path)}" data-d="0"><span class="ic"></span>📄 ${esc(n.name)}<span class="sz ${n.size===0?"zero":""}">${n.size} o</span></div></li>`;
}
async function showFile(p){
  const d = await api("/api/cat?path="+encodeURIComponent(p));
  $("#mTitle").textContent = d.error ? p : `${d.path} · ${d.size} octets`;
  $("#mBody").textContent = d.error ? d.error : (d.lines.join("\n") || "(fichier vide : 0 octet)") + (d.truncated?"\n…":"");
  $("#modal").classList.add("on");
}
async function showLocal(name){
  const d = await api("/api/local?name="+encodeURIComponent(name));
  $("#mTitle").textContent = `donnees/${d.name} · ${d.size} octets${d.crlf?" · fins de ligne Windows (CRLF)":""}`;
  $("#mBody").textContent = d.lines.join("\n");
  $("#modal").classList.add("on");
}

function renderNav(){
  const nav = $("#nav");
  let h = `<a href="#home" class="${ROUTE==="home"?"on":""}"><span class="id">◆</span>Accueil</a>`;
  for (const p of C.parts){
    h += `<h4>${esc(p.title)}</h4>`;
    for (const s of C.steps.filter(s=>s.part===p.id))
      h += `<a href="#s/${s.id}" class="${ROUTE==="s/"+s.id?"on":""}"><span class="id">${s.id}</span>${esc(s.title)}${done.has(s.id)?'<span class="ok">✓</span>':""}</a>`;
    if (p.id === "p3") for (const q of C.questions)
      h += `<a href="#q/${q.n}" class="${ROUTE==="q/"+q.n?"on":""}"><span class="id">Q${q.n}</span>${esc(q.title)}${done.has("q"+q.n)?'<span class="ok">✓</span>':""}</a>`;
  }
  h += `<h4>Outils</h4><a href="#console" class="${ROUTE==="console"?"on":""}"><span class="id">&gt;_</span>Console libre</a>
        <a href="#fichiers" class="${ROUTE==="fichiers"?"on":""}"><span class="id">📄</span>Fichiers fournis</a>`;
  if (!C.student) h += `<a href="#python" class="${ROUTE==="python"?"on":""}"><span class="id">py</span>Analyse en Python pur</a>`;
  nav.innerHTML = h;
}

function editorBlock(id, code, label){
  const lines = Math.min(Math.max((code||"").split("\n").length + 1, 5), 26);
  return `<div class="editor"><div class="bar"><span class="t">${esc(label||"HiveQL")}</span><span>Ctrl+Entrée pour exécuter</span>
    <button class="btn run" data-run="${id}">▶ Exécuter</button></div><textarea id="${id}" rows="${lines}" spellcheck="false">${esc(code||"")}</textarea></div><div class="results" id="${id}-out"></div>`;
}
function wireEditors(stepId){
  document.querySelectorAll("textarea").forEach(ta => {
    ta.addEventListener("keydown", e => {
      if ((e.ctrlKey||e.metaKey) && e.key === "Enter"){ e.preventDefault(); run(ta.value, $("#"+ta.id+"-out"), stepId); }
      if (e.key === "Tab"){ e.preventDefault(); const s=ta.selectionStart; ta.value = ta.value.slice(0,s)+"  "+ta.value.slice(ta.selectionEnd); ta.selectionStart=ta.selectionEnd=s+2; }
    });
  });
  document.querySelectorAll("[data-run]").forEach(b => b.onclick = () => { const id=b.dataset.run; run($("#"+id).value, $("#"+id+"-out"), stepId); });
}
function catchUpButton(id){
  return `<button class="btn light" id="catchup" title="Repart de zéro et exécute toutes les étapes avant celle-ci">↺ Rattraper : rejouer les étapes précédentes</button>`;
}
function wireCatchUp(id){
  const b = $("#catchup"); if (!b) return;
  b.onclick = async () => { b.disabled = true; b.textContent = "Rejeu en cours…"; await api("/api/replay", {upto:id});
    let before = true; C.steps.forEach(s => { if (s.id === id) before = false; if (before) done.add(s.id); });
    await refresh(true); renderNav(); b.disabled = false; b.textContent = "↺ Rattraper : rejouer les étapes précédentes"; toast("État reconstruit jusqu'à l'étape " + id); };
}

function nextLink(route){
  const order = ["home", ...C.steps.map(s=>"s/"+s.id), ...C.questions.map(q=>"q/"+q.n), "console"];
  const i = order.indexOf(route); if (i < 0 || i === order.length-1) return "";
  const nxt = order[i+1]; let label = "Console libre";
  if (nxt.startsWith("s/")) { const s = C.steps.find(x=>"s/"+x.id===nxt); label = `${s.id} · ${s.title}`; }
  if (nxt.startsWith("q/")) { const q = C.questions.find(x=>"q/"+x.n===nxt); label = `Q${q.n} · ${q.title}`; }
  return `<div class="next"><a class="btn light" href="#${nxt}" style="text-decoration:none">Suivant : ${esc(label)} →</a></div>`;
}

function viewHome(){
  return `<div class="home"><span class="kicker">Big Data · FISA A4</span><h1>Le workshop Hive, rejoué en Python</h1>
  <p class="lead">Chaque étape du sujet s'exécute ici sans Cloudera : HDFS est un vrai dossier sur votre disque, le Metastore un fichier JSON, et les requêtes HiveQL sont exécutées avec les règles de lecture de Hive (CAST qui renvoie NULL, en-têtes ignorées, partitions en répertoires, buckets par hachage).</p>
  <div class="grid">${C.parts.map(p => { const n = C.steps.filter(s=>s.part===p.id).length + (p.id==="p3"?C.questions.length:0);
    const first = C.steps.find(s=>s.part===p.id); return `<div class="card" onclick="location.hash='#s/${first.id}'"><div class="n">${n}</div><h5>${esc(p.title)}</h5><div style="color:var(--grey);font-size:13px">${p.id==="p1"?"Bases, tables externes et internes, CTAS, LOAD DATA":p.id==="p2"?"Partitions dynamiques, buckets, pièges de HDFS":"Mise en place puis questions Q1 à Q10"}</div></div>`; }).join("")}</div>
  <div class="watch"><b>Comment s'en servir</b><ul>
    <li>Suivez les étapes dans l'ordre : chacune part de l'état laissé par la précédente.</li>
    <li>Le panneau de droite montre HDFS en direct : les ajouts de la dernière exécution sont surlignés.</li>
    <li>Perdu ? Le bouton « Rattraper » de chaque étape reconstruit l'état attendu.</li>
    <li>Les fichiers « locaux » de la VM (<code>/home/cloudera/…</code>) sont lus dans <code>${esc(C.data_dir)}</code>.</li></ul></div>
  <p style="color:var(--grey);font-size:13px">Ce qui n'est pas simulé : la distribution sur plusieurs nœuds, MapReduce et YARN, les temps de réponse réels. Les résultats, eux, sont ceux que Hive donne sur ces fichiers.</p></div>`;
}
function viewStep(s){
  const part = C.parts.find(p=>p.id===s.part);
  let h = `<span class="kicker">${esc(part.title)} · étape ${s.id}</span><h1>${esc(s.title)}</h1><p class="lead">${esc(s.text)}</p>`;
  if (s.watch && s.watch.length) h += `<div class="watch"><b>À observer</b><ul>${s.watch.map(w=>`<li>${esc(w)}</li>`).join("")}</ul></div>`;
  h += `<div class="row">${catchUpButton(s.id)}</div>`;
  h += editorBlock("ed1", s.sql, s.sql2 ? "1 · HiveQL" : "HiveQL");
  if (s.sql2) h += editorBlock("ed2", s.sql2, "2 · La parade");
  return h + nextLink("s/"+s.id);
}
function viewQuestion(q){
  let h = `<span class="kicker">Partie 3 · question ${q.n} / 10</span><h1>${esc(q.title)}</h1>
    <div class="qbig"><div class="q">Q${q.n}</div><p>${esc(q.question)}</p></div>
    <div class="cards"><div class="card"><h5>Tables utiles</h5><ul>${q.tables.map(t=>`<li><code>${esc(t)}</code></li>`).join("")}</ul></div>
    <div class="card"><h5>Notions</h5><ul>${q.notions.map(t=>`<li>${esc(t)}</li>`).join("")}</ul></div>
    <div class="card hint"><h5>Coup de pouce <span style="font-weight:400;color:var(--grey)">(cliquez pour révéler)</span></h5><ul class="blur" onclick="this.classList.add('show')">${q.hint.map(t=>`<li>${esc(t)}</li>`).join("")}</ul></div></div>
    <div class="row">${catchUpButton("q"+q.n)}</div>`;
  h += editorBlock("ed1", "USE analyse_salaires;\n\n-- Écrivez votre requête HiveQL ici\n", "HiveQL · votre réponse");
  if (!C.student) h += `<div class="row"><button class="btn light" id="bSol">Afficher la correction</button><button class="btn light" id="bSub">Corrigé du sujet 2023</button><button class="btn light" id="bPy">Version Python pur</button></div><div id="qextra"></div>`;
  return h + nextLink("q/"+q.n);
}
function wireQuestion(q){
  wireCatchUpQ(q);
  if (C.student) return;
  const extra = $("#qextra");
  const load = (code, label, cls) => {
    $("#ed1").value = "USE analyse_salaires;\n\n" + code; $("#ed1").rows = Math.min(code.split("\n").length + 4, 26);
    extra.innerHTML = `<div class="expected ${cls}"><b>${label}</b><br>${esc(q.expected)}</div>`;
    run($("#ed1").value, $("#ed1-out"), "q"+q.n);
  };
  $("#bSol").onclick = () => load(q.solution, "Résultat attendu", "");
  $("#bSub").onclick = () => load(q.subject, "Corrigé du sujet : à comparer avec le résultat attendu", "subject");
  $("#bPy").onclick = async () => { const d = await api("/api/python", {n:q.n}); extra.innerHTML = `<div class="results">${renderResult(d)}</div><div class="expected"><b>Résultat attendu</b><br>${esc(q.expected)}</div>`; };
}
function wireCatchUpQ(q){
  const b = $("#catchup"); if (!b) return;
  b.onclick = async () => { b.disabled = true; b.textContent = "Rejeu en cours…"; await api("/api/replay", {});
    C.steps.forEach(s => done.add(s.id)); await refresh(true); renderNav(); b.disabled = false;
    b.textContent = "↺ Rattraper : rejouer les étapes précédentes"; toast("Parties 1 à 3 rejouées : la base analyse_salaires est prête"); };
}
function viewConsole(){
  return `<span class="kicker dark">Console libre</span><h1>Écrire ses propres requêtes</h1>
  <p class="lead">Instructions HiveQL séparées par « ; ». Les commandes <code>hdfs dfs …</code> s'écrivent une par ligne. Exemples : <code>SHOW DATABASES;</code>, <code>DESCRIBE FORMATTED employes;</code>, <code>hdfs dfs -ls -R /user/hive</code>.</p>` +
  editorBlock("ed1", "SHOW DATABASES;\nSHOW TABLES;\nhdfs dfs -ls -R /user/hive/data", "HiveQL · console");
}
function viewFiles(){
  return `<span class="kicker dark">Fichiers fournis</span><h1>Les fichiers du dossier donnees/</h1>
  <p class="lead">Ce sont les fichiers « locaux » de la VM. Ils ont des fins de ligne Windows (CRLF), que Hadoop gère, et quelques accents encodés en Latin-1, que Hive affiche « � ».</p>
  <div class="files">${C.local_files.map(f=>`<div class="f" data-f="${esc(f)}">📄 <b>${esc(f)}</b><span class="sz">voir</span></div>`).join("")}</div>`;
}
function viewPython(){
  return `<span class="kicker dark">Python pur</span><h1>Les dix questions sans SQL</h1>
  <p class="lead">Mêmes résultats que les requêtes HiveQL, avec des listes et des dictionnaires. Utile pour montrer ce que fait réellement une jointure ou un GROUP BY. Les fonctions de lecture :</p>
  <pre class="src">${esc(C.helpers)}</pre><div class="row">${C.questions.map(q=>`<button class="btn light" data-py="${q.n}">Q${q.n}</button>`).join("")}</div><div class="results" id="pyout"></div>`;
}

function route(){
  ROUTE = (location.hash || "#home").slice(1);
  const main = $("#main");
  $("#nav").classList.remove("on"); $("#aside").classList.remove("on");
  if (ROUTE.startsWith("s/")){ const s = C.steps.find(x=>x.id===ROUTE.slice(2)); if (s){ main.innerHTML = viewStep(s); wireEditors(s.id); wireCatchUp(s.id); } }
  else if (ROUTE.startsWith("q/")){ const q = C.questions.find(x=>String(x.n)===ROUTE.slice(2)); if (q){ main.innerHTML = viewQuestion(q); wireEditors("q"+q.n); wireQuestion(q); } }
  else if (ROUTE === "console"){ main.innerHTML = viewConsole(); wireEditors(null); }
  else if (ROUTE === "fichiers"){ main.innerHTML = viewFiles(); main.querySelectorAll("[data-f]").forEach(el=>el.onclick=()=>showLocal(el.dataset.f)); }
  else if (ROUTE === "python" && !C.student){ main.innerHTML = viewPython(); main.querySelectorAll("[data-py]").forEach(b=>b.onclick=async()=>{ const d=await api("/api/python",{n:+b.dataset.py}); $("#pyout").innerHTML = renderResult(d); }); }
  else { ROUTE = "home"; main.innerHTML = viewHome(); }
  main.scrollTop = 0;
  renderNav();
}

(async function init(){
  C = await api("/api/content");
  $("#mode").textContent = C.student ? "Mode élèves" : "Mode tuteur";
  document.querySelectorAll(".tabs button").forEach(b => b.onclick = () => { TAB = b.dataset.t; renderAside(); });
  $("#mClose").onclick = () => $("#modal").classList.remove("on");
  $("#modal").onclick = e => { if (e.target.id === "modal") $("#modal").classList.remove("on"); };
  document.addEventListener("keydown", e => { if (e.key === "Escape") $("#modal").classList.remove("on"); });
  $("#toggleAside").onclick = () => $("#aside").classList.toggle("on");
  $("#toggleNav").onclick = () => $("#nav").classList.toggle("on");
  $("#resetBtn").onclick = async () => { await api("/api/reset", {}); done.clear(); NEWP = new Set(); DIFF = null; await refresh(false); route(); toast("HDFS et Metastore remis à zéro"); };
  window.addEventListener("hashchange", route);
  await refresh(false);
  route();
})();
</script>
</body>
</html>
"""


if __name__ == "__main__":
    main()
