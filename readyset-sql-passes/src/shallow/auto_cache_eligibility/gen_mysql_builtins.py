#!/usr/bin/env python3
"""Generate the MySQL builtin-function allowlist for shallow-cache eligibility.

A shallow cache is auto-created only for queries that call builtin functions; a
call to anything outside this list is treated as a user-defined function (UDF)
and the query is proxied instead of cached.  MySQL has no catalog of native
functions (information_schema.ROUTINES holds stored routines, mysql.func holds
loadable UDFs -- neither lists natives). We union two sources (the fail-safe
direction): the server's own mysql.help_topic/help_category tables, queried from
a throwaway container per major, and the manual's "Built-In Function and
Operator Reference" page. The help tables are the more authoritative source but
are not always populated (they depend on the server shipping help data), so the
docs scrape is the reliable fallback and the union never regresses when the help
tables are empty.

We deliberately do NOT scrape the separate "Loadable Function Reference"
(component/enterprise functions such as mask_*, keyring_*, group_replication_*):
those are not natives and should remain ineligible.

Usage:
    ./gen_mysql_builtins.py [VERSION ...]     # defaults to the supported majors
Writes mysql_builtins.rs next to this script.  Needs Docker and network access.
"""

import html
import re
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

DEFAULT_VERSIONS = ["8.0", "8.4"]

# The server's own help tables enumerate the builtin functions/operators. This
# is the more authoritative source than the manual page, but the help tables
# are not always populated (they depend on the server being built with help
# data), so it is unioned with the docs scrape rather than replacing it.
HELP_TOPIC_QUERY = (
    "SELECT t.name FROM mysql.help_topic t "
    "JOIN mysql.help_category c ON c.help_category_id = t.help_category_id "
    "WHERE c.name LIKE '%Function%' OR c.name LIKE '%Operator%'"
)

# dev.mysql.com returns 403 to non-browser clients; the Oracle docs mirror serves
# the identical page without that block.
URL = "https://docs.oracle.com/cd/E17952_01/mysql-{v}-en/built-in-function-reference.html"
USER_AGENT = "Mozilla/5.0 (readyset builtin-allowlist generator)"

# Each reference entry is an anchor into #function_NAME / #operator_NAME whose
# link text is the function spelled in a <code class="literal"> element. We take
# the link text (not the href fragment, which lossily hyphenates ST_Area ->
# st-area), then keep the identifier before the first '('.
ENTRY_RE = re.compile(
    r'#(?:function|operator)[^"]*"[^>]*>\s*<code class="literal">([^<]+)</code>',
    re.IGNORECASE,
)

# Band covers the union of the docs scrape and the help tables; the help-topic
# source widens the upper end when it is populated.
MIN_EXPECTED = 280
MAX_EXPECTED = 1000


def normalize(raw: str) -> str | None:
    name = html.unescape(raw)            # &gt; -> >, &amp; -> & so operators drop below
    name = name.split("(", 1)[0].strip()  # drop the argument list / parens
    name = name.split(".")[-1]           # keep the last name component
    name = name.lower()
    if not re.search(r"[a-z]", name):    # pure-symbol operators (||, ->>, &)
        return None
    if re.search(r"\s", name):           # multiword keyword forms
        return None
    return name


def collect(version: str) -> set[str]:
    url = URL.format(v=version)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as resp:
        html = resp.read().decode("utf-8", "replace")
    names = {n for raw in ENTRY_RE.findall(html) if (n := normalize(raw))}
    print(f"  mysql {version}: {len(names)} doc-page names", file=sys.stderr)
    return names


def collect_help_topic(version: str) -> set[str]:
    """Query the server's help tables for builtin function/operator names.

    Best-effort: returns an empty set (never raises) if Docker is unavailable or
    the help tables are unpopulated, so the docs scrape stays the fallback.
    """
    container = f"rs-mysqlbuiltins-{version}"
    try:
        subprocess.run(["docker", "rm", "-f", container],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        print(f"  mysql:{version}: starting container", file=sys.stderr)
        subprocess.run(
            ["docker", "run", "-d", "--rm", "--name", container,
             "-e", "MYSQL_ALLOW_EMPTY_PASSWORD=1", f"mysql:{version}"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            for _ in range(180):
                ready = subprocess.run(
                    ["docker", "exec", container, "mysqladmin", "ping",
                     "-uroot", "--silent"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                )
                if ready.returncode == 0:
                    break
                time.sleep(1)
            else:
                print(f"  mysql:{version}: never became ready; using docs only",
                      file=sys.stderr)
                return set()
            out = subprocess.run(
                ["docker", "exec", container, "mysql", "-uroot", "-N", "-B",
                 "-e", HELP_TOPIC_QUERY],
                capture_output=True, text=True,
            )
            names = {n for line in out.stdout.splitlines()
                     if (n := normalize(line.strip()))}
            print(f"  mysql:{version}: {len(names)} help-topic names", file=sys.stderr)
            return names
        finally:
            subprocess.run(["docker", "rm", "-f", container],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:  # noqa: BLE001 - supplementary source, docs is the base
        print(f"  mysql:{version}: help-topic query failed ({e}); using docs only",
              file=sys.stderr)
        return set()


def write_rust_list(out_path: Path, names: set[str], header_lines: list[str]) -> None:
    """Emit a sorted `pub(super) static BUILTINS: &[&str]` the pass binary-searches.

    Only identifier-shaped names are kept: a function callable by bare name is an
    identifier, and this keeps the generated Rust literal safe without escaping.
    """
    safe = sorted(n for n in names if re.fullmatch(r"[a-z_][a-z0-9_]*", n))
    dropped = len(names) - len(safe)
    if dropped:
        print(f"  dropped {dropped} non-identifier name(s)", file=sys.stderr)
    with out_path.open("w") as f:
        for line in header_lines:
            f.write(f"// {line}\n")
        f.write("// GENERATED -- do not edit by hand.\n")
        f.write("#[rustfmt::skip]\n")
        f.write("pub(super) static BUILTINS: &[&str] = &[\n")
        for name in safe:
            f.write(f'    "{name}",\n')
        f.write("];\n")
    print(f"wrote {len(safe)} names to {out_path}", file=sys.stderr)


def main() -> None:
    versions = sys.argv[1:] or DEFAULT_VERSIONS
    union: set[str] = set()
    for version in versions:
        union |= collect(version)  # docs scrape: the reliable base
        union |= collect_help_topic(version)  # server help tables: supplementary

    if not (MIN_EXPECTED <= len(union) <= MAX_EXPECTED):
        raise SystemExit(
            f"union has {len(union)} names, outside sanity band "
            f"[{MIN_EXPECTED}, {MAX_EXPECTED}]; the page layout likely changed"
        )

    header = [
        "MySQL builtin function names for shallow-cache UDF detection.",
        "Source: mysql.help_topic/help_category (when populated) unioned with the",
        "manual's Built-In Function and Operator Reference, across MySQL majors "
        + ", ".join(versions) + ".",
        "Regenerate: ./gen_mysql_builtins.py",
    ]
    write_rust_list(Path(__file__).with_name("mysql_builtins.rs"), union, header)


if __name__ == "__main__":
    main()
