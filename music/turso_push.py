#!/usr/bin/env python3
"""Push master.sqlite to Turso, replacing its contents entirely.

Turso is a durable off-repo backup of the working DB — a replacement for
tracking master.sqlite/.gz in git LFS, which kept hitting storage quota
limits from repeated large-binary commits. mdb.py/sync.py still read and
write the local master.sqlite directly; this script is a one-way mirror
run after a checkpoint, not a live sync.

Requires TURSO_DATABASE_URL and TURSO_AUTH_TOKEN in music/.env.
"""
import base64
import json
import os
import sqlite3
import sys
import time
import urllib.request
from pathlib import Path

SRC = Path(__file__).parent / 'master.sqlite'

STMT_MAX_PARAMS = 12000  # stay well under SQLite's bound-parameter limit
CALL_MAX_ROWS = 4000     # rows per HTTP call, across multiple multi-row statements


def _load_env():
    env_path = Path(__file__).parent / '.env'
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            os.environ.setdefault(k.strip(), v.strip())


def _val(v):
    if v is None:
        return {'type': 'null'}
    if isinstance(v, int):
        return {'type': 'integer', 'value': str(v)}
    if isinstance(v, float):
        return {'type': 'float', 'value': v}
    if isinstance(v, bytes):
        return {'type': 'blob', 'base64': base64.b64encode(v).decode()}
    return {'type': 'text', 'value': str(v)}


class TursoClient:
    def __init__(self, url: str, token: str):
        self.pipeline_url = url.replace('libsql://', 'https://') + '/v2/pipeline'
        self.token = token

    def _hrana(self, requests_list, timeout=180):
        body = json.dumps({'requests': requests_list}).encode()
        req = urllib.request.Request(
            self.pipeline_url, data=body, method='POST',
            headers={'Authorization': f'Bearer {self.token}', 'Content-Type': 'application/json'},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())

    def exec_many(self, sql_stmts: list[str], label: str = ''):
        """Run several standalone statements (no bound args) in one call."""
        reqs = [{'type': 'execute', 'stmt': {'sql': 'PRAGMA foreign_keys=OFF'}}]
        reqs += [{'type': 'execute', 'stmt': {'sql': sql}} for sql in sql_stmts]
        reqs.append({'type': 'close'})
        result = self._hrana(reqs)
        errs = [(i, r['error']) for i, r in enumerate(result['results']) if r['type'] == 'error']
        if errs:
            raise RuntimeError(f'Turso error {label}: {errs[:5]}')

    def exec_batched_inserts(self, requests_list, label=''):
        reqs = [{'type': 'execute', 'stmt': {'sql': 'PRAGMA foreign_keys=OFF'}}] + requests_list
        reqs.append({'type': 'close'})
        result = self._hrana(reqs)
        errs = [(i, r['error']) for i, r in enumerate(result['results']) if r['type'] == 'error']
        if errs:
            raise RuntimeError(f'Turso error {label}: {errs[:5]}')

    def table_names(self):
        result = self._hrana([
            {'type': 'execute', 'stmt': {'sql': "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"}},
            {'type': 'close'},
        ])
        return [row[0]['value'] for row in result['results'][0]['response']['result']['rows']]


def main():
    _load_env()
    url = os.environ.get('TURSO_DATABASE_URL')
    token = os.environ.get('TURSO_AUTH_TOKEN')
    if not url or not token:
        print('TURSO_DATABASE_URL / TURSO_AUTH_TOKEN not set in music/.env', file=sys.stderr)
        return 1

    client = TursoClient(url, token)
    local = sqlite3.connect(str(SRC))
    local.row_factory = sqlite3.Row

    print('Dropping existing Turso tables...')
    existing = client.table_names()
    if existing:
        client.exec_many([f'DROP TABLE IF EXISTS "{t}"' for t in existing], 'drop')

    schema_rows = local.execute(
        "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL AND name != 'sqlite_sequence' "
        "ORDER BY CASE type WHEN 'table' THEN 0 WHEN 'index' THEN 1 WHEN 'trigger' THEN 2 ELSE 3 END"
    ).fetchall()
    print(f'Creating {len(schema_rows)} schema objects...')
    client.exec_many([row['sql'] for row in schema_rows], 'schema')

    tables = [r['name'] for r in schema_rows if r['type'] == 'table']
    t_start = time.time()
    for t in tables:
        cols = [c[1] for c in local.execute(f'PRAGMA table_info("{t}")').fetchall()]
        ncols = len(cols)
        rows_per_stmt = max(1, STMT_MAX_PARAMS // max(ncols, 1))
        col_list = ','.join(f'"{c}"' for c in cols)
        ph_row = '(' + ','.join('?' * ncols) + ')'
        rows = local.execute(f'SELECT {col_list} FROM "{t}"').fetchall()
        total = len(rows)
        if total == 0:
            print(f'  {t}: 0 rows')
            continue

        sent = 0
        i = 0
        t0 = time.time()
        while i < total:
            reqs = []
            rows_in_call = 0
            while i < total and rows_in_call < CALL_MAX_ROWS:
                chunk = rows[i:i + rows_per_stmt]
                sql = f'INSERT INTO "{t}" ({col_list}) VALUES ' + ','.join([ph_row] * len(chunk))
                args = []
                for r in chunk:
                    args.extend(r)
                reqs.append({'type': 'execute', 'stmt': {'sql': sql, 'args': [_val(x) for x in args]}})
                i += len(chunk)
                rows_in_call += len(chunk)
            client.exec_batched_inserts(reqs, f'{t}@{i}')
            sent += rows_in_call
        dt = time.time() - t0
        print(f'  {t}: {sent}/{total} rows in {dt:.1f}s ({sent / dt:.0f} rows/s)')

    print(f'Done in {time.time() - t_start:.1f}s total.')
    return 0


if __name__ == '__main__':
    sys.exit(main())
