"""Inspeção somente leitura do SQLite interno de metadados tabulares."""

from __future__ import annotations

import sqlite3
import sys


def main(path: str) -> int:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        tables = [row[0] for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name"
        )]
        print("SQLITE_TABLES", tables)
        for table in tables:
            columns = [row[1] for row in connection.execute(f'PRAGMA table_info("{table}")')]
            if any(name.lower() in {"name", "tablename", "expression"} for name in columns):
                print("SCHEMA", table, columns)
        model_tables = connection.execute(
            'SELECT Name FROM "Table" ORDER BY Name'
        ).fetchall()
        print("MODEL_TABLES", [row[0] for row in model_tables])
        analytic = connection.execute(
            '''
            SELECT t.Name, COUNT(DISTINCT c.ID), COUNT(DISTINCT m.ID)
            FROM "Table" t
            LEFT JOIN "Column" c ON c.TableID = t.ID
            LEFT JOIN "Measure" m ON m.TableID = t.ID
            WHERE t.Name LIKE 'analitico_%'
            GROUP BY t.ID, t.Name
            ORDER BY t.Name
            '''
        ).fetchall()
        print("ANALYTIC_OBJECTS", analytic)
        measure_errors = connection.execute(
            '''
            SELECT t.Name, m.Name, m.State, COALESCE(m.ErrorMessage, '')
            FROM "Measure" m
            JOIN "Table" t ON t.ID = m.TableID
            WHERE t.Name LIKE 'analitico_%' AND COALESCE(m.ErrorMessage, '') <> ''
            ORDER BY t.Name, m.Name
            '''
        ).fetchall()
        print("ANALYTIC_MEASURE_ERRORS", measure_errors)
        return 0
    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1]))
