"""DuckDB NSV Extension"""
import duckdb
import nsv


def read_nsv(filename, con=None):
    """Read NSV file into DuckDB."""
    if con is None:
        con = duckdb.connect()

    with open(filename) as f:
        data = nsv.load(f)

    if not data:
        raise ValueError("Empty NSV file")

    # First row is header, rest is data
    import pandas as pd
    df = pd.DataFrame(data[1:], columns=data[0])
    return con.from_df(df)


def to_nsv(rel, filename):
    """Write DuckDB relation to NSV file."""
    df = rel.df()
    rows = [df.columns.tolist()] + df.values.tolist()
    str_rows = [[str(c) for c in row] for row in rows]

    with open(filename, 'w') as f:
        f.write(nsv.dumps(str_rows))
