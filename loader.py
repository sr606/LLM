import os
import json
import pandas as pd
from typing import Dict, Optional, List
def load_csv_folder(folder: str, include: Optional[List[str]] = None) -> Dict[str, pd.DataFrame]:

    """
    Load CSV files from folder; table name = filename without extension.
    Handles large files by reading in chunks if specified.
    """
    import logging
    tables = {}
    chunk_size = int(os.getenv("CHUNK_SIZE", "0"))  # 0 means no chunking
    use_dask = os.getenv("USE_DASK", "0") == "1"
    try:
        if use_dask:
            import dask.dataframe as dd
    except ImportError:
        use_dask = False
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".csv"):
            continue
        tname = os.path.splitext(fname)[0]
        if include and tname not in include:
            continue
        path = os.path.join(folder, fname)
        logging.info(f"Loading table {tname} from {path}")
        if use_dask:
            tables[tname] = dd.read_csv(path)
        elif chunk_size > 0:
            # Read in chunks and concatenate
            chunks = []
            for chunk in pd.read_csv(path, chunksize=chunk_size):
                chunks.append(chunk)
            tables[tname] = pd.concat(chunks, ignore_index=True)
        else:
            tables[tname] = pd.read_csv(path)
    return tables

def load_csv_folder_in_chunks(folder: str, chunk_size: int = 100000, include: Optional[List[str]] = None):
    """
    Generator that yields (table_name, chunk_df) for each chunk in each CSV file.
    Useful for processing very large files incrementally.
    """
    for fname in os.listdir(folder):
        if not fname.lower().endswith(".csv"):
            continue
        tname = os.path.splitext(fname)[0]
        if include and tname not in include:
            continue
        path = os.path.join(folder, fname)
        for chunk in pd.read_csv(path, chunksize=chunk_size):
            yield tname, chunk
 
def flatten_json(json_obj, table_prefix="root"):

    """

    Simple flattener: dicts become columns; arrays create child tables.

    Returns dict of {table_name: DataFrame}

    """

    import pandas as pd

    tables = {}
 
    def _dict_to_df(d, name):

        return pd.DataFrame([d])
 
    def _walk(node, name):

        if isinstance(node, dict):

            # accumulate scalar fields

            scalar = {k: v for k, v in node.items() if not isinstance(v, (dict, list))}

            if scalar:

                tables.setdefault(name, []).append(scalar)

            for k, v in node.items():

                if isinstance(v, dict):

                    _walk(v, f"{name}_{k}")

                elif isinstance(v, list):

                    for i, item in enumerate(v):

                        _walk(item, f"{name}_{k}")

        elif isinstance(node, list):

            for item in node:

                _walk(item, name)

        else:

            # scalar: handled in dict pass

            pass
 
    _walk(json_obj, table_prefix)
 
    # Convert lists of dicts to DataFrames

    df_tables = {}

    for tname, rows in tables.items():

        df_tables[tname] = pd.DataFrame(rows)

    return df_tables
 
def load_json_file(path: str) -> Dict[str, pd.DataFrame]:

    with open(path, "r", encoding="utf-8") as fh:

        obj = json.load(fh)

    return flatten_json(obj, table_prefix=os.path.splitext(os.path.basename(path))[0])

 