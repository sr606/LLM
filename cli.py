

import os
import argparse
import pandas as pd
import logging
from tqdm import tqdm

try:
    from .analyzer import analyze, decompose_tables_to_3nf, save_langgraph_model_json
    from .loader import load_csv_folder, load_json_file
    from .sql_generator import generate_ddl, save_ddl
    from .sql_generator import generate_normalized_ddl_from_model, save_normalized_ddl
    from .sql_generator import generate_migration_sql_from_model, save_migration_sql
    from .erd import generate_dot, save_dot
    from .catalog import build_catalog, save_catalog
except ImportError:
    from analyzer import analyze, decompose_tables_to_3nf, save_langgraph_model_json
    from loader import load_csv_folder, load_json_file
    from sql_generator import generate_ddl, save_ddl
    from sql_generator import generate_normalized_ddl_from_model, save_normalized_ddl
    from sql_generator import generate_migration_sql_from_model, save_migration_sql
    from erd import generate_dot, save_dot
    from catalog import build_catalog, save_catalog
 
def main():
    parser = argparse.ArgumentParser(description="3NF modeling CLI with large file support and normalization.")
    parser.add_argument("input", help="Input path (folder for CSVs or a single JSON file)")
    parser.add_argument("--out-sql", default="out/schema.sql", help="Output path for generated SQL DDL.")
    parser.add_argument("--out-dot", default="out/erd.dot", help="Output path for generated ERD DOT file.")
    parser.add_argument("--out-catalog", default="out/catalog.json", help="Output path for generated catalog JSON.")
    parser.add_argument("--out-model", default="out/model.json", help="Output path for generated model JSON.")
    parser.add_argument("--include", nargs="*", help="Specific table names to include (CSV only)")
    parser.add_argument("--chunk-size", type=int, default=0, help="Chunk size for reading large CSVs (0=disable, loads all at once)")
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--use-dask", action="store_true", help="Use Dask for large CSVs (requires dask to be installed)")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format='%(asctime)s %(levelname)s %(message)s')

    os.makedirs(os.path.dirname(args.out_sql), exist_ok=True)
    # Set environment variables for loader
    if args.chunk_size > 0:
        os.environ["CHUNK_SIZE"] = str(args.chunk_size)
    if args.use_dask:
        os.environ["USE_DASK"] = "1"

    # Load
    if args.input.lower().endswith(".json"):
        tables = load_json_file(args.input)
    elif args.input.lower().endswith(".csv"):
        # Single CSV file
        tname = os.path.splitext(os.path.basename(args.input))[0]
        logging.info(f"Loading single CSV file: {args.input}")
        if args.use_dask:
            import dask.dataframe as dd
            tables = {tname: dd.read_csv(args.input)}
        elif args.chunk_size > 0:
            chunks = []
            for chunk in pd.read_csv(args.input, chunksize=args.chunk_size):
                chunks.append(chunk)
            tables = {tname: pd.concat(chunks, ignore_index=True)}
        else:
            tables = {tname: pd.read_csv(args.input)}
    else:
        # Directory of CSVs
        file_list = [f for f in os.listdir(args.input) if f.lower().endswith('.csv')]
        tables = {}
        for fname in tqdm(file_list, desc="Loading CSVs"):
            tname = os.path.splitext(fname)[0]
            if args.include and tname not in args.include:
                continue
            path = os.path.join(args.input, fname)
            if args.use_dask:
                import dask.dataframe as dd
                tables[tname] = dd.read_csv(path)
            elif args.chunk_size > 0:
                chunks = []
                for chunk in pd.read_csv(path, chunksize=args.chunk_size):
                    chunks.append(chunk)
                tables[tname] = pd.concat(chunks, ignore_index=True)
            else:
                tables[tname] = pd.read_csv(path)

    # Analyze
    logging.info("Analyzing tables...")
    analysis = analyze(tables)
    model = decompose_tables_to_3nf(tables, analysis)

    # DDL
    logging.info("Generating DDL...")
    ddl = generate_ddl(model)
    save_ddl(ddl, args.out_sql)

    # Also generate a normalized, user-friendly DDL when possible
    try:
        norm_sql = generate_normalized_ddl_from_model(model)
        save_normalized_ddl(norm_sql, os.path.join(os.path.dirname(args.out_sql), "normalized_schema.sql"))
        logging.info("Saved normalized schema to out/normalized_schema.sql")
    except Exception:
        logging.debug("Could not generate normalized DDL", exc_info=True)

    # ERD
    logging.info("Generating ERD...")
    dot = generate_dot(model)
    save_dot(dot, args.out_dot)

    # Catalog
    logging.info("Building catalog...")
    catalog = build_catalog(tables, analysis, model)
    save_catalog(catalog, args.out_catalog)

    # Model JSON
    logging.info("Saving model JSON...")
    save_langgraph_model_json(model, args.out_model)

    # Migration SQL (best-effort)
    try:
        mig_sql = generate_migration_sql_from_model(model, tables)
        save_migration_sql(mig_sql, os.path.join(os.path.dirname(args.out_sql), "migration.sql"))
        logging.info("Saved migration SQL to out/migration.sql")
    except Exception:
        logging.debug("Could not generate migration SQL", exc_info=True)

    logging.info("Done.")

if __name__ == "__main__":
    main()