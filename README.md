# ModelLangGraph (3NF Data Modeling)

Lightweight toolkit to analyze tabular data and propose a 3rd Normal Form (3NF) decomposition, generate DDL, an ERD DOT file, and a simple catalog.

## Quick setup

Open a PowerShell terminal in the project root (where `cli.py` lives): `c:\3nf1`.

Create and activate a virtual environment (recommended):

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Install dependencies:

```powershell
pip install -r requirements.txt
```

> If you only want progress bars and basic functionality, you can install only `tqdm` and `pandas`.

## Usage

Run the CLI from the project folder (do not use `python -m 3nf1.cli` because the folder name starts with a digit):

- Process a single CSV file (with chunked reading and debug logs):

```powershell
python cli.py sample_large.csv --chunk-size 100000 --log-level DEBUG
```

- Process a directory of CSVs and include only specific tables:

```powershell
python cli.py . --chunk-size 1000 --log-level INFO --include sample_large sample_employees
```

- Use Dask for out-of-core reads (requires `dask`):

```powershell
python cli.py . --use-dask --log-level INFO
```

- Customize outputs:

```powershell
python cli.py data_folder --out-sql out/schema.sql --out-dot out/erd.dot --out-catalog out/catalog.json --out-model out/model.json
```

## Notes and tips

- `--chunk-size N` reads CSV files in chunks of `N` rows and concatenates them; set to `0` to disable.
- `--use-dask` requires `dask` and helps when files are larger than available memory.
- By default the CLI writes these outputs:
  - SQL DDL: `out/schema.sql`
  - ERD (Graphviz DOT): `out/erd.dot`
  - Catalog JSON: `out/catalog.json`
  - Model JSON: `out/model.json`

## If you want to run the package as a module

Rename the folder to a valid Python package name (for example `model_langgraph`) and update imports accordingly if you prefer running as `python -m model_langgraph.cli`.

## Troubleshooting

- If you run into import errors when running as a module, run `python cli.py ...` instead.
- If `dask` is not installed but you passed `--use-dask`, the CLI will error; install `dask` or omit the flag.

## License

(keep your existing license if you have one)
