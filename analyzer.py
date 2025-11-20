import os
import pandas as pd
from typing import Dict, Tuple, Any, List, Set, Optional
import numpy as np
import json
try:

    from transformers import pipeline

except Exception:

    pipeline = None
 
def infer_column_type(series: pd.Series) -> str:

    if pd.api.types.is_integer_dtype(series.dropna()):

        return 'INTEGER'

    if pd.api.types.is_float_dtype(series.dropna()):

        return 'FLOAT'

    if pd.api.types.is_bool_dtype(series.dropna()):

        return 'BOOLEAN'

    if pd.api.types.is_datetime64_any_dtype(series.dropna()):

        return 'TIMESTAMP'

    return 'TEXT'
 
def detect_primary_keys(tables: Dict[str, pd.DataFrame]) -> Dict[str, str]:

    pks = {}

    for name, df in tables.items():

        candidates = []

        for col in df.columns:

            s = df[col]

            unique_frac = s.dropna().nunique() / max(1, len(s))

            if col.lower() == 'id' or col.lower().endswith('_id'):

                candidates.append((col, unique_frac))

            if unique_frac > 0.9 and s.dropna().dtype != object:

                candidates.append((col, unique_frac))

        if candidates:

            # pick most unique

            candidates.sort(key=lambda x: -x[1])

            pks[name] = candidates[0][0]

        else:

            # fallback: no PK

            pks[name] = None

    return pks
 
def detect_foreign_keys(tables: Dict[str, pd.DataFrame], pks: Dict[str, str]) -> Dict[Tuple[str, str], Tuple[str, str]]:

    # returns mapping ((table, col) -> (ref_table, ref_col))

    fks = {}

    # build value -> table map for PKs

    pk_values = {}

    for t, pk in pks.items():

        if pk and pk in tables[t].columns:

            pk_values[t] = set(tables[t][pk].dropna().unique())
 
    for t, df in tables.items():

        for col in df.columns:

            # skip if col is PK of self

            if pks.get(t) == col:

                continue

            vals = set(df[col].dropna().unique())

            if not vals:

                continue

            for ref_table, ref_vals in pk_values.items():

                if ref_table == t:

                    continue

                # heuristic: if >50% of non-null values appear in ref pk values

                common = vals.intersection(ref_vals)

                if len(common) / max(1, len(vals)) > 0.5:

                    fks[(t, col)] = (ref_table, pks[ref_table])

                    break

    return fks

def generate_column_descriptions(tables: Dict[str, pd.DataFrame]) -> Dict[str, Dict[str, str]]:

    desc = {}

    use_llm = False

    if pipeline is not None and os.getenv('LOCAL_LLM_MODEL'):

        try:

            gen = pipeline('text-generation', model=os.getenv('LOCAL_LLM_MODEL'))

            use_llm = True

        except Exception:

            use_llm = False
 
    for t, df in tables.items():

        desc[t] = {}

        for col in df.columns:

            sample = df[col].dropna().astype(str).head(5).tolist()

            simple = f"Column `{col}` of table `{t}`. Type guess: {infer_column_type(df[col])}. Samples: {sample}"

            if use_llm:

                prompt = f"Provide a concise, single-sentence description for this column: {simple}"

                try:

                    out = gen(prompt, max_length=128, do_sample=False)

                    text = out[0]['generated_text'] if isinstance(out, list) else str(out)

                    desc_text = text.strip().split('\n')[0]

                except Exception:

                    desc_text = simple

            else:

                # heuristic descriptions

                low = col.lower()

                if low.endswith('_id') or low == 'id':

                    desc_text = 'Identifier linking to another entity.'

                elif 'date' in low or 'time' in low:

                    desc_text = 'Date or timestamp value.'

                elif any(x in low for x in ['name', 'title', 'desc', 'description']):

                    desc_text = 'Textual descriptive field.'

                elif any(x in low for x in ['qty', 'count', 'number', 'amount', 'price']):

                    desc_text = 'Numeric measure.'

                else:

                    desc_text = simple

            desc[t][col] = desc_text

    return desc
 
def analyze(tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:

    pks = detect_primary_keys(tables)

    fks = detect_foreign_keys(tables, pks)

    descriptions = generate_column_descriptions(tables)

    types = {t: {c: infer_column_type(df[c]) for c in df.columns} for t, df in tables.items()}

    return {

        'pks': pks,

        'fks': fks,

        'descriptions': descriptions,

        'types': types,

    }
 
def detect_functional_dependencies(df: pd.DataFrame,

                                   max_det_size: int = 2,

                                   min_support: float = 1.0) -> List[Dict[str, Any]]:

    """

    Detect candidate FDs: X -> Y where for each distinct X value there is a single Y value.

    - max_det_size: max size of determinant set (1 or 2 typical)

    - min_support: fraction of groups that must be single-valued (1.0 = strict)

    Returns list of {"determinant": [cols], "dependent": col, "holds": bool, "support": float}

    """

    cols = list(df.columns)

    fds = []
 
    # helper: test FD for determinant set det -> dep

    def _test_fd(det: List[str], dep: str) -> Tuple[bool, float]:

        sub = df[det + [dep]].dropna()

        if sub.empty:

            return False, 0.0

        grouped = sub.groupby(det)[dep].nunique()

        single = (grouped == 1).sum()

        total = len(grouped)

        support = single / max(total, 1)

        return (support >= min_support), support
 
    # generate determinant candidates: single columns then pairs

    det_sets: List[List[str]] = [[c] for c in cols]

    if max_det_size >= 2:

        for i in range(len(cols)):

            for j in range(i + 1, len(cols)):

                det_sets.append([cols[i], cols[j]])
 
    for det in det_sets:

        det_low = [d.lower() for d in det]

        # skip determinants that are clearly measures (e.g., quantity, price)

        if any(x in d for d in det_low for x in ["qty", "quantity", "amount", "price", "total"]):

            continue
 
        for dep in cols:

            if dep in det:

                continue

            holds, support = _test_fd(det, dep)

            if holds:

                fds.append({"determinant": det, "dependent": dep, "holds": True, "support": support})

    return fds
 
 
def propose_decomposition_for_table(df: pd.DataFrame,

                                    table_name: str,

                                    pk: Optional[str]) -> Dict[str, Any]:

    def _is_measure(col: str) -> bool:

        low = col.lower()

        return any(x in low for x in ["qty", "quantity", "amount", "price", "total", "cost", "rate"])
 
    def _is_identifier(col: str) -> bool:

        low = col.lower()

        return low == "id" or low.endswith("_id")

    def detect_candidate_keys(df: pd.DataFrame, max_size: int = 2) -> List[Tuple[str, ...]]:
        """Return candidate keys (tuples of column names) whose combined values are unique per row.

        Heuristic: check single columns, then pairs (configurable).
        """
        cols = list(df.columns)
        keys: List[Tuple[str, ...]] = []
        # singles
        for c in cols:
            try:
                if df[c].dropna().nunique() == len(df):
                    keys.append((c,))
            except Exception:
                continue
        if max_size >= 2:
            for i in range(len(cols)):
                for j in range(i + 1, len(cols)):
                    a, b = cols[i], cols[j]
                    try:
                        if df[[a, b]].dropna().drop_duplicates().shape[0] == len(df.dropna()):
                            keys.append((a, b))
                    except Exception:
                        continue
        return keys
 
    # Detect FDs including pair determinants so we can catch partial dependencies (2NF)
    fds = detect_functional_dependencies(df, max_det_size=2, min_support=1.0)

    dependents_by_det: Dict[Tuple[str, ...], Set[str]] = {}

    for fd in fds:

        det_tuple = tuple(fd["determinant"])

        dependents_by_det.setdefault(det_tuple, set()).add(fd["dependent"])
 
    # Filter out trivial PK-based FDs (PK -> everything)

    if pk:

        dependents_by_det = {

            det: deps for det, deps in dependents_by_det.items()

            if not (len(det) == 1 and det[0] == pk)

        }
 
    dim_tables = []

    moved = {}

    # Detect repeated / numbered columns like 'Phone 1', 'Phone_2', 'Email1'
    import re
    repeated_groups = {}
    for col in df.columns:
        m = re.match(r"^(?P<base>.+?)\s*[_\-]?\s*(?P<num>\d+)$", col)
        if m:
            base = m.group('base').strip()
            repeated_groups.setdefault(base, []).append(col)

    # Create dimension tables for repeated groups (unpivot)
    for base, cols in repeated_groups.items():
        if len(cols) < 2:
            continue

        # normalized name for dim table
        safe_base = re.sub(r"[^0-9a-zA-Z]+", "_", base).strip('_').lower()
        dim_name = f"{table_name}_{safe_base}_list"

        # id vars include pk if present to maintain relationship
        id_vars = [pk] if pk and pk in df.columns else []
        try:
            melted = df.melt(id_vars=id_vars, value_vars=cols, var_name='orig_col', value_name=safe_base).dropna(subset=[safe_base])
        except Exception:
            # if melt fails for any reason, skip this group
            continue

        if melted.empty:
            continue

        # build columns dict as Series for inference
        col_series = {c: melted[c] for c in melted.columns}

        dim_tables.append({
            'name': dim_name,
            'pk': None,
            'columns': col_series,
            'source_repeating': cols,
            'parent_fk': id_vars[0] if id_vars else None
        })

        # mark original cols as moved out of base
        for c in cols:
            moved[c] = dim_name
 
    # Candidate keys (for partial dependency detection)
    candidate_keys = detect_candidate_keys(df, max_size=2)
    key_sets = [set(k) for k in candidate_keys]

    def _det_table_name(det_cols: Tuple[str, ...]) -> str:

        if len(det_cols) == 1:

            base = det_cols[0]

            if base.endswith("_id"):

                return base[:-3] + "s"  # dept_id -> depts, course_id -> courses

            return f"{base}_dim"

        return "_".join(det_cols) + "_dim"
 
    for det, deps in dependents_by_det.items():

        det_cols = list(det)

        # Check for partial dependencies (2NF): determinant is a proper subset of a candidate composite key
        is_partial = False
        for kset in key_sets:
            if set(det_cols).issubset(kset) and set(det_cols) != kset:
                is_partial = True
                break

        if is_partial:
            # move these dependents into a table keyed by det_cols
            moved_deps = {dep for dep in deps if not _is_measure(dep)}
            if not moved_deps:
                continue
            dim_name = _det_table_name(tuple(det_cols))
            dim_df = df[det_cols + sorted(list(moved_deps))].drop_duplicates()
            dim_pk = det_cols[0] if len(det_cols) == 1 else None
            dim_tables.append({
                "name": dim_name,
                "pk": dim_pk,
                "columns": {c: dim_df[c] for c in dim_df.columns},
                "source_det": det_cols
            })
            for dep in moved_deps:
                moved[dep] = dim_name
            continue

        # ✅ Only allow determinants that look like identifiers

        if not all(_is_identifier(d) for d in det_cols):

            continue
 
        # Ignore determinants that are measures

        if any(_is_measure(d) for d in det_cols):

            continue
 
        moved_deps = {dep for dep in deps if not _is_measure(dep)}

        if not moved_deps:

            continue
 
        dim_name = _det_table_name(det)

        dim_df = df[det_cols + sorted(list(moved_deps))].drop_duplicates()

        dim_pk = det_cols[0] if len(det_cols) == 1 else None
 
        dim_tables.append({

            "name": dim_name,

            "pk": dim_pk,

            "columns": {c: dim_df[c] for c in dim_df.columns},

            "source_det": det_cols

        })

        for dep in moved_deps:

            moved[dep] = dim_name
 
    # Transitive dependency pass (3NF): if a dependent is itself a determinant for other attributes,
    # move those transitively dependent attributes into a table keyed by that dependent.
    det_to_deps = {det: set(deps) for det, deps in dependents_by_det.items()}
    for det_tuple, deps in list(det_to_deps.items()):
        for dep in list(deps):
            # find if 'dep' is also a determinant for others
            for other_det, other_deps in det_to_deps.items():
                if len(other_det) == 1 and other_det[0] == dep:
                    # dep -> other_deps is a transitive chain; create dim table for dep
                    moved_deps = {d for d in other_deps if not _is_measure(d)}
                    if not moved_deps:
                        continue
                    dim_name = _det_table_name((dep,))
                    dim_df = df[[dep] + sorted(list(moved_deps))].drop_duplicates()
                    dim_pk = dep
                    dim_tables.append({
                        "name": dim_name,
                        "pk": dim_pk,
                        "columns": {c: dim_df[c] for c in dim_df.columns},
                        "source_det": [dep]
                    })
                    for md in moved_deps:
                        moved[md] = dim_name
    # Base table: keep PK + transactional columns, remove moved dependents

    base_cols = {c: df[c] for c in df.columns if c not in moved}
 
    # FKs from base to each dimension

    base_fks = []

    for dim in dim_tables:

        for det_col in dim.get("source_det", []):

            if det_col in base_cols:

                base_fks.append({

                    "col": det_col,

                    "ref_table": dim["name"],

                    "ref_col": dim["pk"] or det_col

                })
 
    return {

        "base_table": {"name": table_name, "columns": base_cols, "pk": pk, "fks": base_fks},

        "dimension_tables": dim_tables,

        "moved": moved

    }
 
 
def decompose_tables_to_3nf(tables: Dict[str, pd.DataFrame], analysis: Dict[str, Any]) -> Dict[str, Any]:

    """

    Apply decomposition to each table using FDs, return a new LangGraph-style model dict.

    The output model contains:

    - Each original table (possibly reduced) as base_table

    - New dimension tables created per FD determinant

    - PKs/FKs wired appropriately

    """

    pks = analysis.get('pks', {})

    types = analysis.get('types', {})

    descriptions = analysis.get('descriptions', {})
 
    model = {"tables": {}, "foreign_keys": [], "_meta": {"generated_by": "ModelLangGraph 3NF", "langgraph_compatible": True}}
 
    for tname, df in tables.items():

        pk = pks.get(tname)

        plan = propose_decomposition_for_table(df, tname, pk)
 
        # Add/replace base table

        base = plan["base_table"]

        model["tables"][base["name"]] = {

            "primary_key": base["pk"],

            "columns": {}

        }

        for col, series in base["columns"].items():

            model["tables"][base["name"]]["columns"][col] = {

                "type": types.get(tname, {}).get(col, infer_column_type(series)),

                "description": descriptions.get(tname, {}).get(col),

                "is_primary": (base["pk"] == col)

            }

        # Add base FKs

        for fk in base["fks"]:

            model["foreign_keys"].append({

                "table": base["name"],

                "column": fk["col"],

                "ref_table": fk["ref_table"],

                "ref_column": fk["ref_col"]

            })
 
        # Add dimension tables

        for dim in plan["dimension_tables"]:

            dname = dim["name"]

            model["tables"][dname] = {

                "primary_key": dim["pk"],

                "columns": {}

            }

            # Construct columns from dim["columns"] dict (Series)

            for col, series in dim["columns"].items():

                model["tables"][dname]["columns"][col] = {

                    "type": infer_column_type(series),

                    "description": descriptions.get(tname, {}).get(col),

                    "is_primary": (dim["pk"] == col)

                }
                # If this dimension was created from repeated columns, and includes a parent FK, add FK from dim -> base
            parent_fk = dim.get("parent_fk")
            if parent_fk:
                # ensure the parent FK column exists in the dim table columns
                if parent_fk in model["tables"][dname]["columns"]:
                    model["foreign_keys"].append({
                        "table": dname,
                        "column": parent_fk,
                        "ref_table": base["name"],
                        "ref_column": parent_fk
                    })
 
    return model

def _looks_like_id(col: str, series: pd.Series) -> bool:
    low = col.lower()
    if low == "id" or low.endswith("_id"):
        return True
    try:
        return pd.api.types.is_integer_dtype(series.dropna()) and (series.dropna().nunique() / max(1, len(series)) > 0.5)
    except Exception:
        return False
 
 
def save_langgraph_model_json(model: Dict, out_path: str):
    import json
    with open(out_path, 'w', encoding='utf8') as fh:
        json.dump(model, fh, indent=2)