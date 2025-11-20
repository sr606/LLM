import json

from typing import Dict, Any
 
def build_catalog(tables: Dict[str, Any], analysis: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:

    """

    Catalog combines: table columns, inferred types, PKs/FKs, and simple profiles.

    `tables` are pandas DataFrames (caller should convert profiles to plain dicts).

    """

    types = analysis.get("types", {})

    pks = analysis.get("pks", {})

    fks = analysis.get("fks", {})

    descriptions = analysis.get("descriptions", {})
 
    fk_list = [

        {"table": t, "column": c, "ref_table": rt, "ref_column": rc}

        for (t, c), (rt, rc) in fks.items()

    ]
 
    cat = {

        "model_meta": model.get("_meta", {}),

        "tables": {},

        "primary_keys": pks,

        "foreign_keys": fk_list,

    }
 
    for t_name, df in tables.items():

        cols = list(df.columns)

        cat["tables"][t_name] = {

            "columns": cols,

            "types": types.get(t_name, {}),

            "primary_key": pks.get(t_name),

            "descriptions": descriptions.get(t_name, {}),

            # lightweight profiles (caller may replace with richer stats)

            "profile": {

                c: {

                    "rows": int(len(df[c])),

                    "distinct": int(df[c].nunique(dropna=True)),

                    "nulls": int(df[c].isna().sum()),

                    "sample_values": df[c].dropna().astype(str).head(5).tolist(),

                }

                for c in cols

            },

        }

    return cat
 
def save_catalog(catalog: Dict[str, Any], out_path: str):

    with open(out_path, "w", encoding="utf-8") as fh:

        json.dump(catalog, fh, indent=2)

 