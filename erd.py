from typing import Dict, Any
 
def generate_dot(model: Dict[str, Any]) -> str:
    """
    Build a Graphviz DOT ERD from model (tables + FKs).
    """
    tables = model.get("tables", {})
    fks = model.get("foreign_keys", [])
 
    lines = ['digraph ERD {', '  rankdir=LR;', '  node [shape=record, fontname=Helvetica];', '']
 
    for t_name, t_meta in tables.items():
        pk = t_meta.get("primary_key")
        cols = t_meta.get("columns", {})
        label_parts = [t_name + "|"]
        # list columns with PK tag
        for c_name, c_meta in cols.items():
            tag = " (PK)" if pk == c_name else ""
            label_parts.append(f'{c_name}{tag}\\l')
        label = "{" + "".join(label_parts) + "}"
        lines.append(f'  "{t_name}" [label="{label}"];')
 
    lines.append("")
    for fk in fks:
        lines.append(
            f'  "{fk["table"]}" -> "{fk["ref_table"]}" [label="{fk["column"]}"];'
        )
 
    lines.append("}")
    return "\n".join(lines)
 
def save_dot(dot: str, out_path: str):
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(dot + "\n")