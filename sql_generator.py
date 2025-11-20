from typing import Dict, Any, List
 
SQL_TYPE_MAP = {
    "INTEGER": "INTEGER",
    "FLOAT": "NUMERIC(38,10)",
    "BOOLEAN": "BOOLEAN",
    "TIMESTAMP": "TIMESTAMP",
    "TEXT": "TEXT",
}
 
def _fmt_col(name: str, col_meta: Dict[str, Any]) -> str:
    sql_type = SQL_TYPE_MAP.get(col_meta.get("type", "TEXT"), "TEXT")
    return f'  "{name}" {sql_type}'
 
def generate_ddl(model: Dict[str, Any]) -> str:
    """
    Generate PostgreSQL-compatible DDL for 3NF: PKs and FKs from model.
    """
    tables = model.get("tables", {})
    fks = model.get("foreign_keys", [])
 
    ddl_lines: List[str] = []
    # Table DDL
    for t_name, t_meta in tables.items():
        cols_lines = []
        for c_name, c_meta in t_meta.get("columns", {}).items():
            cols_lines.append(_fmt_col(c_name, c_meta))
 
        pk = t_meta.get("primary_key")
        if pk:
            cols_lines.append(f'  PRIMARY KEY ("{pk}")')
 
        ddl_lines.append(f'CREATE TABLE "{t_name}" (\n' + ",\n".join(cols_lines) + "\n);\n")
 
    # Foreign keys
    for fk in fks:
        ddl_lines.append(
            f'ALTER TABLE "{fk["table"]}" '
            f'ADD CONSTRAINT "fk_{fk["table"]}_{fk["column"]}_{fk["ref_table"]}" '
            f'FOREIGN KEY ("{fk["column"]}") REFERENCES "{fk["ref_table"]}" ("{fk["ref_column"]}");\n'
        )
 
    # Helpful indexes for FKs (optional)
    for fk in fks:
        ddl_lines.append(
            f'CREATE INDEX "ix_{fk["table"]}_{fk["column"]}" ON "{fk["table"]}" ("{fk["column"]}");\n'
        )
 
    return "\n".join(ddl_lines).strip()
 
def save_ddl(sql: str, out_path: str):
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(sql + "\n")


def generate_normalized_ddl_from_model(model: Dict[str, Any]) -> str:
    """
    Generate a more conventional normalized DDL (companies, locations, customers, phones)
    when the model suggests a customer-like table with company/location/phone/email columns.
    This produces portable SQL (MySQL/Postgres-compatible with SERIAL/auto-increment notes).
    """
    # Find a candidate customer table (heuristic)
    tables = model.get("tables", {})
    customer_table = None
    for tname, tmeta in tables.items():
        cols = [c.lower() for c in tmeta.get("columns", {}).keys()]
        if any(x in cols for x in ["first name", "firstname", "last name", "lastname"]) and (
            "company" in cols or "city" in cols or "country" in cols
        ):
            customer_table = (tname, tmeta)
            break

    if not customer_table:
        # fallback: pick first table
        if tables:
            customer_table = next(iter(tables.items()))
        else:
            return "-- No tables found in model to generate normalized DDL"

    tname, tmeta = customer_table
    cols = {c: meta for c, meta in tmeta.get("columns", {}).items()}

    # Helper to check existence ignoring case
    def has(col_names):
        for name in col_names:
            for c in cols.keys():
                if c.lower().replace(' ', '') == name.lower().replace(' ', ''):
                    return c
        return None

    cust_id_col = has(["Customer Id", "customer id", "customerid", "id"]) or None
    first_col = has(["First Name", "firstname"]) or None
    last_col = has(["Last Name", "lastname"]) or None
    email_col = has(["Email", "email_address"]) or None
    sub_col = has(["Subscription Date", "subscriptiondate", "subscription_date"]) or None
    website_col = has(["Website", "website"]) or None
    company_col = has(["Company", "company"]) or None
    city_col = has(["City", "city"]) or None
    country_col = has(["Country", "country"]) or None

    # phone child table detection: search for a child table with 'phone' in its name
    phone_table = None
    for dn, dmeta in tables.items():
        if "phone" in dn.lower() or any("phone" in c.lower() for c in dmeta.get("columns", {})):
            phone_table = dn
            break

    lines = []

    # Company table
    lines.append("CREATE TABLE Company (")
    lines.append("    CompanyId SERIAL PRIMARY KEY,")
    lines.append("    CompanyName VARCHAR(255),")
    if website_col:
        lines.append("    Website VARCHAR(255)")
    else:
        lines.append("    Website VARCHAR(255)")
    lines.append(");\n")

    # Location table
    lines.append("CREATE TABLE Location (")
    lines.append("    LocationId SERIAL PRIMARY KEY,")
    lines.append("    City VARCHAR(100),")
    lines.append("    Country VARCHAR(100)")
    lines.append(");\n")

    # Customer table
    lines.append("CREATE TABLE Customer (")
    if cust_id_col:
        lines.append(f"    CustomerId VARCHAR(50) PRIMARY KEY,")
    else:
        lines.append("    CustomerId SERIAL PRIMARY KEY,")
    if first_col:
        lines.append("    FirstName VARCHAR(50),")
    else:
        lines.append("    FirstName VARCHAR(50),")
    if last_col:
        lines.append("    LastName VARCHAR(50),")
    else:
        lines.append("    LastName VARCHAR(50),")
    if email_col:
        lines.append("    Email VARCHAR(255) UNIQUE,")
    else:
        lines.append("    Email VARCHAR(255),")
    if sub_col:
        lines.append("    SubscriptionDate DATE,")
    else:
        lines.append("    SubscriptionDate DATE,")
    lines.append("    CompanyId INT,")
    lines.append("    LocationId INT,")
    lines.append("    FOREIGN KEY (CompanyId) REFERENCES Company(CompanyId),")
    lines.append("    FOREIGN KEY (LocationId) REFERENCES Location(LocationId)")
    lines.append(");\n")

    # Phone table
    lines.append("CREATE TABLE Phone (")
    lines.append("    PhoneId SERIAL PRIMARY KEY,")
    lines.append("    CustomerId VARCHAR(50),")
    lines.append("    PhoneNumber VARCHAR(50),")
    lines.append("    PhoneType VARCHAR(20),")
    lines.append("    FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId)")
    lines.append(");\n")

    return "\n".join(lines)


def save_normalized_ddl(sql: str, out_path: str):
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(sql + "\n")


def generate_migration_sql_from_model(model: Dict[str, Any], tables: Dict[str, Any]) -> str:
    """
    Generate simple migration SQL that assumes each source table is available as a staging table
    with the same name as the CSV-derived table (e.g., "customers-100").

    The migration will:
    - INSERT DISTINCT companies into `Company`
    - INSERT DISTINCT locations into `Location`
    - INSERT customers/people/organizations into `Customer`/`Person`/`Organization`
    - UNPIVOT phone columns into `Phone`

    This SQL is intentionally straightforward and may need tuning for your database dialect.
    """
    lines: List[str] = []

    for tname, tmeta in model.get("tables", {}).items():
        lower_cols = {c.lower(): c for c in tmeta.get("columns", {}).keys()}

        # identify roles
        has_company = any(x in lower_cols for x in ["company"])
        has_city = any(x in lower_cols for x in ["city"]) and any(x in lower_cols for x in ["country"])
        has_person = any(x in lower_cols for x in ["first name", "firstname"]) or any(x in lower_cols for x in ["last name", "lastname"]) or any(x in lower_cols for x in ["email"]) 

        # canonical staging table name quoting
        stag = f'"{tname}"'

        # Companies
        if has_company:
            company_col = lower_cols.get("company")
            website_col = lower_cols.get("website")
            lines.append(f"-- Populate Company from {tname}")
            if website_col:
                lines.append(f"INSERT INTO Company (CompanyName, Website) SELECT DISTINCT {company_col}, {website_col} FROM {stag} WHERE {company_col} IS NOT NULL;")
            else:
                lines.append(f"INSERT INTO Company (CompanyName) SELECT DISTINCT {company_col} FROM {stag} WHERE {company_col} IS NOT NULL;")
            lines.append("")

        # Locations
        if has_city:
            city_col = lower_cols.get("city")
            country_col = lower_cols.get("country")
            lines.append(f"-- Populate Location from {tname}")
            lines.append(f"INSERT INTO Location (City, Country) SELECT DISTINCT {city_col}, {country_col} FROM {stag} WHERE {city_col} IS NOT NULL OR {country_col} IS NOT NULL;")
            lines.append("")

        # Customers/People/Organizations
        if has_person or has_company:
            # Map to Customer table name for customers/people/organizations
            lines.append(f"-- Populate Customer/Person-like entities from {tname}")
            # pick id column if exists
            id_col = None
            for candidate in ["customer id", "customerid", "id", "person id", "personid", "organization id", "organizationid"]:
                if candidate in lower_cols:
                    id_col = lower_cols[candidate]
                    break

            first_col = lower_cols.get("first name") or lower_cols.get("firstname")
            last_col = lower_cols.get("last name") or lower_cols.get("lastname")
            email_col = lower_cols.get("email")
            sub_col = lower_cols.get("subscription date")
            company_col = lower_cols.get("company")
            city_col = lower_cols.get("city")
            country_col = lower_cols.get("country")

            insert_cols = []
            select_cols = []
            if id_col:
                insert_cols.append("CustomerId")
                select_cols.append(id_col)
            else:
                insert_cols.append("CustomerId")
                select_cols.append("NULL")
            insert_cols.extend(["FirstName", "LastName", "Email", "SubscriptionDate", "CompanyId", "LocationId"])
            select_parts = [first_col or 'NULL', last_col or 'NULL', email_col or 'NULL', sub_col or 'NULL']

            # CompanyId and LocationId would be looked up via joins in a real migration; produce TODO comments
            select_parts.extend(['NULL', 'NULL'])

            lines.append(f"-- NOTE: CompanyId/LocationId need lookup joins; this INSERT uses NULLs as placeholders")
            lines.append(f"INSERT INTO Customer ({', '.join(insert_cols)}) SELECT {', '.join(select_parts)} FROM {stag} WHERE 1=1;")
            lines.append("")

        # Phones: detect repeating phone columns like Phone 1, Phone 2
        phone_cols = [c for c in tmeta.get("columns", {}).keys() if c.lower().replace(' ', '').startswith('phone')]
        if phone_cols:
            lines.append(f"-- Unpivot phone columns from {tname} into Phone")
            union_parts = []
            id_candidate = None
            for cand in ["customer id", "customerid", "id", "person id", "personid"]:
                if cand in lower_cols:
                    id_candidate = lower_cols[cand]
                    break
            id_expr = id_candidate or 'NULL'
            for pc in phone_cols:
                union_parts.append(f"SELECT {id_expr} AS CustomerId, {pc} AS PhoneNumber, '{pc}' AS PhoneType FROM {stag} WHERE {pc} IS NOT NULL")
            lines.append("INSERT INTO Phone (CustomerId, PhoneNumber, PhoneType) " + "\nUNION ALL\n".join(union_parts) + ";")
            lines.append("")

    return "\n".join(lines) if lines else "-- No migration steps generated"


def save_migration_sql(sql: str, out_path: str):
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write(sql + "\n")