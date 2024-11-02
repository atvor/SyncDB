from psycopg2 import sql


def check_table_exists(table_name):
    """Returns a query to check if a table exists."""
    return sql.SQL(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s);"), [
        table_name]


def get_table_schema(table_name):
    """Returns a query to get the schema (column names and types) of a table."""
    return sql.SQL(
        "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s;"), [
        table_name]


def fetch_primary_keys(table_name):
    """Returns a query to fetch the primary key columns of a table."""
    return sql.SQL(
        "SELECT a.attname FROM pg_index i "
        "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
        "WHERE i.indrelid = %s::regclass AND i.indisprimary;"
    ), [table_name]


def select_all_rows(table_name):
    """Returns a query to select all rows from a table."""
    if not table_name:
        raise ValueError("Table name must be provided")
    return sql.SQL("SELECT * FROM {}").format(sql.Identifier(table_name)), []


def insert_missing_rows(table_name, columns):
    """Returns an INSERT query with ON CONFLICT DO NOTHING for bulk insertion."""
    return sql.SQL(
        "INSERT INTO {} ({}) VALUES %s ON CONFLICT DO NOTHING"
    ).format(
        sql.Identifier(table_name),
        sql.SQL(", ").join(map(sql.Identifier, columns))
    )


def construct_where_clause(primary_keys):
    """Constructs a WHERE clause for primary keys to check for row existence."""
    if not primary_keys:
        return sql.SQL(
            "TRUE")  # Safe fallback; returns all rows as condition is always true.
    return sql.SQL(" AND ").join(
        sql.Composed([
            sql.Identifier(k), sql.SQL("= %s")
        ]) for k in primary_keys
    )
