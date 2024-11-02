import psycopg2
from psycopg2 import sql, IntegrityError
from psycopg2.extras import execute_values
from .queries import (
    check_table_exists,
    get_table_schema,
    fetch_primary_keys,
    select_all_rows,
    insert_missing_rows,
    construct_where_clause,
)


class PostgresDatabaseSync:
    def __init__(self,
                 source_config,
                 target_config,
                 logger,
                 connect_timeout=60):
        self.logger = logger
        self.connect_timeout = connect_timeout

        self.logger.debug(
            f"PostgresDatabaseSync Connecting to target db: {target_config.connection_params['db_name_var']}")
        self.target_conn = self.connect(target_config)

        self.logger.debug(
            f"PostgresDatabaseSync Connecting to source db: {source_config.connection_params['db_name_var']}")
        self.source_conn = self.connect(source_config)

        self.logger.debug("PostgresDatabaseSync connected successfully.")

    def connect(self, config):
        """Establish a connection to a PostgreSQL database using the provided configuration."""
        try:
            conn = psycopg2.connect(
                dbname=config.connection_params['db_name_var'],
                user=config.connection_params['db_user_var'],
                password=config.connection_params['db_password_var'],
                host=config.connection_params['db_host_var'],
                port=config.connection_params['db_port_var'],
                sslmode=config.connection_params['db_SSL_mode'],
                connect_timeout=self.connect_timeout
            )
            self.logger.debug(
                f"Connected to database {config.connection_params['db_name_var']}")
            return conn
        except Exception as e:
            self.logger.error(f"Failed to connect: {e}")
            raise

    def check_and_create_table(self, table_name):
        """Check if table exists in the target, and create it if missing based on source schema."""
        with self.target_conn.cursor() as target_cur, self.source_conn.cursor() as source_cur:
            target_cur.execute(*check_table_exists(table_name))
            if not target_cur.fetchone()[0]:
                source_cur.execute(*get_table_schema(table_name))
                columns = source_cur.fetchall()
                columns_sql = ', '.join(
                    [f"{col[0]} {col[1]}" for col in columns])

                create_stmt = sql.SQL("CREATE TABLE {} ({})").format(
                    sql.Identifier(table_name),
                    sql.SQL(columns_sql)
                )
                target_cur.execute(create_stmt)
                self.target_conn.commit()
                self.logger.info(
                    f"Copied schema for {table_name} to target database.")
            else:
                self.logger.info(
                    f"{table_name} already exists in target database.")

    def get_primary_keys(self, table_name):
        """Get primary keys for a table."""
        with self.source_conn.cursor() as cur:
            cur.execute(*fetch_primary_keys(table_name))
            return [row[0] for row in cur.fetchall()]

    def get_missing_rows(self, table_name: str):
        """Retrieve rows present in the source but not in the target database."""
        with self.source_conn.cursor() as source_cur, self.target_conn.cursor() as target_cur:
            primary_keys = self.get_primary_keys(table_name)
            if not primary_keys:
                self.logger.error(
                    f"No primary keys found for table {table_name}.")
                return [], []  # Return empty to avoid further issues.

            select_query, params = select_all_rows(table_name)
            source_cur.execute(select_query, params)

            source_rows = source_cur.fetchall()
            source_columns = [desc[0] for desc in source_cur.description]

            missing_rows = []
            where_clause = construct_where_clause(primary_keys)

            for row in source_rows:
                pk_values = {pk: row[source_columns.index(pk)] for pk in
                             primary_keys}
                target_cur.execute(
                    sql.SQL("SELECT 1 FROM {} WHERE {}").format(
                        sql.Identifier(table_name),
                        where_clause
                    ),
                    list(pk_values.values())
                )
                if not target_cur.fetchone():
                    missing_rows.append(row)
        return missing_rows, source_columns

    def sync_table(self, table_name: str):
        """Insert missing rows from source to target for a specific table."""
        try:
            self.check_and_create_table(table_name)

            missing_rows, source_columns = self.get_missing_rows(table_name)
            if not missing_rows:
                self.logger.info(f"No missing rows to sync for {table_name}")
                return

            with self.target_conn.cursor() as cur:
                insert_stmt = insert_missing_rows(table_name, source_columns)
                execute_values(cur, insert_stmt, missing_rows)
                self.target_conn.commit()
                self.logger.info(
                    f"Synced table {table_name} successfully with {len(missing_rows)} rows.")
        except IntegrityError:
            self.target_conn.rollback()
            self.logger.warning(
                f"Integrity error syncing {table_name}: Duplicate rows detected.")
        except Exception as e:
            self.target_conn.rollback()
            self.logger.error(f"Unexpected error syncing {table_name}: {e}")
            raise

    def sync_all_tables(self):
        """Sync all tables in source database with target database."""
        with self.source_conn.cursor() as cur:
            try:
                cur.execute(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
                tables = [row[0] for row in cur.fetchall()]
                for table_name in tables:
                    self.sync_table(table_name)
                self.logger.info("All tables synced successfully.")
            except Exception as e:
                self.logger.error(f"Failed to sync all tables: {e}")
                raise

    def close_connections(self):
        """Close all database connections gracefully."""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()
