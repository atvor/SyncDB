class DatabaseConfig:
    """Database configuration loader and manager without dotenv dependency."""

    DEFAULT_USER = "default_user"
    DEFAULT_PASSWORD = "default_password"
    DEFAULT_HOST = "localhost"
    DEFAULT_PORT = "5432"
    DEFAULT_DB_NAME = "default_db"
    DB_SSL_MODE = "prefer"

    def __init__(self, env_file, logger):
        """
        Initialize with the path to an environment file.
        Loads connection parameters directly from the specified file.
        """
        self.logger = logger
        self.env_file = env_file
        self.connection_params = self._load_connection_params()
        self.logger.debug(f"DatabaseConfig initialized with {self.env_file}")

    def _load_connection_params(self):
        """
        Load database connection parameters directly from the environment file.
        Skips lines that are comments or do not contain `=` delimiter.
        """
        connection_params = {}
        try:
            with open(self.env_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        # Handling potential malformed line
                        if '=' not in line:
                            self.logger.warning(
                                f"Skipping malformed line: {line}")
                            continue
                        key, value = line.split("=", 1)
                        connection_params[key] = value

            self.logger.debug(
                f"Environment variables loaded from {self.env_file}")

        except FileNotFoundError:
            self.logger.error(f"Environment file {self.env_file} not found.")
            raise FileNotFoundError(
                f"Environment file {self.env_file} not found.")
        except Exception as e:
            self.logger.error(f"Unexpected error reading {self.env_file}: {e}")
            raise e

        # Populate connection parameters with defaults if missing
        return {
            'db_user_var': connection_params.get("DB_USER", self.DEFAULT_USER),
            'db_password_var': connection_params.get("DB_PASSWORD",
                                                     self.DEFAULT_PASSWORD),
            'db_host_var': connection_params.get("DB_HOST", self.DEFAULT_HOST),
            'db_port_var': connection_params.get("DB_PORT", self.DEFAULT_PORT),
            'db_SSL_mode': connection_params.get("DB_SSL_MODE",
                                                 self.DB_SSL_MODE),
            'db_name_var': connection_params.get("DB_NAME",
                                                 self.DEFAULT_DB_NAME),
        }
