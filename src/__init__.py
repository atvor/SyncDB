import logging

from src.syncDB import DatabaseConfig, PostgresDatabaseSync
from src.setup_logger import setup_logger

prod_env_path = '.environments/.env.production'
dev_env_path = '.environments/.env.development'

logger = setup_logger("SyncDB",
                      folder_path=".temp",
                      logger_name="syncDB",
                      level=logging.DEBUG)


def main():
    logger.info('Syncing databases')

    # Load configurations for production and development using DatabaseConfig
    prod_db_config = DatabaseConfig(prod_env_path, logger)
    dev_db_config = DatabaseConfig(dev_env_path, logger)

    # Initialize the sync object and sync databases
    db_sync = PostgresDatabaseSync(source_config=prod_db_config,
                                   target_config=dev_db_config,
                                   logger=logger)
    db_sync.sync_all_tables()


if __name__ == "__main__":
    main()
