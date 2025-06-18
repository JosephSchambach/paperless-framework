
import os

from ff_framework.context.context_logging import ContextLogger
from ff_framework.database.database_config import DatabaseConfig
from ff_framework.api.formflow_api import FormFlowAPI

class FormFlowContext:
    def __init__(self):
        self.logger = ContextLogger()
        self._get_database()
        self._get_api()
        
    
    def _get_api(self):
        if not hasattr(self, 'database'):
            self.logger.log("Database configuration is not set. Cannot initialize API.", level='error')
            raise ValueError("Database configuration is not set.")
        
        self.api = FormFlowAPI(logger=self.logger, database=self.database)
        self.logger.log("API initialized successfully.", level='info')       
        
    def _get_database(self):
        db_url = os.getenv("DATABASE_URL")
        if db_url is None:
            self.logger.log("DATABASE_URL environment variable is not set.", level='error')
            raise ValueError("DATABASE_URL environment variable is not set.")
        
        db_service_role = os.getenv("DATABASE_SERVICE_ROLE")
        if db_service_role is None:
            self.logger.log("DATABASE_SERVICE_ROLE environment variable is not set.", level='error')
            raise ValueError("DATABASE_SERVICE_ROLE environment variable is not set.")
        
        db_api_key = os.getenv("DATABASE_API_KEY")
        if db_api_key is None:
            self.logger.log("DATABASE_API_KEY environment variable is not set.", level='error')

        self.logger.log("Database configuration loaded successfully.", level='info')
        self.database = DatabaseConfig(
            logger=self.logger,
            db_type="SUPABASE",
            db_url=db_url,
            db_service_role=db_service_role,
            db_api_key=db_api_key
        )