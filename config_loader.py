import yaml
import os
from pathlib import Path
from typing import Dict, Any, Optional
import logging

logger = logging.getLogger(__name__)

class ConfigLoader:
    """
    Configuration loader that reads YAML configuration files
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the config loader

        Args:
            config_path: Path to the configuration file. If None, will look for config.yml in standard locations
        """
        self.config_path = config_path or self._find_config_file()
        self.config: Dict[str, Any] = {}
        self._load_config()

    def _find_config_file(self) -> str:
        """
        Find the configuration file in standard locations
        """
        # Standard locations to look for config.yml
        possible_paths = [
            os.getenv('CONFIG_PATH', ''),
            '/app/config/config.yml',  # Container path (from Dockerfile)
            '/app/config.yml',
            './config.yml',
            './docker/background-tasks/upload-processor/config.yml',
            '/config/config.yml'
        ]

        for path in possible_paths:
            if path and Path(path).exists():
                logger.info(f"Found config file at: {path}")
                return path

        raise FileNotFoundError("Could not find config.yml file in any standard location")

    def _load_config(self):
        """
        Load the YAML configuration file
        """
        try:
            with open(self.config_path, 'r') as file:
                self.config = yaml.safe_load(file)
                logger.info(f"Successfully loaded configuration from {self.config_path}")
        except Exception as e:
            logger.error(f"Failed to load configuration from {self.config_path}: {e}")
            raise

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get a configuration value using dot notation

        Args:
            key: Configuration key using dot notation (e.g., 'sqs.queues.item_master.max_messages')
            default: Default value if key is not found

        Returns:
            Configuration value or default
        """
        keys = key.split('.')
        value = self.config

        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def get_sqs_config(self) -> Dict[str, Any]:
        """Get SQS configuration"""
        return self.get('sqs', {})

    def get_processing_config(self) -> Dict[str, Any]:
        """Get processing configuration"""
        return self.get('processing', {})

    def get_file_processing_config(self) -> Dict[str, Any]:
        """Get file processing configuration"""
        return self.get('file_processing', {})

    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging configuration"""
        return self.get('logging', {})

    def get_database_config(self) -> Dict[str, Any]:
        """Get database configuration"""
        return self.get('database', {})

    def get_health_check_config(self) -> Dict[str, Any]:
        """Get health check configuration"""
        return self.get('health_check', {})

    def get_task_config(self) -> Dict[str, Any]:
        """Get task configuration"""
        return self.get('task', {})

    def reload(self):
        """
        Reload the configuration file
        """
        self._load_config()
 