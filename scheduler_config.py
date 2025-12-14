"""
Scheduler Configuration and Logging Setup Module

This module handles logging configuration and other shared configuration
for the scheduler system to avoid global side effects in the main module.
"""

import logging
import os
import sys
from typing import Optional


def setup_logging(log_level: int = logging.DEBUG, 
                  log_directory: str = "logs",
                  log_filename: str = "scheduler.log") -> bool:
    """
    Configure logging for the scheduler system.
    
    Args:
        log_level: Logging level (default: DEBUG)
        log_directory: Directory for log files (default: "logs")
        log_filename: Name of the log file (default: "scheduler.log")
        
    Returns:
        bool: True if logging setup was successful, False otherwise
    """
    try:
        # Ensure log directory exists
        if not os.path.exists(log_directory):
            try:
                os.makedirs(log_directory)
                print(f"Log directory '{log_directory}' created successfully.")
            except OSError as e:
                print(f"Error creating log directory '{log_directory}': {e}. Using current directory.")
                log_directory = "."
        
        log_file_path = os.path.join(log_directory, log_filename)
        print(f"Logging to: {os.path.abspath(log_file_path)}")
        
        # Configure root logger
        logger = logging.getLogger()
        logger.setLevel(log_level)
        
        # Remove any existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
            handler.close()
        
        # Create and add handlers
        # File handler with UTF-8 encoding
        file_handler = logging.FileHandler(
            log_file_path, 
            mode='a', 
            encoding='utf-8', 
            errors='replace'
        )
        file_handler.setLevel(log_level)
        file_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
        
        # Console handler with UTF-8 encoding if possible
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(log_level)
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_formatter)
        
        # Try to configure console for UTF-8
        if hasattr(console_handler.stream, 'reconfigure'):
            try:
                console_handler.stream.reconfigure(encoding='utf-8', errors='replace')
            except Exception as e:
                print(f"Could not reconfigure console stream encoding: {e}")
        
        logger.addHandler(console_handler)
        
        # Test log message to verify configuration
        logging.info("Logging system configured successfully.")
        return True
        
    except Exception as e:
        print(f"ERROR: Failed to configure logging: {e}")
        # Set up a basic console logger as fallback
        logging.basicConfig(level=log_level, format='%(levelname)s: %(message)s')
        logging.error(f"Failed to set up file logging. Logs will only appear in console. Error: {e}")
        return False


class SchedulerConfig:
    """
    Configuration class for scheduler settings and constants.
    """
    
    # Default configuration values
    DEFAULT_GAP_BETWEEN_SHIFTS = 3
    DEFAULT_MAX_CONSECUTIVE_WEEKENDS = 3
    DEFAULT_NUM_SHIFTS = 3
    DEFAULT_OPTIMIZATION_LOOPS = 70
    DEFAULT_LAST_POST_ADJUSTMENT_ITERATIONS = 5
    
    # Performance optimization settings
    CACHE_ENABLED = True
    LAZY_EVALUATION = True
    BATCH_SIZE = 100
    
    # Logging configuration
    LOG_LEVEL = logging.DEBUG
    LOG_DIRECTORY = "logs"
    LOG_FILENAME = "scheduler.log"
    
    @classmethod
    def get_default_config(cls) -> dict:
        """
        Get default configuration dictionary.
        
        Returns:
            dict: Default configuration settings
        """
        return {
            'gap_between_shifts': cls.DEFAULT_GAP_BETWEEN_SHIFTS,
            'max_consecutive_weekends': cls.DEFAULT_MAX_CONSECUTIVE_WEEKENDS,
            'num_shifts': cls.DEFAULT_NUM_SHIFTS,
            'max_improvement_loops': cls.DEFAULT_OPTIMIZATION_LOOPS,
            'last_post_adjustment_max_iterations': cls.DEFAULT_LAST_POST_ADJUSTMENT_ITERATIONS,
            'cache_enabled': cls.CACHE_ENABLED,
            'lazy_evaluation': cls.LAZY_EVALUATION,
            'batch_size': cls.BATCH_SIZE
        }
    
    @classmethod
    def validate_config(cls, config: dict) -> tuple[bool, Optional[str]]:
        """
        Validate configuration dictionary.
        
        Args:
            config: Configuration dictionary to validate
            
        Returns:
            tuple: (is_valid, error_message)
        """
        required_fields = ['start_date', 'end_date', 'num_shifts', 'workers_data']
        
        for field in required_fields:
            if field not in config:
                return False, f"Missing required configuration field: {field}"
        
        # Validate types and ranges
        if not isinstance(config.get('num_shifts', 0), int) or config['num_shifts'] < 1:
            return False, "num_shifts must be a positive integer"
        
        if config.get('gap_between_shifts', 0) < 0:
            return False, "gap_between_shifts must be non-negative"
        
        if config.get('max_consecutive_weekends', 0) <= 0:
            return False, "max_consecutive_weekends must be positive"
        
        return True, None