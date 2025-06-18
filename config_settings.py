#!/usr/bin/env python3
"""
Configuration settings for Lakeland Dairies Batch Processing System
"""

import os
import json
from dataclasses import dataclass
from typing import Optional
from pathlib import Path


@dataclass
class ModbusConfig:
    """Modbus PLC connection configuration"""
    host: str = "10.100.1.20"
    port: int = 502
    slave_id: int = 1
    timeout: float = 5.0
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class ZanasiConfig:
    """Zanasi printer configuration"""
    host: str = "10.100.1.10"
    printhead1_port: int = 43110
    printhead2_port: int = 43111
    timeout: float = 10.0
    command_delay: float = 0.1
    retry_attempts: int = 2


@dataclass
class FirebaseConfig:
    """Firebase connection configuration"""
    url: str = "https://getbatches-r3r2ldlmza-ew.a.run.app/?okwenClient=lakeland_dairies"
    timeout: float = 10.0
    retry_attempts: int = 3
    retry_delay: float = 2.0


@dataclass
class ProcessingConfig:
    """Batch processing configuration"""
    polling_interval: float = 1.0
    max_batches: int = 5
    batch_registers_per_batch: int = 20
    total_registers: int = 120


@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    log_dir: str = "~/logs"
    log_file: str = "batch_processor.log"
    max_file_size: int = 10485760  # 10MB
    backup_count: int = 5
    console_output: bool = True


@dataclass
class ServiceConfig:
    """Service runtime configuration"""
    run_as_daemon: bool = True
    pid_file: str = "/var/run/lakeland_batch_processor.pid"
    working_directory: str = "/opt/lakeland_batch_system"
    user: Optional[str] = None
    group: Optional[str] = None


class Settings:
    """Main settings class that loads and manages all configuration"""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or self._get_default_config_path()
        
        # Initialize with defaults
        self.modbus = ModbusConfig()
        self.zanasi = ZanasiConfig()
        self.firebase = FirebaseConfig()
        self.processing = ProcessingConfig()
        self.logging = LoggingConfig()
        self.service = ServiceConfig()
        
        # Load from config file if it exists
        self._load_config()
        
        # Override with environment variables
        self._load_environment_variables()
    
    def _get_default_config_path(self) -> str:
        """Get default configuration file path"""
        # Look for config in multiple locations
        possible_paths = [
            "./config/lakeland_batch_config.json",
            "/etc/lakeland_batch_system/config.json",
            "~/lakeland_batch_config.json"
        ]
        
        for path in possible_paths:
            expanded_path = Path(path).expanduser().absolute()
            if expanded_path.exists():
                return str(expanded_path)
        
        # Return first path as default (will be created if needed)
        return str(Path(possible_paths[0]).expanduser().absolute())
    
    def _load_config(self):
        """Load configuration from JSON file"""
        config_file = Path(self.config_path)
        
        if not config_file.exists():
            print(f"Config file {self.config_path} not found, using defaults")
            return
        
        try:
            with open(config_file, 'r') as f:
                config_data = json.load(f)
            
            # Update configurations
            if 'modbus' in config_data:
                self._update_dataclass(self.modbus, config_data['modbus'])
            
            if 'zanasi' in config_data:
                self._update_dataclass(self.zanasi, config_data['zanasi'])
            
            if 'firebase' in config_data:
                self._update_dataclass(self.firebase, config_data['firebase'])
            
            if 'processing' in config_data:
                self._update_dataclass(self.processing, config_data['processing'])
            
            if 'logging' in config_data:
                self._update_dataclass(self.logging, config_data['logging'])
            
            if 'service' in config_data:
                self._update_dataclass(self.service, config_data['service'])
                
            print(f"Loaded configuration from {self.config_path}")
            
        except Exception as e:
            print(f"Error loading config file {self.config_path}: {e}")
            print("Using default configuration")
    
    def _update_dataclass(self, instance, data):
        """Update dataclass instance with dictionary data"""
        for key, value in data.items():
            if hasattr(instance, key):
                setattr(instance, key, value)
    
    def _load_environment_variables(self):
        """Load configuration from environment variables"""
        # Modbus settings
        if os.getenv('MODBUS_HOST'):
            self.modbus.host = os.getenv('MODBUS_HOST')
        if os.getenv('MODBUS_PORT'):
            self.modbus.port = int(os.getenv('MODBUS_PORT'))
        
        # Zanasi settings
        if os.getenv('ZANASI_HOST'):
            self.zanasi.host = os.getenv('ZANASI_HOST')
        if os.getenv('ZANASI_PH1_PORT'):
            self.zanasi.printhead1_port = int(os.getenv('ZANASI_PH1_PORT'))
        if os.getenv('ZANASI_PH2_PORT'):
            self.zanasi.printhead2_port = int(os.getenv('ZANASI_PH2_PORT'))
        
        # Firebase settings
        if os.getenv('FIREBASE_URL'):
            self.firebase.url = os.getenv('FIREBASE_URL')
        
        # Processing settings
        if os.getenv('POLLING_INTERVAL'):
            self.processing.polling_interval = float(os.getenv('POLLING_INTERVAL'))
        
        # Logging settings
        if os.getenv('LOG_LEVEL'):
            self.logging.level = os.getenv('LOG_LEVEL')
        if os.getenv('LOG_DIR'):
            self.logging.log_dir = os.getenv('LOG_DIR')
    
    def create_sample_config(self, path: Optional[str] = None):
        """Create a sample configuration file"""
        sample_path = path or self.config_path
        
        sample_config = {
            "modbus": {
                "host": self.modbus.host,
                "port": self.modbus.port,
                "slave_id": self.modbus.slave_id,
                "timeout": self.modbus.timeout,
                "retry_attempts": self.modbus.retry_attempts,
                "retry_delay": self.modbus.retry_delay
            },
            "zanasi": {
                "host": self.zanasi.host,
                "printhead1_port": self.zanasi.printhead1_port,
                "printhead2_port": self.zanasi.printhead2_port,
                "timeout": self.zanasi.timeout,
                "command_delay": self.zanasi.command_delay,
                "retry_attempts": self.zanasi.retry_attempts
            },
            "firebase": {
                "url": self.firebase.url,
                "timeout": self.firebase.timeout,
                "retry_attempts": self.firebase.retry_attempts,
                "retry_delay": self.firebase.retry_delay
            },
            "processing": {
                "polling_interval": self.processing.polling_interval,
                "max_batches": self.processing.max_batches,
                "batch_registers_per_batch": self.processing.batch_registers_per_batch,
                "total_registers": self.processing.total_registers
            },
            "logging": {
                "level": self.logging.level,
                "format": self.logging.format,
                "log_dir": self.logging.log_dir,
                "log_file": self.logging.log_file,
                "max_file_size": self.logging.max_file_size,
                "backup_count": self.logging.backup_count,
                "console_output": self.logging.console_output
            },
            "service": {
                "run_as_daemon": self.service.run_as_daemon,
                "pid_file": self.service.pid_file,
                "working_directory": self.service.working_directory,
                "user": self.service.user,
                "group": self.service.group
            }
        }
        
        # Ensure directory exists
        Path(sample_path).parent.mkdir(parents=True, exist_ok=True)
        
        with open(sample_path, 'w') as f:
            json.dump(sample_config, f, indent=4)
        
        print(f"Sample configuration created at {sample_path}")
    
    def validate(self) -> bool:
        """Validate configuration settings"""
        errors = []
        
        # Validate network addresses
        if not self.modbus.host or not self.zanasi.host:
            errors.append("Network hosts cannot be empty")
        
        # Validate ports
        if not (1 <= self.modbus.port <= 65535):
            errors.append("Modbus port must be between 1024 and 65535")

        if not (1024 <= self.zanasi.printhead1_port <= 65535):
            errors.append("Zanasi printhead1 port must be between 1024 and 65535")
        
        if not (1024 <= self.zanasi.printhead2_port <= 65535):
            errors.append("Zanasi printhead2 port must be between 1024 and 65535")
        
        # Validate URLs
        if not self.firebase.url.startswith(('http://', 'https://')):
            errors.append("Firebase URL must be a valid HTTP/HTTPS URL")
        
        # Validate processing settings
        if self.processing.polling_interval <= 0:
            errors.append("Polling interval must be positive")
        
        if self.processing.max_batches <= 0:
            errors.append("Max batches must be positive")
        
        # Validate logging settings
        valid_log_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if self.logging.level not in valid_log_levels:
            errors.append(f"Log level must be one of: {valid_log_levels}")
        
        if errors:
            for error in errors:
                print(f"Configuration error: {error}")
            return False
        
        return True


# Global settings instance
settings = Settings()
