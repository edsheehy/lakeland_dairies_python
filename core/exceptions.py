#!/usr/bin/env python3
"""
Custom exceptions for Lakeland Dairies Batch Processing System
"""

from typing import Optional, Dict, Any
from core.enums import ErrorCodes, SystemComponent


class LakelandBatchException(Exception):
    """Base exception class for all Lakeland Batch System errors"""
    
    def __init__(self, message: str, error_code: Optional[ErrorCodes] = None, 
                 component: Optional[SystemComponent] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.component = component
        self.details = details or {}
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for logging/serialization"""
        return {
            'exception_type': self.__class__.__name__,
            'message': self.message,
            'error_code': self.error_code.value if self.error_code else None,
            'component': self.component.value if self.component else None,
            'details': self.details
        }
    
    def __str__(self) -> str:
        parts = [self.message]
        if self.component:
            parts.append(f"Component: {self.component.value}")
        if self.error_code:
            parts.append(f"Error Code: {self.error_code.value}")
        return " | ".join(parts)


class ConnectionException(LakelandBatchException):
    """Exception for connection-related errors"""
    
    def __init__(self, message: str, component: Optional[SystemComponent] = None, 
                 host: Optional[str] = None, port: Optional[int] = None, **kwargs):
        details = kwargs.get('details', {})
        if host:
            details['host'] = host
        if port:
            details['port'] = port
        
        super().__init__(message, component=component, details=details, **kwargs)
        self.host = host
        self.port = port


class ModbusException(ConnectionException):
    """Exception for Modbus communication errors"""
    
    def __init__(self, message: str, register: Optional[int] = None, 
                 slave_id: Optional[int] = None, **kwargs):
        details = kwargs.get('details', {})
        if register:
            details['register'] = register
        if slave_id:
            details['slave_id'] = slave_id
            
        super().__init__(message, component=SystemComponent.MODBUS_CLIENT, **kwargs)
        self.register = register
        self.slave_id = slave_id


class FirebaseException(ConnectionException):
    """Exception for Firebase communication errors"""
    
    def __init__(self, message: str, url: Optional[str] = None, 
                 status_code: Optional[int] = None, **kwargs):
        details = kwargs.get('details', {})
        if url:
            details['url'] = url
        if status_code:
            details['status_code'] = status_code
            
        super().__init__(message, component=SystemComponent.FIREBASE_CLIENT, 
                        error_code=ErrorCodes.FIREBASE_FAIL, **kwargs)
        self.url = url
        self.status_code = status_code


class ZanasiException(ConnectionException):
    """Exception for Zanasi printer communication errors"""
    
    def __init__(self, message: str, printhead: Optional[int] = None, 
                 command: Optional[str] = None, **kwargs):
        details = kwargs.get('details', {})
        if printhead:
            details['printhead'] = printhead
        if command:
            details['command'] = command
            
        super().__init__(message, component=SystemComponent.ZANASI_CLIENT, 
                        error_code=ErrorCodes.ZANASI_COMM_FAIL, **kwargs)
        self.printhead = printhead
        self.command = command


class DataValidationException(LakelandBatchException):
    """Exception for data validation errors"""
    
    def __init__(self, message: str, field: Optional[str] = None, 
                 value: Optional[Any] = None, validation_errors: Optional[list] = None, **kwargs):
        details = kwargs.get('details', {})
        if field:
            details['field'] = field
        if value is not None:
            details['value'] = str(value)
        if validation_errors:
            details['validation_errors'] = validation_errors
            
        super().__init__(message, error_code=ErrorCodes.DATA_FORMAT_ERROR, **kwargs)
        self.field = field
        self.value = value
        self.validation_errors = validation_errors or []


class BatchProcessingException(LakelandBatchException):
    """Exception for batch processing logic errors"""
    
    def __init__(self, message: str, batch_index: Optional[int] = None, 
                 operation: Optional[str] = None, **kwargs):
        details = kwargs.get('details', {})
        if batch_index:
            details['batch_index'] = batch_index
        if operation:
            details['operation'] = operation
            
        super().__init__(message, component=SystemComponent.BATCH_MANAGER, **kwargs)
        self.batch_index = batch_index
        self.operation = operation


class RegisterException(LakelandBatchException):
    """Exception for register mapping and conversion errors"""
    
    def __init__(self, message: str, register: Optional[int] = None, 
                 register_range: Optional[tuple] = None, **kwargs):
        details = kwargs.get('details', {})
        if register:
            details['register'] = register
        if register_range:
            details['register_range'] = register_range
            
        super().__init__(message, error_code=ErrorCodes.DATA_FORMAT_ERROR, **kwargs)
        self.register = register
        self.register_range = register_range


class ConfigurationException(LakelandBatchException):
    """Exception for configuration and settings errors"""
    
    def __init__(self, message: str, config_section: Optional[str] = None, 
                 config_key: Optional[str] = None, **kwargs):
        details = kwargs.get('details', {})
        if config_section:
            details['config_section'] = config_section
        if config_key:
            details['config_key'] = config_key
            
        super().__init__(message, **kwargs)
        self.config_section = config_section
        self.config_key = config_key


class TimeoutException(LakelandBatchException):
    """Exception for timeout-related errors"""
    
    def __init__(self, message: str, timeout_seconds: Optional[float] = None, 
                 operation: Optional[str] = None, **kwargs):
        details = kwargs.get('details', {})
        if timeout_seconds:
            details['timeout_seconds'] = timeout_seconds
        if operation:
            details['operation'] = operation
            
        super().__init__(message, **kwargs)
        self.timeout_seconds = timeout_seconds
        self.operation = operation


class RetryExhaustedException(LakelandBatchException):
    """Exception when retry attempts are exhausted"""
    
    def __init__(self, message: str, max_attempts: Optional[int] = None, 
                 last_error: Optional[Exception] = None, **kwargs):
        details = kwargs.get('details', {})
        if max_attempts:
            details['max_attempts'] = max_attempts
        if last_error:
            details['last_error'] = str(last_error)
            details['last_error_type'] = type(last_error).__name__
            
        super().__init__(message, **kwargs)
        self.max_attempts = max_attempts
        self.last_error = last_error


class StateException(LakelandBatchException):
    """Exception for invalid state transitions or operations"""
    
    def __init__(self, message: str, current_state: Optional[Any] = None, 
                 attempted_operation: Optional[str] = None, **kwargs):
        details = kwargs.get('details', {})
        if current_state is not None:
            details['current_state'] = str(current_state)
        if attempted_operation:
            details['attempted_operation'] = attempted_operation
            
        super().__init__(message, **kwargs)
        self.current_state = current_state
        self.attempted_operation = attempted_operation


class CriticalSystemException(LakelandBatchException):
    """Exception for critical system errors that require immediate attention"""
    
    def __init__(self, message: str, requires_restart: bool = False, 
                 system_state: Optional[Dict[str, Any]] = None, **kwargs):
        details = kwargs.get('details', {})
        details['requires_restart'] = requires_restart
        if system_state:
            details['system_state'] = system_state
            
        super().__init__(message, **kwargs)
        self.requires_restart = requires_restart
        self.system_state = system_state


# Exception context managers for common operations
class ExceptionContext:
    """Context manager for handling exceptions with additional context"""
    
    def __init__(self, operation: str, component: Optional[SystemComponent] = None, 
                 reraise_as: Optional[type] = None):
        self.operation = operation
        self.component = component
        self.reraise_as = reraise_as or LakelandBatchException
        
    def __enter__(self):
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None and not issubclass(exc_type, LakelandBatchException):
            # Wrap non-Lakeland exceptions
            message = f"Error during {self.operation}: {str(exc_val)}"
            details = {
                'original_exception': str(exc_val),
                'original_exception_type': exc_type.__name__
            }
            
            wrapped_exception = self.reraise_as(
                message=message,
                component=self.component,
                details=details
            )
            
            # Preserve original traceback
            raise wrapped_exception from exc_val
        
        return False  # Don't suppress Lakeland exceptions


# Utility functions for exception handling
def handle_connection_error(func):
    """Decorator for handling connection errors"""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ConnectionException:
            raise  # Re-raise connection exceptions as-is
        except Exception as e:
            # Wrap other exceptions as connection errors
            raise ConnectionException(f"Connection error in {func.__name__}: {str(e)}") from e
    return wrapper


def validate_and_raise(condition: bool, exception_class: type, message: str, **kwargs):
    """Helper function to validate condition and raise exception if false"""
    if not condition:
        raise exception_class(message, **kwargs)


def format_exception_for_logging(exception: Exception) -> Dict[str, Any]:
    """Format exception for structured logging"""
    if isinstance(exception, LakelandBatchException):
        return exception.to_dict()
    else:
        return {
            'exception_type': type(exception).__name__,
            'message': str(exception),
            'details': {}
        }
