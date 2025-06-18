#!/usr/bin/env python3
"""
Enumerations for Lakeland Dairies Batch Processing System
"""

from enum import Enum, IntEnum


class TriggerStates(IntEnum):
    """PLC trigger states for batch operations"""
    IDLE = 0
    DOWNLOAD_BATCH = 1
    LOAD_TO_ZANASI = 2


class ProcessingStates(IntEnum):
    """Raspberry Pi processing states"""
    IDLE = 0
    DOWNLOADING = 1
    PROCESSING_DATA = 2
    READY_TO_SEND = 3
    SENDING_TO_ZANASI = 4
    COMPLETE = 5
    ERROR = 9


class PLCStates(IntEnum):
    """PLC internal states"""
    IDLE = 0
    TRIGGERING_DOWNLOAD = 1
    WAITING_FOR_DATA = 2
    DATA_RECEIVED = 3
    DISPLAYING = 4
    READY_TO_LOAD = 5


class BatchStates(IntEnum):
    """Individual batch status states"""
    QUEUED = 0              # Orange in HMI - can be modified
    NEXT_IN_QUEUE = 1       # Orange with Load button - can be modified
    CURRENT_PRINTING = 2    # Blue with End button - read-only
    LAST_PRINTED = 3        # Green with Resume button - read-only
    PRINTED = 4             # Green, completed - read-only


class ErrorCodes(IntEnum):
    """System error codes"""
    NO_ERROR = 0
    FIREBASE_FAIL = 1
    ZANASI_COMM_FAIL = 2
    DATA_FORMAT_ERROR = 3


class ZanasiStatus(IntEnum):
    """Zanasi communication status"""
    DISCONNECTED = 0
    CONNECTED = 1
    SENDING = 2
    SUCCESS = 3
    FAILED = 4


class LogLevel(Enum):
    """Logging levels"""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class OperationResult(Enum):
    """Generic operation result states"""
    SUCCESS = "success"
    FAILURE = "failure"
    TIMEOUT = "timeout"
    RETRY = "retry"
    PARTIAL = "partial"


class ConnectionState(Enum):
    """Connection states for external systems"""
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    FAILED = "failed"
    TIMEOUT = "timeout"


class BatchOperation(Enum):
    """Types of batch operations"""
    DOWNLOAD = "download"
    LOAD = "load"
    UPDATE = "update"
    DELETE = "delete"
    VALIDATE = "validate"


class SystemComponent(Enum):
    """System components for logging and monitoring"""
    MODBUS_CLIENT = "modbus_client"
    FIREBASE_CLIENT = "firebase_client"
    ZANASI_CLIENT = "zanasi_client"
    BATCH_MANAGER = "batch_manager"
    STATUS_MANAGER = "status_manager"
    DATA_PARSER = "data_parser"
    MAIN_PROCESSOR = "main_processor"


# Utility functions for enum validation and conversion
def validate_enum_value(enum_class, value, default=None):
    """
    Validate that a value is a valid enum member
    
    Args:
        enum_class: The enum class to validate against
        value: The value to validate
        default: Default value if validation fails
        
    Returns:
        Valid enum member or default value
    """
    try:
        if isinstance(value, enum_class):
            return value
        return enum_class(value)
    except (ValueError, TypeError):
        if default is not None:
            return default
        raise ValueError(f"Invalid {enum_class.__name__} value: {value}")


def get_enum_choices(enum_class):
    """Get list of valid choices for an enum class"""
    return [member.value for member in enum_class]


def enum_to_dict(enum_class):
    """Convert enum class to dictionary for serialization"""
    return {member.name: member.value for member in enum_class}


# Constants for register mapping
class RegisterConstants:
    """Constants for PLC register mapping"""
    
    # Control and Status Registers (1-9)
    TRIGGER = 1
    RASP_PI_STATUS = 2
    PLC_STATUS = 3
    ZANASI_STATUS = 4
    ERROR_CODE = 5
    RESERVED_6 = 6
    SELECTED_BATCH = 7
    RESERVED_8 = 8
    RESERVED_9 = 9
    
    # Batch data configuration
    BATCH_START_REGISTER = 10
    REGISTERS_PER_BATCH = 20
    NUM_BATCHES = 5
    TOTAL_REGISTERS = 120
    
    # String encoding limits
    MAX_BATCH_CODE_LENGTH = 5
    MAX_DRYER_CODE_LENGTH = 5
    MAX_DATE_LENGTH = 10
    
    # Register allocations within each batch
    BATCH_INDEX_OFFSET = 0      # 1 register
    BATCH_STATUS_OFFSET = 1     # 1 register
    BATCH_COUNT_OFFSET = 2      # 1 register
    BATCH_CODE_OFFSET = 3       # 3 registers (5 chars)
    DRYER_CODE_OFFSET = 6       # 3 registers (5 chars)
    PROD_DATE_OFFSET = 9        # 6 registers (10 chars)
    EXP_DATE_OFFSET = 15        # 5 registers (10 chars)


# Color mappings for HMI/Dashboard
class UIColors:
    """Color mappings for different batch states"""
    
    STATE_COLORS = {
        BatchStates.QUEUED: "orange",
        BatchStates.NEXT_IN_QUEUE: "orange",
        BatchStates.CURRENT_PRINTING: "blue",
        BatchStates.LAST_PRINTED: "green",
        BatchStates.PRINTED: "green"
    }
    
    STATUS_COLORS = {
        ProcessingStates.IDLE: "gray",
        ProcessingStates.DOWNLOADING: "yellow",
        ProcessingStates.PROCESSING_DATA: "orange",
        ProcessingStates.READY_TO_SEND: "blue",
        ProcessingStates.SENDING_TO_ZANASI: "purple",
        ProcessingStates.COMPLETE: "green",
        ProcessingStates.ERROR: "red"
    }
    
    @classmethod
    def get_batch_color(cls, state: BatchStates) -> str:
        """Get color for batch state"""
        return cls.STATE_COLORS.get(state, "gray")
    
    @classmethod
    def get_status_color(cls, status: ProcessingStates) -> str:
        """Get color for processing status"""
        return cls.STATUS_COLORS.get(status, "gray")


# Validation rules
class ValidationRules:
    """Validation rules for batch data"""
    
    # Integer field limits
    MIN_BATCH_INDEX = 1001
    MAX_BATCH_INDEX = 99999
    MIN_PRINT_COUNT = 0
    MAX_PRINT_COUNT = 65535
    
    # Status validation
    MODIFIABLE_STATES = {BatchStates.QUEUED, BatchStates.NEXT_IN_QUEUE}
    READONLY_STATES = {BatchStates.CURRENT_PRINTING, BatchStates.LAST_PRINTED, BatchStates.PRINTED}
    
    @classmethod
    def is_batch_modifiable(cls, state: BatchStates) -> bool:
        """Check if batch can be modified based on its state"""
        return state in cls.MODIFIABLE_STATES
    
    @classmethod
    def validate_batch_index(cls, index: int) -> bool:
        """Validate batch index range"""
        return cls.MIN_BATCH_INDEX <= index <= cls.MAX_BATCH_INDEX
    
    @classmethod
    def validate_print_count(cls, count: int) -> bool:
        """Validate print count range"""
        return cls.MIN_PRINT_COUNT <= count <= cls.MAX_PRINT_COUNT
