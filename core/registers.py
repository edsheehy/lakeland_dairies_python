#!/usr/bin/env python3
"""
PLC Register mapping and utilities for Lakeland Dairies Batch Processing System
"""

from typing import Tuple, List, Dict, Any
from core.enums import RegisterConstants


class PLCRegisters:
    """PLC Register Map for 5 Batches (Array[1..120] of Word)"""
    
    # Control and Status Registers (1-9)
    TRIGGER = RegisterConstants.TRIGGER
    RASP_PI_STATUS = RegisterConstants.RASP_PI_STATUS
    PLC_STATUS = RegisterConstants.PLC_STATUS
    ZANASI_STATUS = RegisterConstants.ZANASI_STATUS
    ERROR_CODE = RegisterConstants.ERROR_CODE
    RESERVED_6 = RegisterConstants.RESERVED_6
    SELECTED_BATCH = RegisterConstants.SELECTED_BATCH
    RESERVED_8 = RegisterConstants.RESERVED_8
    RESERVED_9 = RegisterConstants.RESERVED_9
    
    # Batch configuration
    BATCH_START_REGISTER = RegisterConstants.BATCH_START_REGISTER
    REGISTERS_PER_BATCH = RegisterConstants.REGISTERS_PER_BATCH
    NUM_BATCHES = RegisterConstants.NUM_BATCHES
    TOTAL_REGISTERS = RegisterConstants.TOTAL_REGISTERS
    
    @staticmethod
    def get_batch_registers(batch_number: int) -> Tuple[int, int, int, int, int, int, int]:
        """
        Get register addresses for a specific batch (1-5)
        
        Args:
            batch_number: Batch number (1-5)
            
        Returns:
            Tuple of (index_reg, status_reg, count_reg, batch_code_start, 
                     dryer_code_start, prod_date_start, exp_date_start)
        """
        if not 1 <= batch_number <= PLCRegisters.NUM_BATCHES:
            raise ValueError(f"Batch number must be between 1 and {PLCRegisters.NUM_BATCHES}")
        
        base = PLCRegisters.BATCH_START_REGISTER + (batch_number - 1) * PLCRegisters.REGISTERS_PER_BATCH
        
        return (
            base + RegisterConstants.BATCH_INDEX_OFFSET,    # batchIndex
            base + RegisterConstants.BATCH_STATUS_OFFSET,   # unified status
            base + RegisterConstants.BATCH_COUNT_OFFSET,    # printCount
            base + RegisterConstants.BATCH_CODE_OFFSET,     # batchCode start (3 registers)
            base + RegisterConstants.DRYER_CODE_OFFSET,     # dryerCode start (3 registers)
            base + RegisterConstants.PROD_DATE_OFFSET,      # productionDate start (6 registers)
            base + RegisterConstants.EXP_DATE_OFFSET        # expiryDate start (5 registers)
        )
    
    @staticmethod
    def get_control_registers() -> Dict[str, int]:
        """Get all control register addresses"""
        return {
            'trigger': PLCRegisters.TRIGGER,
            'rasp_pi_status': PLCRegisters.RASP_PI_STATUS,
            'plc_status': PLCRegisters.PLC_STATUS,
            'zanasi_status': PLCRegisters.ZANASI_STATUS,
            'error_code': PLCRegisters.ERROR_CODE,
            'selected_batch': PLCRegisters.SELECTED_BATCH
        }
    
    @staticmethod
    def validate_register_address(address: int) -> bool:
        """Validate that a register address is within valid range"""
        return 1 <= address <= PLCRegisters.TOTAL_REGISTERS
    
    @staticmethod
    def get_batch_register_range(batch_number: int) -> Tuple[int, int]:
        """
        Get the start and end register addresses for a batch
        
        Args:
            batch_number: Batch number (1-5)
            
        Returns:
            Tuple of (start_register, end_register) inclusive
        """
        if not 1 <= batch_number <= PLCRegisters.NUM_BATCHES:
            raise ValueError(f"Batch number must be between 1 and {PLCRegisters.NUM_BATCHES}")
        
        start = PLCRegisters.BATCH_START_REGISTER + (batch_number - 1) * PLCRegisters.REGISTERS_PER_BATCH
        end = start + PLCRegisters.REGISTERS_PER_BATCH - 1
        
        return (start, end)


class RegisterUtils:
    """Utilities for register data conversion and validation"""
    
    @staticmethod
    def string_to_registers(text: str, max_length: int) -> List[int]:
        """
        Convert string to list of register values (2 chars per register)
        
        Args:
            text: String to convert
            max_length: Maximum allowed string length
            
        Returns:
            List of register values
        """
        # Truncate if too long and ensure it's a string
        text = str(text)[:max_length]
        encoded_string = text.encode('utf-8')
        registers = []
        
        # Pack bytes into 16-bit registers (2 bytes per register)
        for i in range(0, len(encoded_string), 2):
            if i + 1 < len(encoded_string):
                # Two characters
                value = (encoded_string[i] << 8) + encoded_string[i + 1]
            else:
                # Last character + null
                value = (encoded_string[i] << 8)
            registers.append(value)
        
        # Add null terminator if needed
        if len(encoded_string) % 2 == 0:
            registers.append(0)
        
        return registers
    
    @staticmethod
    def registers_to_string(registers: List[int]) -> str:
        """
        Convert list of register values back to string
        
        Args:
            registers: List of register values
            
        Returns:
            Decoded string
        """
        try:
            text = ""
            for reg_val in registers:
                if reg_val == 0:  # Null terminator
                    break
                # Extract high and low bytes
                high_byte = (reg_val >> 8) & 0xFF
                low_byte = reg_val & 0xFF
                
                if high_byte != 0:
                    text += chr(high_byte)
                if low_byte != 0:
                    text += chr(low_byte)
                else:
                    break  # Null terminator in low byte
                    
            return text.strip()
        except Exception as e:
            raise ValueError(f"Error converting registers to string: {e}")
    
    @staticmethod
    def validate_integer(value: int, field_name: str = "value") -> int:
        """
        Validate integer fits in 16-bit unsigned range
        
        Args:
            value: Integer value to validate
            field_name: Name of field for error messages
            
        Returns:
            Validated integer value
        """
        if value < 0:
            raise ValueError(f"{field_name} cannot be negative: {value}")
        elif value > 65535:
            raise ValueError(f"{field_name} too large (max 65535): {value}")
        return value
    
    @staticmethod
    def calculate_register_count_for_string(max_chars: int) -> int:
        """
        Calculate number of registers needed for a string of given max length
        
        Args:
            max_chars: Maximum character count
            
        Returns:
            Number of registers needed
        """
        # 2 characters per register + null terminator
        return (max_chars + 1) // 2 + 1
    
    @staticmethod
    def get_batch_field_info() -> Dict[str, Dict[str, Any]]:
        """
        Get information about batch fields and their register requirements
        
        Returns:
            Dictionary with field information
        """
        return {
            'batchIndex': {
                'type': 'int',
                'registers': 1,
                'max_value': 65535,
                'description': 'Unique batch identifier'
            },
            'status': {
                'type': 'int',
                'registers': 1,
                'max_value': 4,
                'description': 'Batch processing status'
            },
            'printCount': {
                'type': 'int',
                'registers': 1,
                'max_value': 65535,
                'description': 'Number of items printed'
            },
            'batchCode': {
                'type': 'string',
                'registers': 3,
                'max_chars': RegisterConstants.MAX_BATCH_CODE_LENGTH,
                'description': 'Batch identification code'
            },
            'dryerCode': {
                'type': 'string',
                'registers': 3,
                'max_chars': RegisterConstants.MAX_DRYER_CODE_LENGTH,
                'description': 'Dryer identification code'
            },
            'productionDate': {
                'type': 'string',
                'registers': 6,
                'max_chars': RegisterConstants.MAX_DATE_LENGTH,
                'description': 'Production date'
            },
            'expiryDate': {
                'type': 'string',
                'registers': 5,
                'max_chars': RegisterConstants.MAX_DATE_LENGTH,
                'description': 'Expiry date'
            }
        }


class BatchRegisterBuilder:
    """Builder class for constructing batch register arrays"""
    
    def __init__(self):
        self.register_utils = RegisterUtils()
    
    def build_batch_registers(self, batch_data: Dict[str, Any]) -> List[int]:
        """
        Convert single batch data to list of 20 register values
        
        Args:
            batch_data: Dictionary containing batch information
            
        Returns:
            List of 20 register values for this batch
        """
        registers = [0] * PLCRegisters.REGISTERS_PER_BATCH
        
        # Integer fields (registers 0-2 of batch)
        registers[RegisterConstants.BATCH_INDEX_OFFSET] = self.register_utils.validate_integer(
            batch_data.get('batchIndex', 0), 'batchIndex'
        )
        registers[RegisterConstants.BATCH_STATUS_OFFSET] = self.register_utils.validate_integer(
            batch_data.get('status', 0), 'status'
        )
        registers[RegisterConstants.BATCH_COUNT_OFFSET] = self.register_utils.validate_integer(
            batch_data.get('printCount', 0), 'printCount'
        )
        
        # String fields
        batch_code_regs = self.register_utils.string_to_registers(
            batch_data.get('batchCode', ''), RegisterConstants.MAX_BATCH_CODE_LENGTH
        )
        dryer_code_regs = self.register_utils.string_to_registers(
            batch_data.get('dryerCode', ''), RegisterConstants.MAX_DRYER_CODE_LENGTH
        )
        prod_date_regs = self.register_utils.string_to_registers(
            batch_data.get('productionDate', ''), RegisterConstants.MAX_DATE_LENGTH
        )
        exp_date_regs = self.register_utils.string_to_registers(
            batch_data.get('expiryDate', ''), RegisterConstants.MAX_DATE_LENGTH
        )
        
        # Place strings in their designated positions (with padding)
        # batchCode: registers 3-5 (3 registers for 5 chars)
        for i, reg_val in enumerate(batch_code_regs[:3]):
            registers[RegisterConstants.BATCH_CODE_OFFSET + i] = reg_val
            
        # dryerCode: registers 6-8 (3 registers for 5 chars)
        for i, reg_val in enumerate(dryer_code_regs[:3]):
            registers[RegisterConstants.DRYER_CODE_OFFSET + i] = reg_val
            
        # productionDate: registers 9-14 (6 registers for 10 chars)
        for i, reg_val in enumerate(prod_date_regs[:6]):
            registers[RegisterConstants.PROD_DATE_OFFSET + i] = reg_val
            
        # expiryDate: registers 15-19 (5 registers for 10 chars)
        for i, reg_val in enumerate(exp_date_regs[:5]):
            registers[RegisterConstants.EXP_DATE_OFFSET + i] = reg_val
        
        return registers
    
    def build_complete_register_array(self, all_batch_data: List[Dict[str, Any]]) -> List[int]:
        """
        Build complete 120-register array for PLC transfer
        
        Args:
            all_batch_data: List of up to 5 batch dictionaries
            
        Returns:
            List of 120 register values
        """
        # Initialize all 120 registers to zero
        all_registers = [0] * PLCRegisters.TOTAL_REGISTERS
        
        # Fill control/status registers (registers 1-9, but array is 0-indexed so 0-8)
        # Leave these as 0 for now - they'll be set by separate status update methods
        
        # Fill batch data starting at register 10 (array index 9)
        for batch_idx, batch_data in enumerate(all_batch_data[:PLCRegisters.NUM_BATCHES]):
            batch_registers = self.build_batch_registers(batch_data)
            start_idx = (PLCRegisters.BATCH_START_REGISTER - 1) + (batch_idx * PLCRegisters.REGISTERS_PER_BATCH)
            
            for i, reg_val in enumerate(batch_registers):
                if start_idx + i < PLCRegisters.TOTAL_REGISTERS:
                    all_registers[start_idx + i] = reg_val
        
        return all_registers
    
    def extract_batch_from_registers(self, registers: List[int], batch_number: int) -> Dict[str, Any]:
        """
        Extract batch data from register array
        
        Args:
            registers: Complete register array or batch-specific registers
            batch_number: Batch number (1-5) if using complete array, or 0 for batch-only registers
            
        Returns:
            Dictionary containing batch data
        """
        if batch_number == 0:
            # Working with batch-specific registers only
            batch_registers = registers[:PLCRegisters.REGISTERS_PER_BATCH]
        else:
            # Extract from complete register array
            if not 1 <= batch_number <= PLCRegisters.NUM_BATCHES:
                raise ValueError(f"Batch number must be between 1 and {PLCRegisters.NUM_BATCHES}")
            
            start_idx = (PLCRegisters.BATCH_START_REGISTER - 1) + (batch_number - 1) * PLCRegisters.REGISTERS_PER_BATCH
            batch_registers = registers[start_idx:start_idx + PLCRegisters.REGISTERS_PER_BATCH]
        
        if len(batch_registers) < PLCRegisters.REGISTERS_PER_BATCH:
            raise ValueError(f"Insufficient registers for batch data: {len(batch_registers)}")
        
        # Extract integer fields
        batch_index = batch_registers[RegisterConstants.BATCH_INDEX_OFFSET]
        status = batch_registers[RegisterConstants.BATCH_STATUS_OFFSET]
        print_count = batch_registers[RegisterConstants.BATCH_COUNT_OFFSET]
        
        # Skip empty batches
        if batch_index == 0:
            return None
        
        # Extract strings
        batch_code = self.register_utils.registers_to_string(
            batch_registers[RegisterConstants.BATCH_CODE_OFFSET:RegisterConstants.BATCH_CODE_OFFSET + 3]
        )
        dryer_code = self.register_utils.registers_to_string(
            batch_registers[RegisterConstants.DRYER_CODE_OFFSET:RegisterConstants.DRYER_CODE_OFFSET + 3]
        )
        production_date = self.register_utils.registers_to_string(
            batch_registers[RegisterConstants.PROD_DATE_OFFSET:RegisterConstants.PROD_DATE_OFFSET + 6]
        )
        expiry_date = self.register_utils.registers_to_string(
            batch_registers[RegisterConstants.EXP_DATE_OFFSET:RegisterConstants.EXP_DATE_OFFSET + 5]
        )
        
        return {
            'batchIndex': batch_index,
            'status': status,
            'printCount': print_count,
            'batchCode': batch_code,
            'dryerCode': dryer_code,
            'productionDate': production_date,
            'expiryDate': expiry_date
        }


class RegisterValidator:
    """Validator for register data and batch information"""
    
    def __init__(self):
        self.register_utils = RegisterUtils()
    
    def validate_batch_data(self, batch_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate batch data before converting to registers
        
        Args:
            batch_data: Dictionary containing batch information
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Validate required fields
        required_fields = ['batchIndex', 'status', 'printCount', 'batchCode', 'dryerCode', 'productionDate', 'expiryDate']
        for field in required_fields:
            if field not in batch_data:
                errors.append(f"Missing required field: {field}")
        
        if errors:  # Don't continue if required fields are missing
            return False, errors
        
        # Validate integer ranges
        try:
            batch_index = int(batch_data['batchIndex'])
            if not (1001 <= batch_index <= 99999):
                errors.append(f"batchIndex must be between 1001 and 99999: {batch_index}")
        except (ValueError, TypeError):
            errors.append(f"batchIndex must be an integer: {batch_data['batchIndex']}")
        
        try:
            status = int(batch_data['status'])
            if not (0 <= status <= 4):
                errors.append(f"status must be between 0 and 4: {status}")
        except (ValueError, TypeError):
            errors.append(f"status must be an integer: {batch_data['status']}")
        
        try:
            print_count = int(batch_data['printCount'])
            if not (0 <= print_count <= 65535):
                errors.append(f"printCount must be between 0 and 65535: {print_count}")
        except (ValueError, TypeError):
            errors.append(f"printCount must be an integer: {batch_data['printCount']}")
        
        # Validate string lengths
        string_fields = {
            'batchCode': RegisterConstants.MAX_BATCH_CODE_LENGTH,
            'dryerCode': RegisterConstants.MAX_DRYER_CODE_LENGTH,
            'productionDate': RegisterConstants.MAX_DATE_LENGTH,
            'expiryDate': RegisterConstants.MAX_DATE_LENGTH
        }
        
        for field, max_length in string_fields.items():
            value = str(batch_data.get(field, ''))
            if len(value) > max_length:
                errors.append(f"{field} too long (max {max_length}): '{value}'")
        
        return len(errors) == 0, errors
    
    def validate_register_array(self, registers: List[int]) -> Tuple[bool, List[str]]:
        """
        Validate complete register array
        
        Args:
            registers: List of register values
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check array length
        if len(registers) != PLCRegisters.TOTAL_REGISTERS:
            errors.append(f"Register array must have {PLCRegisters.TOTAL_REGISTERS} elements, got {len(registers)}")
            return False, errors
        
        # Validate each register value
        for i, value in enumerate(registers):
            if not isinstance(value, int):
                errors.append(f"Register {i + 1} must be an integer: {type(value)}")
            elif not (0 <= value <= 65535):
                errors.append(f"Register {i + 1} value out of range (0-65535): {value}")
        
        # Validate batch data within the array
        builder = BatchRegisterBuilder()
        for batch_num in range(1, PLCRegisters.NUM_BATCHES + 1):
            try:
                batch_data = builder.extract_batch_from_registers(registers, batch_num)
                if batch_data:  # Skip empty batches
                    is_valid, batch_errors = self.validate_batch_data(batch_data)
                    if not is_valid:
                        for error in batch_errors:
                            errors.append(f"Batch {batch_num}: {error}")
            except Exception as e:
                errors.append(f"Error validating batch {batch_num}: {e}")
        
        return len(errors) == 0, errors