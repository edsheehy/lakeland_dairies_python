#!/usr/bin/env python3
"""
Data parsing and validation for Lakeland Dairies Batch Processing System
"""

import logging
from typing import List, Dict, Any, Tuple, Optional

from core.enums import BatchStates, ValidationRules
from core.exceptions import DataValidationException
from core.registers import BatchRegisterBuilder, RegisterValidator


class DataParser:
    """Parser and validator for batch data from various sources"""
    
    def __init__(self):
        self.logger = logging.getLogger(f"{__name__}.DataParser")
        self.register_builder = BatchRegisterBuilder()
        self.validator = RegisterValidator()
    
    def parse_firebase_data(self, firebase_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Parse and validate Firebase data into standard batch format
        
        Args:
            firebase_data: Raw data from Firebase
            
        Returns:
            List of validated batch dictionaries
            
        Raises:
            DataValidationException: On validation errors
        """
        if not isinstance(firebase_data, list):
            raise DataValidationException(
                "Firebase data must be a list",
                field="firebase_data",
                value=type(firebase_data).__name__
            )
        
        parsed_batches = []
        validation_errors = []
        
        for i, batch_entry in enumerate(firebase_data):
            try:
                parsed_batch = self._parse_single_batch_entry(batch_entry, f"firebase_batch_{i}")
                parsed_batches.append(parsed_batch)
                
            except DataValidationException as e:
                error_msg = f"Batch {i}: {e.message}"
                validation_errors.append(error_msg)
                self.logger.warning(error_msg)
                # Continue processing other batches
                
        if validation_errors and not parsed_batches:
            # All batches failed validation
            raise DataValidationException(
                "All Firebase batch entries failed validation",
                validation_errors=validation_errors
            )
        
        if validation_errors:
            self.logger.warning(f"Parsed {len(parsed_batches)} valid batches, {len(validation_errors)} failed validation")
        else:
            self.logger.info(f"Successfully parsed {len(parsed_batches)} batches from Firebase")
        
        return parsed_batches
    
    def _parse_single_batch_entry(self, batch_entry: Dict[str, Any], source_name: str) -> Dict[str, Any]:
        """
        Parse and validate a single batch entry
        
        Args:
            batch_entry: Raw batch dictionary
            source_name: Source identifier for error reporting
            
        Returns:
            Validated batch dictionary
        """
        if not isinstance(batch_entry, dict):
            raise DataValidationException(
                f"Batch entry must be a dictionary, got {type(batch_entry).__name__}",
                field="batch_entry",
                value=str(batch_entry)
            )
        
        # Extract and validate fields
        parsed_batch = {}
        
        # Integer fields with validation
        parsed_batch['batchIndex'] = self._parse_integer_field(
            batch_entry, 'batchIndex', 1001, 99999, source_name
        )
        
        parsed_batch['status'] = self._parse_integer_field(
            batch_entry, 'status', 0, 4, source_name, default=0
        )
        
        parsed_batch['printCount'] = self._parse_integer_field(
            batch_entry, 'printCount', 0, 65535, source_name, default=0
        )
        
        # String fields with length validation
        parsed_batch['batchCode'] = self._parse_string_field(
            batch_entry, 'batchCode', 5, source_name, default=''
        )
        
        parsed_batch['dryerCode'] = self._parse_string_field(
            batch_entry, 'dryerCode', 5, source_name, default=''
        )
        
        parsed_batch['productionDate'] = self._parse_string_field(
            batch_entry, 'productionDate', 10, source_name, default=''
        )
        
        parsed_batch['expiryDate'] = self._parse_string_field(
            batch_entry, 'expiryDate', 10, source_name, default=''
        )
        
        # Additional validation
        self._validate_batch_business_rules(parsed_batch, source_name)
        
        return parsed_batch
    
    def _parse_integer_field(self, data: Dict, field_name: str, min_val: int, max_val: int, 
                           source_name: str, default: Optional[int] = None) -> int:
        """Parse and validate integer field"""
        value = data.get(field_name, default)
        
        if value is None:
            raise DataValidationException(
                f"Missing required field: {field_name}",
                field=field_name
            )
        
        try:
            int_value = int(value)
        except (ValueError, TypeError):
            if default is not None:
                self.logger.warning(f"{source_name}: Invalid {field_name} '{value}', using default {default}")
                return default
            else:
                raise DataValidationException(
                    f"Field {field_name} must be an integer, got '{value}'",
                    field=field_name,
                    value=value
                )
        
        if not (min_val <= int_value <= max_val):
            if default is not None and min_val <= default <= max_val:
                self.logger.warning(f"{source_name}: {field_name} {int_value} out of range [{min_val}-{max_val}], using default {default}")
                return default
            else:
                raise DataValidationException(
                    f"Field {field_name} must be between {min_val} and {max_val}, got {int_value}",
                    field=field_name,
                    value=int_value
                )
        
        return int_value
    
    def _parse_string_field(self, data: Dict, field_name: str, max_length: int, 
                          source_name: str, default: str = '') -> str:
        """Parse and validate string field"""
        value = data.get(field_name, default)
        
        if value is None:
            value = default
        
        str_value = str(value).strip()
        
        if len(str_value) > max_length:
            self.logger.warning(f"{source_name}: {field_name} too long, truncating from {len(str_value)} to {max_length} chars")
            str_value = str_value[:max_length]
        
        return str_value
    
    def _validate_batch_business_rules(self, batch: Dict[str, Any], source_name: str):
        """Apply business rule validation to batch data"""
        # Validate batch index format
        batch_index = batch['batchIndex']
        if not ValidationRules.validate_batch_index(batch_index):
            self.logger.warning(f"{source_name}: Unusual batch index: {batch_index}")
        
        # Validate print count
        print_count = batch['printCount']
        if not ValidationRules.validate_print_count(print_count):
            raise DataValidationException(
                f"Invalid print count: {print_count}",
                field='printCount',
                value=print_count
            )
        
        # Validate required fields are not empty for active batches
        if batch['status'] in [BatchStates.CURRENT_PRINTING, BatchStates.LAST_PRINTED]:
            required_fields = ['batchCode', 'dryerCode', 'productionDate', 'expiryDate']
            for field in required_fields:
                if not batch[field].strip():
                    raise DataValidationException(
                        f"Field {field} cannot be empty for active batch",
                        field=field,
                        value=batch[field]
                    )
    
    def map_firebase_to_plc_positions(self, firebase_batches: List[Dict[str, Any]], 
                                     current_plc_batches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Intelligently map Firebase batches to PLC positions
        
        Args:
            firebase_batches: Validated batches from Firebase
            current_plc_batches: Current batches from PLC
            
        Returns:
            List of 5 batches mapped to PLC positions
        """
        self.logger.info(f"Mapping {len(firebase_batches)} Firebase batches to PLC positions")
        
        # Create lookup of current PLC data by batchIndex
        plc_lookup = {}
        for plc_batch in current_plc_batches:
            batch_index = plc_batch.get('batchIndex', 0)
            if batch_index > 0:  # Valid batch
                plc_lookup[batch_index] = plc_batch
        
        # Sort Firebase batches by batchIndex descending (newest first)
        firebase_sorted = sorted(firebase_batches, key=lambda x: x.get('batchIndex', 0), reverse=True)
        
        # Fill positions 1-5 with intelligently mapped data
        result_batches = []
        
        for position in range(5):  # 5 PLC positions
            if position < len(firebase_sorted):
                firebase_batch = firebase_sorted[position]
                batch_index = firebase_batch.get('batchIndex', 0)
                
                # Check if this batch exists in current PLC data
                if batch_index in plc_lookup:
                    # Existing batch - apply preservation logic
                    mapped_batch = self._merge_existing_batch(firebase_batch, plc_lookup[batch_index])
                    self.logger.info(f"Position {position + 1}: Existing batch {batch_index} - preserving status={mapped_batch['status']}, count={mapped_batch['printCount']}")
                else:
                    # New batch - use Firebase data
                    mapped_batch = firebase_batch.copy()
                    self.logger.info(f"Position {position + 1}: New batch {batch_index} - using Firebase status={mapped_batch['status']}, count={mapped_batch['printCount']}")
                
                result_batches.append(mapped_batch)
            else:
                # Empty position
                empty_batch = self._create_empty_batch()
                result_batches.append(empty_batch)
                self.logger.info(f"Position {position + 1}: Empty")
        
        self.logger.info(f"Mapping completed: {len([b for b in result_batches if b['batchIndex'] > 0])} active batches")
        return result_batches
    
    def _merge_existing_batch(self, firebase_batch: Dict[str, Any], plc_batch: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge Firebase data with existing PLC batch, preserving critical fields
        
        Args:
            firebase_batch: New data from Firebase
            plc_batch: Existing data from PLC
            
        Returns:
            Merged batch data
        """
        batch_status = BatchStates(plc_batch.get('status', 0))
        
        # Determine what to preserve based on batch state
        if ValidationRules.is_batch_modifiable(batch_status):
            # Modifiable batch - use Firebase data but preserve counts
            merged_batch = firebase_batch.copy()
            merged_batch['printCount'] = plc_batch.get('printCount', 0)  # Preserve print count
        else:
            # Read-only batch - preserve status and count, update string fields only
            merged_batch = firebase_batch.copy()
            merged_batch['status'] = plc_batch.get('status', 0)
            merged_batch['printCount'] = plc_batch.get('printCount', 0)
        
        return merged_batch
    
    def _create_empty_batch(self) -> Dict[str, Any]:
        """Create empty batch dictionary"""
        return {
            'batchIndex': 0,
            'status': 0,
            'printCount': 0,
            'batchCode': '',
            'dryerCode': '',
            'productionDate': '',
            'expiryDate': ''
        }
    
    def convert_batches_to_registers(self, batches: List[Dict[str, Any]]) -> List[int]:
        """
        Convert batch data to PLC register format
        
        Args:
            batches: List of up to 5 batch dictionaries
            
        Returns:
            List of 120 register values
            
        Raises:
            DataValidationException: On conversion errors
        """
        try:
            # Validate each batch before conversion
            for i, batch in enumerate(batches):
                is_valid, errors = self.validator.validate_batch_data(batch)
                if not is_valid:
                    raise DataValidationException(
                        f"Batch {i} validation failed",
                        validation_errors=errors
                    )
            
            # Convert to registers
            register_array = self.register_builder.build_complete_register_array(batches)
            
            # Final validation of register array
            is_valid, errors = self.validator.validate_register_array(register_array)
            if not is_valid:
                raise DataValidationException(
                    "Register array validation failed",
                    validation_errors=errors
                )
            
            self.logger.info(f"Successfully converted {len(batches)} batches to {len(register_array)} registers")
            return register_array
            
        except Exception as e:
            if isinstance(e, DataValidationException):
                raise
            else:
                raise DataValidationException(
                    f"Error converting batches to registers: {e}"
                ) from e
    
    def extract_batches_from_registers(self, register_array: List[int]) -> List[Dict[str, Any]]:
        """
        Extract batch data from PLC register array
        
        Args:
            register_array: Complete 120-register array from PLC
            
        Returns:
            List of batch dictionaries (empty batches are None)
        """
        try:
            batches = []
            
            for batch_num in range(1, 6):  # Batches 1-5
                try:
                    batch_data = self.register_builder.extract_batch_from_registers(register_array, batch_num)
                    batches.append(batch_data)  # Can be None for empty batches
                except Exception as e:
                    self.logger.error(f"Error extracting batch {batch_num}: {e}")
                    batches.append(None)  # Empty batch
            
            active_batches = [b for b in batches if b is not None]
            self.logger.info(f"Extracted {len(active_batches)} active batches from registers")
            
            return batches
            
        except Exception as e:
            raise DataValidationException(
                f"Error extracting batches from registers: {e}"
            ) from e
    
    def validate_zanasi_data(self, batch_data: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate batch data for Zanasi transmission
        
        Args:
            batch_data: Batch dictionary
            
        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        
        # Check required fields for Zanasi
        zanasi_fields = ['batchCode', 'dryerCode', 'productionDate', 'expiryDate']
        for field in zanasi_fields:
            if field not in batch_data:
                errors.append(f"Missing required Zanasi field: {field}")
            elif not isinstance(batch_data[field], str):
                errors.append(f"Zanasi field {field} must be a string")
            elif len(batch_data[field].strip()) == 0:
                errors.append(f"Zanasi field {field} cannot be empty")
        
        # Check for problematic characters
        for field in zanasi_fields:
            if field in batch_data:
                value = str(batch_data[field])
                if '"' in value:
                    errors.append(f"Field {field} contains quotes which may cause Zanasi protocol issues")
                if '\n' in value or '\r' in value:
                    errors.append(f"Field {field} contains line breaks which are not allowed")
        
        return len(errors) == 0, errors
    
    def sanitize_for_zanasi(self, batch_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sanitize batch data for Zanasi transmission
        
        Args:
            batch_data: Original batch data
            
        Returns:
            Sanitized batch data
        """
        sanitized = batch_data.copy()
        
        # Fields to sanitize for Zanasi
        string_fields = ['batchCode', 'dryerCode', 'productionDate', 'expiryDate']
        
        for field in string_fields:
            if field in sanitized:
                value = str(sanitized[field])
                
                # Remove problematic characters
                value = value.replace('"', "'")  # Replace quotes with apostrophes
                value = value.replace('\n', ' ').replace('\r', ' ')  # Replace line breaks with spaces
                value = value.replace('\t', ' ')  # Replace tabs with spaces
                
                # Trim whitespace
                value = value.strip()
                
                sanitized[field] = value
        
        return sanitized
    
    def get_batch_summary_for_logging(self, batch_data: Dict[str, Any]) -> str:
        """
        Generate a summary string for batch data logging
        
        Args:
            batch_data: Batch dictionary
            
        Returns:
            Summary string
        """
        if not batch_data or batch_data.get('batchIndex', 0) == 0:
            return "Empty batch"
        
        return (f"Batch {batch_data.get('batchIndex', 'unknown')}: "
                f"Code={batch_data.get('batchCode', '')}, "
                f"Status={batch_data.get('status', 0)}, "
                f"Count={batch_data.get('printCount', 0)}")
    
    def compare_batch_data(self, batch1: Dict[str, Any], batch2: Dict[str, Any]) -> Dict[str, Any]:
        """
        Compare two batch dictionaries and return differences
        
        Args:
            batch1: First batch (e.g., current)
            batch2: Second batch (e.g., new)
            
        Returns:
            Dictionary with comparison results
        """
        comparison = {
            'identical': True,
            'differences': {},
            'fields_changed': []
        }
        
        # Compare all fields
        all_fields = set(list(batch1.keys()) + list(batch2.keys()))
        
        for field in all_fields:
            val1 = batch1.get(field)
            val2 = batch2.get(field)
            
            if val1 != val2:
                comparison['identical'] = False
                comparison['differences'][field] = {'old': val1, 'new': val2}
                comparison['fields_changed'].append(field)
        
        return comparison
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get data processing statistics"""
        # This would be extended to track actual statistics
        return {
            'total_batches_processed': 0,  # Would track actual count
            'validation_errors': 0,       # Would track actual errors
            'conversion_errors': 0,       # Would track conversion issues
            'last_processing_time': None  # Would track timing
        }