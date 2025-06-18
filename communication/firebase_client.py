#!/usr/bin/env python3
"""
Firebase client for batch data retrieval in Lakeland Dairies Batch Processing System
"""

import json
import time
import logging
import urllib.request
import urllib.error
from typing import List, Dict, Any, Optional
from urllib.parse import urlparse

from config_settings import FirebaseConfig
from core.enums import SystemComponent, OperationResult
from core.exceptions import FirebaseException, TimeoutException, RetryExhaustedException


class FirebaseClient:
    """Client for Firebase Cloud Firestore batch data operations"""
    
    def __init__(self, config: FirebaseConfig):
        self.config = config
        self.logger = logging.getLogger(f"{__name__}.FirebaseClient")
        self.last_request_time = None
        self.request_count = 0
        self.last_error = None
        
        # Validate URL
        self._validate_url()
    
    def _validate_url(self):
        """Validate Firebase URL format"""
        try:
            parsed = urlparse(self.config.url)
            if not parsed.scheme or not parsed.netloc:
                raise FirebaseException(
                    f"Invalid Firebase URL format: {self.config.url}",
                    url=self.config.url
                )
        except Exception as e:
            raise FirebaseException(
                f"Error validating Firebase URL: {e}",
                url=self.config.url
            ) from e
    
    def fetch_batch_data(self) -> List[Dict[str, Any]]:
        """
        Fetch batch data from Firebase with retry logic
        
        Returns:
            List of batch dictionaries (up to 5 batches)
            
        Raises:
            FirebaseException: On communication or data errors
        """
        self.logger.info(f"Fetching batch data from Firebase: {self.config.url}")
        
        for attempt in range(self.config.retry_attempts):
            try:
                self.request_count += 1
                start_time = time.time()
                
                # Create request with timeout
                request = urllib.request.Request(
                    self.config.url,
                    headers={
                        'User-Agent': 'Lakeland-Batch-System/1.0',
                        'Accept': 'application/json',
                        'Content-Type': 'application/json'
                    }
                )
                
                # Make HTTP request
                with urllib.request.urlopen(request, timeout=self.config.timeout) as response:
                    if response.status != 200:
                        raise FirebaseException(
                            f"HTTP error {response.status}: {response.reason}",
                            url=self.config.url,
                            status_code=response.status
                        )
                    
                    # Read and decode response
                    data = response.read().decode('utf-8')
                    json_data = json.loads(data)
                
                request_time = time.time() - start_time
                self.last_request_time = request_time
                self.last_error = None
                
                self.logger.info(f"Successfully fetched data from Firebase in {request_time:.2f}s")
                
                # Validate and process response
                return self._process_response(json_data)
                
            except urllib.error.HTTPError as e:
                error_msg = f"HTTP error {e.code}: {e.reason}"
                self.last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                
                if e.code in [404, 401, 403]:  # Don't retry for these errors
                    raise FirebaseException(
                        error_msg,
                        url=self.config.url,
                        status_code=e.code
                    ) from e
                    
            except urllib.error.URLError as e:
                error_msg = f"URL error: {e.reason}"
                self.last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                
            except json.JSONDecodeError as e:
                error_msg = f"Invalid JSON response: {e}"
                self.last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
                
                # Don't retry JSON decode errors
                raise FirebaseException(
                    error_msg,
                    url=self.config.url
                ) from e
                
            except Exception as e:
                error_msg = f"Unexpected error: {e}"
                self.last_error = e
                self.logger.warning(f"Attempt {attempt + 1} failed: {error_msg}")
            
            # Wait before retry (except on last attempt)
            if attempt < self.config.retry_attempts - 1:
                wait_time = self.config.retry_delay * (attempt + 1)  # Exponential backoff
                self.logger.info(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
        
        # All attempts failed
        raise RetryExhaustedException(
            f"Failed to fetch batch data after {self.config.retry_attempts} attempts",
            max_attempts=self.config.retry_attempts,
            last_error=self.last_error
        )
    
    def _process_response(self, json_data: Any) -> List[Dict[str, Any]]:
        """
        Process and validate Firebase response data
        
        Args:
            json_data: Raw JSON response from Firebase
            
        Returns:
            List of validated batch dictionaries
        """
        try:
            # Ensure we have a list
            if not isinstance(json_data, list):
                if isinstance(json_data, dict):
                    # Single batch returned as dict
                    json_data = [json_data]
                else:
                    raise FirebaseException(
                        f"Expected list or dict, got {type(json_data).__name__}",
                        url=self.config.url
                    )
            
            if len(json_data) == 0:
                self.logger.warning("No batch data returned from Firebase")
                return []
            
            # Limit to maximum 5 batches
            batch_entries = json_data[:5]
            
            # Validate each batch entry
            validated_batches = []
            for i, batch in enumerate(batch_entries):
                try:
                    validated_batch = self._validate_batch_entry(batch, i)
                    validated_batches.append(validated_batch)
                except Exception as e:
                    self.logger.error(f"Error validating batch entry {i}: {e}")
                    # Continue with other batches instead of failing completely
                    continue
            
            self.logger.info(f"Successfully processed {len(validated_batches)} valid batches")
            return validated_batches
            
        except Exception as e:
            raise FirebaseException(
                f"Error processing Firebase response: {e}",
                url=self.config.url
            ) from e
    
    def _validate_batch_entry(self, batch: Dict[str, Any], index: int) -> Dict[str, Any]:
        """
        Validate and normalize a single batch entry
        
        Args:
            batch: Raw batch dictionary from Firebase
            index: Batch index for error reporting
            
        Returns:
            Validated and normalized batch dictionary
        """
        if not isinstance(batch, dict):
            raise FirebaseException(
                f"Batch entry {index} must be a dictionary, got {type(batch).__name__}",
                url=self.config.url
            )
        
        # Required fields with defaults
        required_fields = {
            'batchIndex': 0,
            'status': 0,
            'printCount': 0,
            'batchCode': '',
            'dryerCode': '',
            'productionDate': '',
            'expiryDate': ''
        }
        
        validated_batch = {}
        
        for field, default_value in required_fields.items():
            value = batch.get(field, default_value)
            
            # Type validation and conversion
            if field in ['batchIndex', 'status', 'printCount']:
                try:
                    validated_batch[field] = int(value)
                except (ValueError, TypeError):
                    self.logger.warning(f"Invalid {field} in batch {index}: {value}, using default {default_value}")
                    validated_batch[field] = default_value
            else:
                # String fields
                validated_batch[field] = str(value) if value is not None else default_value
        
        # Additional validation
        self._validate_batch_values(validated_batch, index)
        
        return validated_batch
    
    def _validate_batch_values(self, batch: Dict[str, Any], index: int):
        """
        Validate batch field values
        
        Args:
            batch: Batch dictionary to validate
            index: Batch index for error reporting
        """
        # Validate batchIndex range
        batch_index = batch['batchIndex']
        if not (1001 <= batch_index <= 99999):
            self.logger.warning(f"Batch {index} has invalid batchIndex: {batch_index}")
        
        # Validate status range
        status = batch['status']
        if not (0 <= status <= 4):
            self.logger.warning(f"Batch {index} has invalid status: {status}")
            batch['status'] = 0  # Reset to default
        
        # Validate printCount range
        print_count = batch['printCount']
        if not (0 <= print_count <= 65535):
            self.logger.warning(f"Batch {index} has invalid printCount: {print_count}")
            batch['printCount'] = max(0, min(65535, print_count))  # Clamp to valid range
        
        # Validate string lengths
        max_lengths = {
            'batchCode': 5,
            'dryerCode': 5,
            'productionDate': 10,
            'expiryDate': 10
        }
        
        for field, max_length in max_lengths.items():
            value = batch[field]
            if len(value) > max_length:
                self.logger.warning(f"Batch {index} {field} too long, truncating: '{value}'")
                batch[field] = value[:max_length]
    
    def test_connection(self) -> bool:
        """
        Test Firebase connection by making a simple request
        
        Returns:
            True if connection test successful
        """
        try:
            self.fetch_batch_data()
            return True
        except Exception as e:
            self.logger.warning(f"Firebase connection test failed: {e}")
            return False
    
    def get_connection_info(self) -> Dict[str, Any]:
        """Get current connection information"""
        return {
            'url': self.config.url,
            'timeout': self.config.timeout,
            'retry_attempts': self.config.retry_attempts,
            'retry_delay': self.config.retry_delay,
            'request_count': self.request_count,
            'last_request_time': self.last_request_time,
            'last_error': str(self.last_error) if self.last_error else None
        }
    
    def fetch_with_cache(self, cache_duration: float = 30.0) -> List[Dict[str, Any]]:
        """
        Fetch batch data with simple time-based caching
        
        Args:
            cache_duration: Cache duration in seconds
            
        Returns:
            List of batch dictionaries
        """
        current_time = time.time()
        
        # Check if we have cached data that's still valid
        if (hasattr(self, '_cached_data') and 
            hasattr(self, '_cache_time') and 
            current_time - self._cache_time < cache_duration):
            
            self.logger.debug("Returning cached batch data")
            return self._cached_data
        
        # Fetch fresh data
        fresh_data = self.fetch_batch_data()
        
        # Update cache
        self._cached_data = fresh_data
        self._cache_time = current_time
        
        return fresh_data
    
    def clear_cache(self):
        """Clear cached data"""
        if hasattr(self, '_cached_data'):
            delattr(self, '_cached_data')
        if hasattr(self, '_cache_time'):
            delattr(self, '_cache_time')
        self.logger.debug("Cache cleared")


class FirebaseClientFactory:
    """Factory for creating configured Firebase clients"""
    
    @staticmethod
    def create_client(config: FirebaseConfig) -> FirebaseClient:
        """Create a new Firebase client with the given configuration"""
        return FirebaseClient(config)
    
    @staticmethod
    def create_with_custom_url(base_config: FirebaseConfig, custom_url: str) -> FirebaseClient:
        """Create Firebase client with custom URL"""
        custom_config = FirebaseConfig(
            url=custom_url,
            timeout=base_config.timeout,
            retry_attempts=base_config.retry_attempts,
            retry_delay=base_config.retry_delay
        )
        return FirebaseClient(custom_config)
