"""
Base Golden File Testing Framework - Generalized for Complex Business Objects

DESIGN PRINCIPLES:
1. **Extensible**: Support any business object type (OHLCV, UniverseState, etc.)
2. **Deterministic**: Consistent serialization across all object types
3. **Real Data**: Use actual production data structures for validation
4. **Proto-Compatible**: Structured serialization with versioning
5. **Comprehensive**: Handle nested objects, complex hierarchies, and business logic

USAGE PATTERN:
1. Extend BaseGoldenTestCase for your business object
2. Define proto schema for your object structure  
3. Implement object-specific serialization logic
4. Write tests using real data and business logic
5. Framework handles golden file management automatically
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, Any, Optional, List, TypeVar, Generic
import json
import hashlib
from datetime import datetime
import logging

# Generic type for business objects
BusinessObjectType = TypeVar('BusinessObjectType')

class BaseGoldenFileManager(ABC, Generic[BusinessObjectType]):
    """Abstract base for managing golden files for specific business object types."""
    
    def __init__(self, test_dir: Path, object_type_name: str):
        self.test_dir = test_dir
        self.object_type_name = object_type_name
        self.logger = logging.getLogger(f"{__name__}.{object_type_name}")
        
    @abstractmethod
    def serialize_object(self, obj: BusinessObjectType) -> Dict[str, Any]:
        """Convert business object to deterministic dictionary for golden file."""
        pass
        
    @abstractmethod
    def deserialize_object(self, data: Dict[str, Any]) -> BusinessObjectType:
        """Convert golden file dictionary back to business object."""
        pass
    
    @abstractmethod
    def get_object_schema_version(self) -> str:
        """Return schema version for backward compatibility."""
        pass
    
    def get_golden_file_path(self, test_method_name: str) -> Path:
        """Get path to golden file for specific test method."""
        golden_dir = self.test_dir / "golden_files" / self.object_type_name
        golden_dir.mkdir(parents=True, exist_ok=True)
        return golden_dir / f"{test_method_name}.json"
    
    def save_golden_file(self, test_method_name: str, obj: BusinessObjectType, 
                        test_metadata: Optional[Dict[str, Any]] = None):
        """Save business object as golden file with metadata."""
        golden_path = self.get_golden_file_path(test_method_name)
        
        # Create deterministic golden file structure
        golden_data = {
            "schema_version": self.get_object_schema_version(),
            "object_type": self.object_type_name,
            "test_method": test_method_name,
            "generated_at_utc_micros": int(datetime.utcnow().timestamp() * 1_000_000),
            "test_metadata": test_metadata or {},
            "business_object": self.serialize_object(obj)
        }
        
        # Write with deterministic formatting
        with open(golden_path, 'w') as f:
            json.dump(golden_data, f, indent=2, sort_keys=True, separators=(',', ': '))
            
        self.logger.info(f"✅ Saved golden file: {golden_path}")
        
    def load_golden_file(self, test_method_name: str) -> Optional[BusinessObjectType]:
        """Load business object from golden file."""
        golden_path = self.get_golden_file_path(test_method_name)
        
        if not golden_path.exists():
            return None
            
        with open(golden_path, 'r') as f:
            golden_data = json.load(f)
            
        # Validate schema version compatibility
        stored_version = golden_data.get("schema_version")
        current_version = self.get_object_schema_version()
        if stored_version != current_version:
            self.logger.warning(f"Schema version mismatch: stored={stored_version}, current={current_version}")
            
        return self.deserialize_object(golden_data["business_object"])
    
    def compare_objects(self, actual: BusinessObjectType, expected: BusinessObjectType) -> List[str]:
        """Compare two business objects and return list of differences."""
        actual_data = self.serialize_object(actual)
        expected_data = self.serialize_object(expected)
        
        return self._deep_compare_dicts(actual_data, expected_data, "")
    
    def _deep_compare_dicts(self, actual: Dict, expected: Dict, path: str) -> List[str]:
        """Deep comparison of nested dictionaries."""
        differences = []
        
        # Check for missing/extra keys
        actual_keys = set(actual.keys())
        expected_keys = set(expected.keys())
        
        for key in expected_keys - actual_keys:
            differences.append(f"{path}.{key}: Missing in actual")
            
        for key in actual_keys - expected_keys:
            differences.append(f"{path}.{key}: Extra in actual")
            
        # Compare common keys
        for key in actual_keys & expected_keys:
            actual_val = actual[key]
            expected_val = expected[key]
            current_path = f"{path}.{key}" if path else key
            
            if isinstance(actual_val, dict) and isinstance(expected_val, dict):
                differences.extend(self._deep_compare_dicts(actual_val, expected_val, current_path))
            elif isinstance(actual_val, list) and isinstance(expected_val, list):
                differences.extend(self._compare_lists(actual_val, expected_val, current_path))
            elif actual_val != expected_val:
                differences.append(f"{current_path}: {actual_val} != {expected_val}")
                
        return differences
    
    def _compare_lists(self, actual: List, expected: List, path: str) -> List[str]:
        """Compare lists with detailed difference reporting."""
        differences = []
        
        if len(actual) != len(expected):
            differences.append(f"{path}: Length mismatch - actual={len(actual)}, expected={len(expected)}")
            
        for i in range(min(len(actual), len(expected))):
            actual_item = actual[i]
            expected_item = expected[i]
            item_path = f"{path}[{i}]"
            
            if isinstance(actual_item, dict) and isinstance(expected_item, dict):
                differences.extend(self._deep_compare_dicts(actual_item, expected_item, item_path))
            elif actual_item != expected_item:
                differences.append(f"{item_path}: {actual_item} != {expected_item}")
                
        return differences


class BaseGoldenTestCase(ABC):
    """Base test case for golden file regression testing."""
    
    def __init__(self, test_dir: Path):
        self.test_dir = test_dir
        self.logger = logging.getLogger(self.__class__.__name__)
        
    @abstractmethod
    def get_golden_file_manager(self) -> BaseGoldenFileManager:
        """Return the golden file manager for this test type."""
        pass
        
    def run_golden_test(self, test_method_name: str, actual_object: BusinessObjectType, 
                       update_golden: bool = False, test_metadata: Optional[Dict[str, Any]] = None):
        """Run golden file test - core testing logic."""
        manager = self.get_golden_file_manager()
        
        if update_golden:
            # Update mode: save new golden file
            manager.save_golden_file(test_method_name, actual_object, test_metadata)
            self.logger.info(f"🔄 Updated golden file for {test_method_name}")
            return True
            
        else:
            # Validation mode: compare against existing golden file
            expected_object = manager.load_golden_file(test_method_name)
            
            if expected_object is None:
                # No golden file exists - create it
                manager.save_golden_file(test_method_name, actual_object, test_metadata)
                self.logger.info(f"📁 Created initial golden file for {test_method_name}")
                return True
                
            else:
                # Compare actual vs expected
                differences = manager.compare_objects(actual_object, expected_object)
                
                if differences:
                    self.logger.error(f"❌ Golden file test FAILED for {test_method_name}:")
                    for diff in differences[:10]:  # Show first 10 differences
                        self.logger.error(f"   - {diff}")
                    if len(differences) > 10:
                        self.logger.error(f"   ... and {len(differences) - 10} more differences")
                    return False
                else:
                    self.logger.info(f"✅ Golden file test PASSED for {test_method_name}")
                    return True


class TestDataHelper:
    """Helper for managing test data across different business object types."""
    
    @staticmethod
    def create_test_metadata(symbols: List[str], timeframe: str, start_time: datetime, 
                           end_time: datetime, **kwargs) -> Dict[str, Any]:
        """Create standardized test metadata."""
        return {
            "symbols": symbols,
            "timeframe": timeframe,
            "start_datetime": start_time.isoformat(),
            "end_datetime": end_time.isoformat(),
            "test_data_hash": TestDataHelper.generate_test_hash(symbols, timeframe, start_time, end_time),
            **kwargs
        }
    
    @staticmethod
    def generate_test_hash(symbols: List[str], timeframe: str, start_time: datetime, end_time: datetime) -> str:
        """Generate deterministic hash for test data identification."""
        data_str = f"{','.join(sorted(symbols))}_{timeframe}_{start_time.isoformat()}_{end_time.isoformat()}"
        return hashlib.md5(data_str.encode()).hexdigest()