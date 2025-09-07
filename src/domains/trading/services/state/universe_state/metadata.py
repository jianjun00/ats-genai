"""
Universe State Metadata Management.
"""
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional
import json
import hashlib
from datetime import datetime

@dataclass
class UniverseStateMetadata:
    """Metadata for universe state files."""
    timestamp: str
    record_count: int
    file_size_bytes: int
    checksum: str
    created_at: str
    columns: List[str]
    data_sources: List[str]
    universe_type: str = "default"
    version: str = "1.0"

class MetadataManager:
    """Handles metadata operations for universe state."""
    
    def __init__(self, base_path: str):
        self.base_path = Path(base_path)
        
    def create_metadata(self, df, timestamp: str, data_sources: List[str]) -> UniverseStateMetadata:
        """Create metadata for a universe state dataframe."""
        
        # Calculate checksum
        data_hash = hashlib.md5(df.to_string().encode()).hexdigest()
        
        return UniverseStateMetadata(
            timestamp=timestamp,
            record_count=len(df),
            file_size_bytes=df.memory_usage(deep=True).sum(),
            checksum=data_hash,
            created_at=datetime.now().isoformat(),
            columns=df.columns.tolist(),
            data_sources=data_sources
        )
        
    def save_metadata(self, metadata: UniverseStateMetadata, filepath: Path) -> None:
        """Save metadata to file."""
        metadata_path = filepath.with_suffix('.metadata.json')
        with open(metadata_path, 'w') as f:
            json.dump(asdict(metadata), f, indent=2)
            
    def load_metadata(self, filepath: Path) -> UniverseStateMetadata:
        """Load metadata from file."""
        metadata_path = filepath.with_suffix('.metadata.json')
        with open(metadata_path, 'r') as f:
            data = json.load(f)
        return UniverseStateMetadata(**data)
