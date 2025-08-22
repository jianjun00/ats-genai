import secrets
import string
import bcrypt
import asyncpg
from typing import Optional, List
from datetime import datetime, timedelta

from config.environment import Environment
from .models import APIKey, APIKeyTier, APIKeyStatus


class APIKeyManager:
    """Manages API key generation, validation, and lifecycle"""
    
    def __init__(self):
        self.env = Environment()
    
    def generate_api_key(self) -> str:
        """Generate a secure 32-character API key"""
        alphabet = string.ascii_letters + string.digits
        return ''.join(secrets.choice(alphabet) for _ in range(32))
    
    def hash_api_key(self, api_key: str) -> str:
        """Hash API key for secure storage"""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(api_key.encode('utf-8'), salt).decode('utf-8')
    
    def verify_api_key(self, api_key: str, key_hash: str) -> bool:
        """Verify API key against stored hash"""
        return bcrypt.checkpw(api_key.encode('utf-8'), key_hash.encode('utf-8'))
    
    async def create_api_key(
        self,
        tier: APIKeyTier,
        name: Optional[str] = None,
        description: Optional[str] = None,
        expires_days: Optional[int] = None,
        created_by: str = "system"
    ) -> tuple[str, APIKey]:
        """
        Create a new API key and store in database
        
        Returns:
            tuple: (raw_api_key, api_key_model)
        """
        # Generate the key
        raw_key = self.generate_api_key()
        key_hash = self.hash_api_key(raw_key)
        key_prefix = raw_key[:8]
        
        expires_at = None
        if expires_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_days)
        
        # Get database connection
        db_url = self.env.get_database_url()
        conn = await asyncpg.connect(db_url)
        
        try:
            # Insert into database
            table_name = self.env.get_table_name("api_keys")
            query = f"""
                INSERT INTO {table_name} 
                (key_hash, key_prefix, tier, name, description, expires_at, created_by)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id, created_at
            """
            
            result = await conn.fetchrow(
                query, key_hash, key_prefix, tier.value, name, description, expires_at, created_by
            )
            
            # Create APIKey model
            api_key = APIKey(
                id=result['id'],
                key_hash=key_hash,
                key_prefix=key_prefix,
                tier=tier,
                status=APIKeyStatus.ACTIVE,
                name=name,
                description=description,
                created_at=result['created_at'],
                expires_at=expires_at,
                created_by=created_by
            )
            
            return raw_key, api_key
            
        finally:
            await conn.close()
    
    async def validate_api_key(self, raw_key: str) -> Optional[APIKey]:
        """
        Validate API key and return APIKey model if valid
        
        Args:
            raw_key: The raw API key from request
            
        Returns:
            APIKey model if valid, None otherwise
        """
        if not raw_key or len(raw_key) != 32:
            return None
        
        key_prefix = raw_key[:8]
        
        # Get database connection
        db_url = self.env.get_database_url()
        conn = await asyncpg.connect(db_url)
        
        try:
            # Find potential keys by prefix (for performance)
            table_name = self.env.get_table_name("api_keys")
            query = f"""
                SELECT id, key_hash, key_prefix, tier, status, name, description,
                       created_at, last_used_at, expires_at, created_by
                FROM {table_name}
                WHERE key_prefix = $1 AND status = 'active'
            """
            
            candidates = await conn.fetch(query, key_prefix)
            
            # Verify hash for each candidate
            for record in candidates:
                if self.verify_api_key(raw_key, record['key_hash']):
                    # Check if expired
                    if record['expires_at'] and record['expires_at'] < datetime.utcnow():
                        continue
                    
                    return APIKey(
                        id=record['id'],
                        key_hash=record['key_hash'],
                        key_prefix=record['key_prefix'],
                        tier=APIKeyTier(record['tier']),
                        status=APIKeyStatus(record['status']),
                        name=record['name'],
                        description=record['description'],
                        created_at=record['created_at'],
                        last_used_at=record['last_used_at'],
                        expires_at=record['expires_at'],
                        created_by=record['created_by']
                    )
            
            return None
            
        finally:
            await conn.close()
    
    async def revoke_api_key(self, key_id: int) -> bool:
        """Revoke an API key by ID"""
        db_url = self.env.get_database_url()
        conn = await asyncpg.connect(db_url)
        
        try:
            table_name = self.env.get_table_name("api_keys")
            query = f"UPDATE {table_name} SET status = 'revoked' WHERE id = $1"
            result = await conn.execute(query, key_id)
            return result == "UPDATE 1"
            
        finally:
            await conn.close()
    
    async def list_api_keys(self, limit: int = 100) -> List[APIKey]:
        """List all API keys (admin function)"""
        db_url = self.env.get_database_url()
        conn = await asyncpg.connect(db_url)
        
        try:
            table_name = self.env.get_table_name("api_keys")
            query = f"""
                SELECT id, key_hash, key_prefix, tier, status, name, description,
                       created_at, last_used_at, expires_at, created_by
                FROM {table_name}
                ORDER BY created_at DESC
                LIMIT $1
            """
            
            records = await conn.fetch(query, limit)
            
            return [
                APIKey(
                    id=record['id'],
                    key_hash=record['key_hash'],
                    key_prefix=record['key_prefix'],
                    tier=APIKeyTier(record['tier']),
                    status=APIKeyStatus(record['status']),
                    name=record['name'],
                    description=record['description'],
                    created_at=record['created_at'],
                    last_used_at=record['last_used_at'],
                    expires_at=record['expires_at'],
                    created_by=record['created_by']
                ) for record in records
            ]
            
        finally:
            await conn.close()
    
    async def get_daily_usage(self, api_key_id: int) -> int:
        """Get daily usage for an API key"""
        db_url = self.env.get_database_url()
        conn = await asyncpg.connect(db_url)
        
        try:
            table_name = self.env.get_table_name("api_usage")
            query = f"""
                SELECT COALESCE(SUM(request_count), 0) as daily_usage
                FROM {table_name}
                WHERE api_key_id = $1 AND date = CURRENT_DATE
            """
            
            result = await conn.fetchval(query, api_key_id)
            return result or 0
            
        finally:
            await conn.close()