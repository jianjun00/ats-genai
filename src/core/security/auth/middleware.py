from fastapi import HTTPException, status, Depends
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional
from dataclasses import dataclass
import asyncpg
import logging
import gin

from core.platform.config_env.environment import Environment
from .models import AuthContext, APIKeyTier
from .api_key_manager import APIKeyManager

logger = logging.getLogger(__name__)
security = HTTPBearer(auto_error=False)

@gin.configurable
@dataclass
class AuthenticationConfig:
    """Configuration for authentication middleware"""
    api_key_length: int = 32
    free_tier_daily_limit: int = 24
    rate_limit_reset_seconds: int = 86400  # 24 hours
    premium_unlimited: bool = True
    track_usage: bool = True
    log_invalid_attempts: bool = True

    # Error messages
    api_key_required_message: str = "API key required. Include 'Authorization: Bearer <your-api-key>' header."
    invalid_format_message: str = "Invalid API key format. API key must be 32 characters."
    invalid_key_message: str = "Invalid or expired API key."
    rate_limit_exceeded_message: str = "Rate limit exceeded. Free tier allows 24 requests per day. Upgrade to premium for unlimited access."


class AuthenticationMiddleware:
    """FastAPI middleware for API key authentication and rate limiting"""

    def __init__(self, config: AuthenticationConfig = None):
        self.env = Environment()
        self.api_key_manager = APIKeyManager()
        self.config = config or AuthenticationConfig()

    async def authenticate_request(
        self,
        credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
    ) -> AuthContext:
        """
        Authenticate API request and return auth context

        Raises:
            HTTPException: 401 for invalid/missing auth, 403 for rate limits
        """
        # Check if credentials provided
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=self.config.api_key_required_message
            )

        # Validate API key format
        raw_key = credentials.credentials
        if not raw_key or len(raw_key) != self.config.api_key_length:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=self.config.invalid_format_message
            )

        # Validate API key
        api_key = await self.api_key_manager.validate_api_key(raw_key)
        if not api_key:
            if self.config.log_invalid_attempts:
                logger.warning(f"Invalid API key attempted: {raw_key[:8]}...")
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=self.config.invalid_key_message
            )

        # Get current usage
        daily_usage = await self.api_key_manager.get_daily_usage(api_key.id)

        # Check rate limits
        is_premium = api_key.tier == APIKeyTier.PREMIUM
        rate_limit = float('inf') if (is_premium and self.config.premium_unlimited) else self.config.free_tier_daily_limit
        rate_limit_remaining = max(0, rate_limit - daily_usage) if not (is_premium and self.config.premium_unlimited) else float('inf')

        # Block if over rate limit
        if not (is_premium and self.config.premium_unlimited) and daily_usage >= self.config.free_tier_daily_limit:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=self.config.rate_limit_exceeded_message,
                headers={
                    "X-RateLimit-Limit": str(self.config.free_tier_daily_limit),
                    "X-RateLimit-Remaining": "0",
                    "X-RateLimit-Reset": str(self.config.rate_limit_reset_seconds)
                }
            )

        # Create auth context
        return AuthContext(
            api_key_id=api_key.id,
            tier=api_key.tier,
            key_prefix=api_key.key_prefix,
            daily_usage=daily_usage,
            rate_limit_remaining=int(rate_limit_remaining) if rate_limit_remaining != float('inf') else -1,
            is_premium=is_premium
        )

    async def track_request(
        self,
        auth_context: AuthContext,
        endpoint: str,
        method: str = "GET",
        status_code: int = 200,
        response_time_ms: Optional[int] = None
    ):
        """Track API request for usage analytics and rate limiting"""
        if not self.config.track_usage:
            return

        try:
            db_url = self.env.get_database_url()
            conn = await asyncpg.connect(db_url)

            try:
                # Use the stored function for efficient upsert
                await conn.execute(
                    f"SELECT update_api_usage($1, $2, $3, $4, $5)",
                    auth_context.api_key_id,
                    endpoint,
                    method,
                    status_code,
                    response_time_ms
                )

            finally:
                await conn.close()

        except Exception as e:
            # Don't fail the request if usage tracking fails
            logger.error(f"Failed to track API usage: {e}")


# Global middleware instance
auth_middleware = AuthenticationMiddleware()


# Dependency functions for FastAPI
async def require_auth() -> AuthContext:
    """Dependency that requires authentication"""
    return await auth_middleware.authenticate_request()


async def optional_auth(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security)
) -> Optional[AuthContext]:
    """Dependency that allows optional authentication"""
    if not credentials:
        return None

    try:
        return await auth_middleware.authenticate_request(credentials)
    except HTTPException:
        return None


# Rate limit headers dependency
def add_rate_limit_headers(auth_context: AuthContext, config: AuthenticationConfig = None) -> dict:
    """Generate rate limit headers for response"""
    config = config or AuthenticationConfig()

    if auth_context.is_premium and config.premium_unlimited:
        return {
            "X-RateLimit-Limit": "unlimited",
            "X-RateLimit-Remaining": "unlimited"
        }
    else:
        return {
            "X-RateLimit-Limit": str(config.free_tier_daily_limit),
            "X-RateLimit-Remaining": str(auth_context.rate_limit_remaining),
            "X-RateLimit-Reset": str(config.rate_limit_reset_seconds)
        }