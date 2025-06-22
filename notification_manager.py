"""
Notification Manager for Congressional Trading Monitor.

This module provides high-level notification orchestration including
template-based message formatting, duplicate detection, queuing,
and integration with the existing system components.
"""

import asyncio
import aiohttp
import os
import time
from typing import Optional, Dict, Any
from urllib.parse import quote, urljoin
from dataclasses import dataclass

from dotenv import load_dotenv
load_dotenv()  # Load environment variables from .env file

@dataclass
class NotificationRequest:
    """Simple notification request."""
    title: str
    body: str
    subtitle: Optional[str] = None
    url: Optional[str] = None  # URL to jump to when notification is clicked


@dataclass
class NotificationResponse:
    """Simple notification response."""
    success: bool
    error: Optional[str] = None
    timestamp: Optional[str] = None


class NotificationManager:
    """
    Minimal manager for sending notifications via Bark API.
    
    Supports both GET and POST requests with basic retry logic.
    """
    
    def __init__(self, api_key: str|None = None, base_url: str|None = None, 
                 max_retries: int = 3, timeout: int = 30):
        if api_key is None:
            api_key = os.getenv("BARK_API_KEY", "")
        self.api_key = api_key
        
        if base_url is None:
            base_url = os.getenv("BARK_BASE_URL", "https://api.day.app")
        self.base_url = base_url.rstrip('/')
        
        # Load notification icon from environment
        self.notification_icon = os.getenv("NOTIFICATION_ICON")
        
        self.max_retries = max_retries
        self.timeout = timeout
        self._session: Optional[aiohttp.ClientSession] = None
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self._ensure_session()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()
    
    async def _ensure_session(self):
        """Ensure aiohttp session is available."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout)
    
    async def _close_session(self):
        """Close aiohttp session."""
        if self._session and not self._session.closed:
            await self._session.close()
    
    def _sanitize_content(self, content: str, max_length: int = 500) -> str:
        """Sanitize and truncate content."""
        if not content:
            return ""
        
        # Basic sanitization
        sanitized = content.replace('\r\n', '\n').replace('\r', '\n')
        
        # Truncate if too long
        if len(sanitized) > max_length:
            sanitized = sanitized[:max_length-3] + "..."
        
        return sanitized
    
    def _build_get_url(self, title: str, subtitle: Optional[str], body: str, url: Optional[str] = None) -> str:
        """Build URL for GET request."""
        if not self.api_key:
            raise ValueError("API key is required")
        
        # URL-encode components
        encoded_key = quote(self.api_key, safe='')
        encoded_title = quote(title, safe='')
        encoded_body = quote(body, safe='')
        
        if subtitle:
            encoded_subtitle = quote(subtitle, safe='')
            path = f"/{encoded_key}/{encoded_title}/{encoded_subtitle}/{encoded_body}"
        else:
            path = f"/{encoded_key}/{encoded_title}/{encoded_body}"
        
        full_url = urljoin(self.base_url, path)
        
        # Add parameters
        params = []
        if url:
            params.append(f"url={quote(url, safe='')}")
        if self.notification_icon:
            params.append(f"icon={quote(self.notification_icon, safe='')}")
        
        if params:
            full_url += "?" + "&".join(params)
        
        return full_url
    
    async def send_notification(self, request: NotificationRequest, 
                              use_post: bool = True) -> NotificationResponse:
        """
        Send a notification.
        
        Args:
            request: The notification to send
            use_post: Whether to use POST (True) or GET (False) method
        
        Returns:
            NotificationResponse with success status
        """
        await self._ensure_session()
        
        # Sanitize content
        title = self._sanitize_content(request.title)
        subtitle = self._sanitize_content(request.subtitle) if request.subtitle else None
        body = self._sanitize_content(request.body)
        url = request.url  # URLs don't need sanitization, just validation
        
        timestamp = time.strftime('%Y-%m-%dT%H:%M:%S')
        
        for attempt in range(self.max_retries + 1):
            try:
                if use_post:
                    response = await self._send_post(title, subtitle, body, url)
                else:
                    response = await self._send_get(title, subtitle, body, url)
                
                if response:
                    print(f"Notification sent successfully: {title}")
                    return NotificationResponse(success=True, timestamp=timestamp)
                
            except Exception as e:
                print(f"Notification attempt {attempt + 1} failed: {e}")
                if attempt < self.max_retries:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    return NotificationResponse(
                        success=False, 
                        error=str(e), 
                        timestamp=timestamp
                    )
        
        return NotificationResponse(
            success=False, 
            error="Failed after all retries", 
            timestamp=timestamp
        )
    
    async def _send_get(self, title: str, subtitle: Optional[str], body: str, url: Optional[str] = None) -> bool:
        """Send notification via GET request."""
        full_url = self._build_get_url(title, subtitle, body, url)
        
        async with self._session.get(full_url) as response:
            return response.status == 200
    
    async def _send_post(self, title: str, subtitle: Optional[str], body: str, url: Optional[str] = None) -> bool:
        """Send notification via POST request."""
        endpoint_url = urljoin(self.base_url, f"/{quote(self.api_key, safe='')}")
        
        data = {"title": title, "body": body}
        if subtitle:
            data["subtitle"] = subtitle
        if url:
            data["url"] = url
        if self.notification_icon:
            data["icon"] = self.notification_icon
        
        async with self._session.post(endpoint_url, json=data) as response:
            return response.status == 200
    
    async def test_connection(self) -> bool:
        """Test the connection with a simple notification."""
        test_request = NotificationRequest(
            title="Test",
            body="Connection test"
        )
        
        try:
            response = await self.send_notification(test_request)
            return response.success
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    async def cleanup(self):
        """Clean up resources."""
        await self._close_session()


# Usage example:
async def example_usage():
    """Example of how to use the notification manager."""
    client = NotificationManager()
    
    async with client:
        # Test connection
        if await client.test_connection():
            print("Connection successful!")
        
        # Send a notification with URL
        request = NotificationRequest(
            title="New Filing Alert",
            subtitle="Congressional Trading",
            body="New PTR filing detected. Click to view details.",
            url="https://disclosures-clerk.house.gov/public_disc/ptr-pdfs/2025/20030461.pdf"
        )
        
        response = await client.send_notification(request)
        
        if response.success:
            print("Notification sent successfully!")
        else:
            print(f"Failed to send notification: {response.error}")


if __name__ == "__main__":
    # Run example
    asyncio.run(example_usage())