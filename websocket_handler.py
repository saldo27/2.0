"""
WebSocket handler for real-time collaboration in the scheduler.
Enables live collaboration between multiple users with real-time updates.
"""

import asyncio
import json
import logging
import websockets
import threading
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Callable
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

logger = logging.getLogger(__name__)

class MessageType(Enum):
    """Types of WebSocket messages"""
    CONNECT = "connect"
    DISCONNECT = "disconnect"
    HEARTBEAT = "heartbeat"
    SCHEDULE_UPDATE = "schedule_update"
    WORKER_ASSIGNMENT = "worker_assignment"
    VALIDATION_RESULT = "validation_result"
    USER_ACTION = "user_action"
    ERROR = "error"
    SYNC_REQUEST = "sync_request"
    SYNC_RESPONSE = "sync_response"

@dataclass
class WSMessage:
    """WebSocket message structure"""
    message_type: MessageType
    user_id: str
    timestamp: datetime
    data: Dict[str, Any]
    message_id: str = None
    
    def __post_init__(self):
        if self.message_id is None:
            self.message_id = str(uuid.uuid4())
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        data = asdict(self)
        data['message_type'] = self.message_type.value
        data['timestamp'] = self.timestamp.isoformat()
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> 'WSMessage':
        """Create from JSON string"""
        data = json.loads(json_str)
        data['message_type'] = MessageType(data['message_type'])
        data['timestamp'] = datetime.fromisoformat(data['timestamp'])
        return cls(**data)

@dataclass
class ConnectedUser:
    """Information about a connected user"""
    user_id: str
    websocket: Any  # websockets.WebSocketServerProtocol
    connection_time: datetime
    last_heartbeat: datetime
    is_active: bool = True
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary (excluding websocket)"""
        return {
            'user_id': self.user_id,
            'connection_time': self.connection_time.isoformat(),
            'last_heartbeat': self.last_heartbeat.isoformat(),
            'is_active': self.is_active
        }

class WebSocketHandler:
    """
    Handles WebSocket connections for real-time collaboration.
    Manages multiple concurrent users and message broadcasting.
    """
    
    def __init__(self, host: str = "localhost", port: int = 8765):
        """
        Initialize WebSocket handler
        
        Args:
            host: Server host
            port: Server port
        """
        self.host = host
        self.port = port
        self.connected_users: Dict[str, ConnectedUser] = {}
        self.message_handlers: Dict[MessageType, Callable] = {}
        self.server = None
        self.is_running = False
        
        # Set up default message handlers
        self._setup_default_handlers()
        
        logger.info(f"WebSocketHandler initialized on {host}:{port}")
    
    def _setup_default_handlers(self):
        """Set up default message handlers"""
        self.message_handlers[MessageType.HEARTBEAT] = self._handle_heartbeat
        self.message_handlers[MessageType.CONNECT] = self._handle_connect
        self.message_handlers[MessageType.DISCONNECT] = self._handle_disconnect
        self.message_handlers[MessageType.SYNC_REQUEST] = self._handle_sync_request
    
    def register_handler(self, message_type: MessageType, handler: Callable):
        """
        Register a custom message handler
        
        Args:
            message_type: Type of message to handle
            handler: Handler function
        """
        self.message_handlers[message_type] = handler
        logger.info(f"Registered handler for {message_type.value}")
    
    async def start_server(self):
        """Start the WebSocket server"""
        try:
            self.server = await websockets.serve(
                self._handle_client, 
                self.host, 
                self.port
            )
            self.is_running = True
            
            # Start heartbeat checker
            asyncio.create_task(self._heartbeat_checker())
            
            logger.info(f"WebSocket server started on {self.host}:{self.port}")
            
        except Exception as e:
            logger.error(f"Failed to start WebSocket server: {e}")
            raise
    
    async def stop_server(self):
        """Stop the WebSocket server"""
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.is_running = False
            logger.info("WebSocket server stopped")
    
    def start_in_thread(self):
        """Start WebSocket server in a separate thread"""
        def run_server():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.start_server())
            loop.run_forever()
        
        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
        logger.info("WebSocket server started in background thread")
    
    async def _handle_client(self, websocket, path):
        """Handle a new client connection"""
        user_id = None
        try:
            # Wait for initial connection message
            initial_message = await websocket.recv()
            message = WSMessage.from_json(initial_message)
            
            if message.message_type != MessageType.CONNECT:
                await websocket.close(1002, "First message must be CONNECT")
                return
            
            user_id = message.user_id
            
            # Add to connected users
            self.connected_users[user_id] = ConnectedUser(
                user_id=user_id,
                websocket=websocket,
                connection_time=datetime.now(),
                last_heartbeat=datetime.now()
            )
            
            logger.info(f"User {user_id} connected")
            
            # Send connection confirmation
            await self._send_message(user_id, MessageType.CONNECT, {
                'status': 'connected',
                'server_time': datetime.now().isoformat()
            })
            
            # Notify other users
            await self._broadcast_user_status(user_id, 'connected')
            
            # Handle messages from this client
            async for raw_message in websocket:
                try:
                    message = WSMessage.from_json(raw_message)
                    message.user_id = user_id  # Ensure user_id matches connection
                    await self._process_message(message)
                except json.JSONDecodeError:
                    logger.error(f"Invalid JSON from user {user_id}")
                except Exception as e:
                    logger.error(f"Error processing message from user {user_id}: {e}")
                    
        except websockets.exceptions.ConnectionClosed:
            logger.info(f"User {user_id} disconnected")
        except Exception as e:
            logger.error(f"Error handling client {user_id}: {e}")
        finally:
            # Clean up connection
            if user_id and user_id in self.connected_users:
                del self.connected_users[user_id]
                await self._broadcast_user_status(user_id, 'disconnected')
    
    async def _process_message(self, message: WSMessage):
        """Process incoming message"""
        try:
            # Update last heartbeat
            if message.user_id in self.connected_users:
                self.connected_users[message.user_id].last_heartbeat = datetime.now()
            
            # Get handler for message type
            handler = self.message_handlers.get(message.message_type)
            if handler:
                await handler(message)
            else:
                logger.warning(f"No handler for message type: {message.message_type}")
                
        except Exception as e:
            logger.error(f"Error processing message: {e}")
            await self._send_error(message.user_id, f"Error processing message: {e}")
    
    async def _handle_heartbeat(self, message: WSMessage):
        """Handle heartbeat message"""
        # Heartbeat is already handled by updating last_heartbeat in _process_message
        pass
    
    async def _handle_connect(self, message: WSMessage):
        """Handle connection message"""
        # Already handled in _handle_client
        pass
    
    async def _handle_disconnect(self, message: WSMessage):
        """Handle disconnect message"""
        if message.user_id in self.connected_users:
            await self.connected_users[message.user_id].websocket.close()
    
    async def _handle_sync_request(self, message: WSMessage):
        """Handle synchronization request"""
        # This would be implemented by the scheduler to provide current state
        await self._send_message(message.user_id, MessageType.SYNC_RESPONSE, {
            'message': 'Sync not implemented',
            'timestamp': datetime.now().isoformat()
        })
    
    async def _send_message(self, user_id: str, message_type: MessageType, data: Dict[str, Any]):
        """
        Send message to specific user
        
        Args:
            user_id: Target user ID
            message_type: Type of message
            data: Message data
        """
        if user_id not in self.connected_users:
            logger.warning(f"User {user_id} not connected")
            return
        
        message = WSMessage(
            message_type=message_type,
            user_id="server",
            timestamp=datetime.now(),
            data=data
        )
        
        try:
            user = self.connected_users[user_id]
            await user.websocket.send(message.to_json())
        except Exception as e:
            logger.error(f"Failed to send message to user {user_id}: {e}")
            # Remove disconnected user
            if user_id in self.connected_users:
                del self.connected_users[user_id]
    
    async def _send_error(self, user_id: str, error_message: str):
        """Send error message to user"""
        await self._send_message(user_id, MessageType.ERROR, {
            'error': error_message,
            'timestamp': datetime.now().isoformat()
        })
    
    async def broadcast_message(
        self, 
        message_type: MessageType, 
        data: Dict[str, Any], 
        exclude_user: str = None
    ):
        """
        Broadcast message to all connected users
        
        Args:
            message_type: Type of message
            data: Message data
            exclude_user: User ID to exclude from broadcast
        """
        message = WSMessage(
            message_type=message_type,
            user_id="server",
            timestamp=datetime.now(),
            data=data
        )
        
        disconnected_users = []
        
        for user_id, user in self.connected_users.items():
            if exclude_user and user_id == exclude_user:
                continue
                
            try:
                await user.websocket.send(message.to_json())
            except Exception as e:
                logger.error(f"Failed to broadcast to user {user_id}: {e}")
                disconnected_users.append(user_id)
        
        # Clean up disconnected users
        for user_id in disconnected_users:
            del self.connected_users[user_id]
    
    async def _broadcast_user_status(self, user_id: str, status: str):
        """Broadcast user connection status"""
        await self.broadcast_message(
            MessageType.USER_ACTION,
            {
                'action': 'user_status_change',
                'user_id': user_id,
                'status': status,
                'connected_users': list(self.connected_users.keys())
            },
            exclude_user=user_id
        )
    
    async def _heartbeat_checker(self):
        """Check for inactive connections and clean them up"""
        while self.is_running:
            try:
                await asyncio.sleep(30)  # Check every 30 seconds
                
                now = datetime.now()
                inactive_users = []
                
                for user_id, user in self.connected_users.items():
                    # Consider inactive if no heartbeat for 2 minutes
                    if (now - user.last_heartbeat).seconds > 120:
                        inactive_users.append(user_id)
                
                # Remove inactive users
                for user_id in inactive_users:
                    logger.info(f"Removing inactive user: {user_id}")
                    user = self.connected_users.pop(user_id, None)
                    if user:
                        try:
                            await user.websocket.close()
                        except:
                            pass
                        await self._broadcast_user_status(user_id, 'disconnected')
                
            except Exception as e:
                logger.error(f"Error in heartbeat checker: {e}")
    
    def get_connected_users(self) -> List[Dict[str, Any]]:
        """Get list of connected users"""
        return [user.to_dict() for user in self.connected_users.values()]
    
    def is_user_connected(self, user_id: str) -> bool:
        """Check if user is connected"""
        return user_id in self.connected_users
    
    def get_connection_count(self) -> int:
        """Get number of connected users"""
        return len(self.connected_users)

# Singleton instance for global access
websocket_handler = None

def get_websocket_handler(host: str = "localhost", port: int = 8765) -> WebSocketHandler:
    """Get singleton WebSocket handler instance"""
    global websocket_handler
    if websocket_handler is None:
        websocket_handler = WebSocketHandler(host, port)
    return websocket_handler