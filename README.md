import asyncio
import websockets
import json
import struct
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DepthDataMonitor:
    """Monitor for real-time 200-depth market data for a specific stock"""
    
    def __init__(self, security_id: str, stock_name: str):
        self.security_id = security_id
        self.stock_name = stock_name
        
        # Market data storage
        self.bids: List[Dict] = []  # 100 bid levels
        self.asks: List[Dict] = []  # 100 ask levels
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.ltp: Optional[float] = None
        self.last_update: datetime = datetime.now()
        
        # Statistics
        self.total_messages = 0
        self.message_rate = 0
        self.message_timestamps = []
        
        # Connection status
        self.connected = False
        
        # Display cache for in-place updates
        self.last_display_values = {
            'ltp': None,
            'best_bid': None,
            'best_ask': None,
            'bids': [None] * 10,
            'asks': [None] * 10,
            'status': None
        }
    
    def update_market_data(self, bids: List[Dict], asks: List[Dict]):
        """Update market depth data"""
        self.bids = bids
        self.asks = asks
        
        # Sort bids (highest to lowest) and asks (lowest to highest)
        if bids:
            sorted_bids = sorted(bids, key=lambda x: x['price'], reverse=True)
            self.best_bid = sorted_bids[0]['price']
        else:
            self.best_bid = None
            
        if asks:
            sorted_asks = sorted(asks, key=lambda x: x['price'])
            self.best_ask = sorted_asks[0]['price']
        else:
            self.best_ask = None
        
        # Update LTP
        if self.best_bid and self.best_ask:
            self.ltp = (self.best_bid + self.best_ask) / 2
        elif self.best_bid:
            self.ltp = self.best_bid
        elif self.best_ask:
            self.ltp = self.best_ask
        else:
            self.ltp = None
        
        # Track update rate
        self.last_update = datetime.now()
        self.total_messages += 1
        current_time = time.time()
        self.message_timestamps.append(current_time)
        
        # Keep only last 5 seconds of timestamps
        cutoff = current_time - 5
        self.message_timestamps = [t for t in self.message_timestamps if t > cutoff]
        self.message_rate = len(self.message_timestamps) / 5 if self.message_timestamps else 0
    
    def update_connection_status(self, status: bool):
        """Update connection status"""
        self.connected = status
    
    def get_changed_values(self) -> Dict:
        """Get only the values that have changed since last update"""
        current_values = {
            'ltp': self.ltp,
            'best_bid': self.best_bid,
            'best_ask': self.best_ask,
            'bids': self.bids[:10] if len(self.bids) >= 10 else self.bids + [None] * (10 - len(self.bids)),
            'asks': self.asks[:10] if len(self.asks) >= 10 else self.asks + [None] * (10 - len(self.asks)),
            'status': f"Updates: {self.total_messages:,} | Rate: {self.message_rate:.1f}/s"
        }
        
        # Find what changed
        changes = {}
        for key in current_values:
            if current_values[key] != self.last_display_values[key]:
                changes[key] = current_values[key]
        
        # Update cache
        self.last_display_values = current_values
        
        return changes
    
    def get_static_table_positions(self) -> Dict:
        """Get line positions for each value in the static table"""
        # Line positions (0-indexed)
        return {
            'header_line': 4,  # Stock info line
            'table_start': 6,  # Start of table data
            'status_line': 18  # Status line
        }


def parse_market_depth_message(message: bytes):
    """Parse the binary market depth message from DhanHQ (200 levels)"""
    if len(message) < 12:
        return None
    
    try:
        # Parse header: message_length, feed_code, exchange_seg, security_id, num_rows
        header = message[:12]
        msg_length, feed_code, exchange_seg, security_id, num_rows = struct.unpack('<HBBII', header)
        
        if len(message) < msg_length:
            return None
        
        # Parse depth data rows (16 bytes each: price, quantity, orders, flags)
        data = message[12:msg_length]
        row_size = 16
        total_expected_bytes = num_rows * row_size
        
        if len(data) < total_expected_bytes:
            return None
        
        # Initialize arrays for bids (first 100) and asks (next 100)
        bids = []
        asks = []
        
        for i in range(num_rows):
            start = i * row_size
            end = start + row_size
            row_data = data[start:end]
            
            # Parse each row: price (int64), quantity (int32), orders (int16), flags (int16)
            price_raw, quantity, orders, flags = struct.unpack('<qihh', row_data)
            price = price_raw / 100.0  # Convert to float
            
            level_data = {
                'level': (i % 100) + 1,  # Level 1-100 for bids and asks
                'price': price,
                'quantity': quantity,
                'orders': orders,
                'flags': flags
            }
            
            # First 100 rows are bids, next 100 are asks
            if i < 100:
                bids.append(level_data)
            else:
                asks.append(level_data)
        
        return {
            'security_id': str(security_id),
            'market_depth': {
                'bids': bids,
                'asks': asks
            }
        }
        
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return None


class WebSocketDepthClient:
    """WebSocket client for real-time depth data"""
    
    def __init__(self, token: str, client_id: str, monitor: DepthDataMonitor):
        self.token = token
        self.client_id = client_id
        self.monitor = monitor
        self.is_running = False
        self.ws_connection = None
        
    async def connect_and_listen(self):
        """Maintain WebSocket connection and listen for depth data"""
        uri = f"wss://full-depth-api.dhan.co/twohundreddepth?token={self.token}&clientId={self.client_id}&authType=2"
        
        while self.is_running:
            try:
                # Update connection status
                self.monitor.update_connection_status(False)
                
                # Connect to WebSocket
                self.ws_connection = await asyncio.wait_for(
                    websockets.connect(
                        uri,
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=5,
                        max_size=20*1024*1024,
                        compression=None
                    ),
                    timeout=10
                )
                
                # Update connection status
                self.monitor.update_connection_status(True)
                logger.info(f"✅ Connected to {self.monitor.stock_name}")
                
                # Send subscription request
                subscription = {
                    "RequestCode": 23,
                    "ExchangeSegment": "NSE_EQ",
                    "SecurityId": self.monitor.security_id
                }
                
                await self.ws_connection.send(json.dumps(subscription))
                logger.info(f"📡 Subscribed to {self.monitor.stock_name} depth data")
                
                # Heartbeat tracking
                last_ping = time.time()
                last_message = time.time()
                
                # Listen for messages
                while self.is_running:
                    try:
                        # Receive message with timeout
                        message = await asyncio.wait_for(
                            self.ws_connection.recv(),
                            timeout=1
                        )
                        
                        last_message = time.time()
                        
                        # Process binary message
                        if isinstance(message, bytes):
                            depth_data = parse_market_depth_message(message)
                            if depth_data:
                                # Update monitor with new depth data
                                self.monitor.update_market_data(
                                    depth_data['market_depth']['bids'],
                                    depth_data['market_depth']['asks']
                                )
                        
                        # Send ping every 15 seconds
                        if time.time() - last_ping > 15:
                            try:
                                await self.ws_connection.ping()
                                last_ping = time.time()
                            except:
                                break
                        
                        # Check for data timeout
                        if time.time() - last_message > 30:
                            logger.warning(f"No data from {self.monitor.stock_name} for 30s, reconnecting...")
                            break
                            
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"Connection closed for {self.monitor.stock_name}")
                        break
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        break
                
                # Close connection gracefully
                if self.ws_connection:
                    try:
                        await self.ws_connection.close()
                    except:
                        pass
                    
            except asyncio.TimeoutError:
                logger.warning(f"⏱️ Connection timeout for {self.monitor.stock_name}")
            except Exception as e:
                logger.error(f"❌ Connection error: {e}")
            
            # Update connection status
            self.monitor.update_connection_status(False)
            
            # Wait before reconnecting
            if self.is_running:
                logger.info(f"🔄 Reconnecting {self.monitor.stock_name} in 2 seconds...")
                await asyncio.sleep(2)
    
    async def start(self):
        """Start the WebSocket client"""
        self.is_running = True
        await self.connect_and_listen()
    
    async def stop(self):
        """Stop the WebSocket client"""
        self.is_running = False
        if self.ws_connection:
            try:
                await self.ws_connection.close()
            except:
                pass


class StaticTableDisplay:
    """Display with static table that only updates values - table printed once"""
    
    def __init__(self, monitor: DepthDataMonitor):
        self.monitor = monitor
        self.is_running = False
        self.refresh_rate = 0.3  # Update every 0.3 seconds
        self.table_height = 20  # Total lines in static display
        self.initialized = False
        
    def clear_screen(self):
        """Clear terminal screen - called only once at start"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_static_table(self):
        """Print the static table structure ONE TIME ONLY"""
        print("\n" + "="*80)
        print("📊 REAL-TIME MARKET DEPTH DATA")
        print("="*80)
        print("Press Ctrl+C to exit")
        print("="*80 + "\n")
        
        # Stock info line (will be updated with values)
        print(" " * 100)
        
        # Separator
        print("─" * 100)
        
        # Table header (static - printed once)
        print(f"{'Level':>6} | {'Bid Price':>10} | {'Bid Qty':>12} | {'Bid Orders':>10} || {'Ask Price':>10} | {'Ask Qty':>12} | {'Ask Orders':>10}")
        print("─" * 100)
        
        # Table rows (10 rows - printed once with placeholders)
        for i in range(10):
            print(f"{i+1:6} | {'-':>10} | {'-':>12} | {'-':>10} || {'-':>10} | {'-':>12} | {'-':>10}")
        
        # Separator
        print("─" * 100)
        
        # Status line (will be updated with values)
        print(" " * 100)
        
        # Save cursor position at the bottom
        print("\033[s", end="")
        sys.stdout.flush()
    
    def update_table_value(self, line_num: int, text: str):
        """Update a specific line in the existing table without reprinting"""
        # Move cursor up to the line we want to update
        print(f"\033[{line_num + 1}H", end="")
        # Clear that line
        print("\033[2K", end="")
        # Print new text
        print(text)
        # Move cursor back to saved position (bottom)
        print("\033[u", end="")
        sys.stdout.flush()
    
    def update_header_line(self):
        """Update only the stock info header line"""
        if self.monitor.ltp and self.monitor.best_bid and self.monitor.best_ask:
            spread = self.monitor.best_ask - self.monitor.best_bid
            spread_pct = (spread / self.monitor.ltp * 100) if self.monitor.ltp else 0
            header_text = f"📊 {self.monitor.stock_name} | LTP: ₹{self.monitor.ltp:.2f} | Bid: ₹{self.monitor.best_bid:.2f} | Ask: ₹{self.monitor.best_ask:.2f} | Spread: ₹{spread:.2f} ({spread_pct:.2f}%)"
            self.update_table_value(5, header_text)
        else:
            header_text = f"📊 {self.monitor.stock_name} | Waiting for data..."
            self.update_table_value(5, header_text)
    
    def update_table_rows(self):
        """Update only the 10 bid/ask rows - values change, table stays same"""
        for i in range(10):
            bid_data = self.monitor.bids[i] if i < len(self.monitor.bids) else None
            ask_data = self.monitor.asks[i] if i < len(self.monitor.asks) else None
            
            # Line number calculation (starting from line 8 for first data row)
            line_num = 8 + i
            
            if bid_data and ask_data:
                row_text = f"{i+1:6} | {bid_data['price']:10.2f} | {bid_data['quantity']:12,d} | {bid_data['orders']:10,d} || {ask_data['price']:10.2f} | {ask_data['quantity']:12,d} | {ask_data['orders']:10,d}"
            elif bid_data:
                row_text = f"{i+1:6} | {bid_data['price']:10.2f} | {bid_data['quantity']:12,d} | {bid_data['orders']:10,d} || {'-':>10} | {'-':>12} | {'-':>10}"
            elif ask_data:
                row_text = f"{i+1:6} | {'-':>10} | {'-':>12} | {'-':>10} || {ask_data['price']:10.2f} | {ask_data['quantity']:12,d} | {ask_data['orders']:10,d}"
            else:
                row_text = f"{i+1:6} | {'-':>10} | {'-':>12} | {'-':10} || {'-':>10} | {'-':>12} | {'-':>10}"
            
            self.update_table_value(line_num, row_text)
    
    def update_status_line(self):
        """Update only the status line"""
        age = (datetime.now() - self.monitor.last_update).total_seconds()
        if not self.monitor.connected:
            status = "🔄 Connecting..."
        else:
            status = f"✅ Connected | Updates: {self.monitor.total_messages:,} | Rate: {self.monitor.message_rate:.1f}/s | Last: {age:.3f}s ago"
        
        # Status line is at line 20
        self.update_table_value(20, status)
    
    async def run_display(self):
        """Main display loop - table printed ONCE, only values updated"""
        self.is_running = True
        
        # STEP 1: Clear screen and print table ONE TIME
        self.clear_screen()
        self.print_static_table()
        self.initialized = True
        
        try:
            # STEP 2: Continuous update loop - ONLY updates values
            while self.is_running:
                # Update only the values in the static table
                self.update_header_line()   # Updates line 5
                self.update_table_rows()    # Updates lines 8-17
                self.update_status_line()   # Updates line 20
                
                # Wait before next update
                await asyncio.sleep(self.refresh_rate)
                
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Display error: {e}")
        finally:
            self.is_running = False
    
    def stop(self):
        """Stop the display"""
        self.is_running = False


async def main():
    """Main function"""
    # Updated token
    TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY4OTgxNzQwLCJpYXQiOjE3Njg4OTUzNDAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.YSsmuldr5miDdk7DFLMyuJjIiyfIu4DzMoe2Gp8KmqRI6i6RJBK7giRMaJlOs7iRy_Sxr3BhUy5FIFTDOAvi1Q"
    CLIENT_ID = "1108703565"
    
    # Popular Indian Stocks
    available_stocks = {
        "11536": "RELIANCE",
        "1660": "TCS",
        "1333": "HDFCBANK",
        "4963": "INFY",
        "1594": "ICICIBANK",
        "3045": "HINDUNILVR",
        "1394": "ITC",
        "2475": "SBIN",
        "10604": "BHARTIARTL",
        "5258": "WIPRO",
    }
    
    print("\n" + "="*80)
    print("📈 STOCK SELECTION")
    print("="*80)
    
    for idx, (stock_id, stock_name) in enumerate(available_stocks.items(), 1):
        print(f"{idx:2}. {stock_name}")
    
    print("="*80)
    
    try:
        # User selection
        selection = input("\nSelect stock (1-10): ").strip()
        
        if selection.isdigit() and 1 <= int(selection) <= 10:
            stock_id = list(available_stocks.keys())[int(selection)-1]
            stock_name = available_stocks[stock_id]
        else:
            stock_id = "11536"
            stock_name = "RELIANCE"
            print(f"Using default: {stock_name}")
        
        print(f"\nStarting {stock_name} depth stream...")
        print("Table will be displayed once. Only values will update in-place.\n")
        
        # Create monitor, display, and WebSocket client
        monitor = DepthDataMonitor(stock_id, stock_name)
        display = StaticTableDisplay(monitor)
        ws_client = WebSocketDepthClient(TOKEN, CLIENT_ID, monitor)
        
        # Wait a moment before starting display
        await asyncio.sleep(1)
        
        # Run both tasks concurrently
        display_task = asyncio.create_task(display.run_display())
        ws_task = asyncio.create_task(ws_client.start())
        
        # Wait for both tasks
        await asyncio.gather(display_task, ws_task)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
    finally:
        # Cleanup
        print("\n\n✅ Application terminated")
        if 'monitor' in locals():
            print(f"Total updates received: {monitor.total_messages:,}")


if __name__ == "__main__":
    try:
        import websockets
    except ImportError:
        print("Installing required package: websockets")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets"])
        import websockets
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
