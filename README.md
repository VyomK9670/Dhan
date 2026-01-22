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
import csv
import aiofiles

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DepthDataMonitor:
    """Monitor for real-time 200-depth market data for multiple stocks"""
    
    def __init__(self, security_id: str, stock_name: str):
        self.security_id = security_id
        self.stock_name = stock_name
        
        # Store ALL 100 levels for bids and asks
        self.bids: List[Dict] = []  # All 100 bid levels
        self.asks: List[Dict] = []  # All 100 ask levels
        
        # Statistics
        self.total_messages = 0
        self.last_update: datetime = datetime.now()
        
        # Connection status
        self.connected = False
        
        # CSV file path for this stock
        self.csv_file = f"{stock_name}_depth_data.csv"
        
        # Initialize CSV with headers
        self.initialize_csv()
    
    def format_price(self, price: float) -> str:
        """Format price with 4 decimal places"""
        return f"{price:.4f}" if price else "0.0000"
    
    def initialize_csv(self):
        """Initialize CSV file with headers"""
        headers = ['Timestamp', 'Stock']
        
        # Add headers for all 100 levels (bids and asks)
        for i in range(1, 101):
            headers.extend([
                f'Bid_{i}_Price(₹)',
                f'Bid_{i}_Quantity',
                f'Bid_{i}_Orders',
                f'Bid_{i}_Flags'
            ])
        
        for i in range(1, 101):
            headers.extend([
                f'Ask_{i}_Price(₹)',
                f'Ask_{i}_Quantity',
                f'Ask_{i}_Orders',
                f'Ask_{i}_Flags'
            ])
        
        # Write headers if file doesn't exist
        if not os.path.exists(self.csv_file):
            with open(self.csv_file, 'w', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(headers)
    
    async def save_to_csv(self):
        """Save current depth data to CSV with prices formatted to 4 decimal places"""
        try:
            row = [datetime.now().isoformat(), self.stock_name]
            
            # Add all bid data (100 levels) with formatted prices
            for i in range(100):
                if i < len(self.bids):
                    bid = self.bids[i]
                    row.extend([
                        self.format_price(bid.get('price', 0.0)),  # Price with 4 decimals
                        bid.get('quantity', 0),
                        bid.get('orders', 0),
                        bid.get('flags', 0)
                    ])
                else:
                    row.extend(["0.0000", 0, 0, 0])  # Formatted price with 4 decimals
            
            # Add all ask data (100 levels) with formatted prices
            for i in range(100):
                if i < len(self.asks):
                    ask = self.asks[i]
                    row.extend([
                        self.format_price(ask.get('price', 0.0)),  # Price with 4 decimals
                        ask.get('quantity', 0),
                        ask.get('orders', 0),
                        ask.get('flags', 0)
                    ])
                else:
                    row.extend(["0.0000", 0, 0, 0])  # Formatted price with 4 decimals
            
            # Write to CSV
            async with aiofiles.open(self.csv_file, 'a', newline='') as f:
                writer = csv.writer(f)
                await writer.writerow(row)
                
        except Exception as e:
            logger.error(f"CSV save error for {self.stock_name}: {e}")
    
    def update_market_data(self, bids: List[Dict], asks: List[Dict]):
        """Update market depth data with ALL 100 levels"""
        # Store all 100 levels
        self.bids = bids[:100]  # Ensure we only take up to 100 levels
        self.asks = asks[:100]  # Ensure we only take up to 100 levels
        
        # Track update rate
        self.last_update = datetime.now()
        self.total_messages += 1
    
    def update_connection_status(self, status: bool):
        """Update connection status"""
        self.connected = status
    
    def get_formatted_table_data(self) -> List[List[str]]:
        """Get formatted table data for display with ALL 100 levels"""
        table_data = []
        
        # Header
        table_data.append(["="*120])
        table_data.append([f"📊 {self.stock_name} - ALL 100 DEPTH LEVELS"])
        table_data.append(["="*120])
        table_data.append([" "])
        
        # Table headers for bids and asks side by side
        header_row = [
            "Level".rjust(6),
            "Bid Price(₹)".rjust(14),
            "Bid Qty".rjust(12),
            "Bid Orders".rjust(10),
            "Bid Flags".rjust(8),
            " | ",
            "Level".rjust(6),
            "Ask Price(₹)".rjust(14),
            "Ask Qty".rjust(12),
            "Ask Orders".rjust(10),
            "Ask Flags".rjust(8)
        ]
        table_data.append(["".join(header_row)])
        table_data.append(["-"*120])
        
        # Display ALL 100 levels
        for i in range(100):
            # Get bid data for this level
            if i < len(self.bids):
                bid = self.bids[i]
                bid_cols = [
                    str(bid.get('level', i+1)).rjust(6),
                    f"{bid.get('price', 0.0):14.4f}",  # Display with 4 decimals
                    f"{bid.get('quantity', 0):12,d}",
                    f"{bid.get('orders', 0):10,d}",
                    f"{bid.get('flags', 0):8d}"
                ]
            else:
                bid_cols = [
                    str(i+1).rjust(6),
                    "0.0000".rjust(14),  # Display with 4 decimals
                    "0".rjust(12),
                    "0".rjust(10),
                    "0".rjust(8)
                ]
            
            # Get ask data for this level
            if i < len(self.asks):
                ask = self.asks[i]
                ask_cols = [
                    str(ask.get('level', i+1)).rjust(6),
                    f"{ask.get('price', 0.0):14.4f}",  # Display with 4 decimals
                    f"{ask.get('quantity', 0):12,d}",
                    f"{ask.get('orders', 0):10,d}",
                    f"{ask.get('flags', 0):8d}"
                ]
            else:
                ask_cols = [
                    str(i+1).rjust(6),
                    "0.0000".rjust(14),  # Display with 4 decimals
                    "0".rjust(12),
                    "0".rjust(10),
                    "0".rjust(8)
                ]
            
            # Combine bid and ask columns
            row = bid_cols + [" | "] + ask_cols
            table_data.append(["".join(row)])
        
        # Separator
        table_data.append(["-"*120])
        
        # Summary row
        total_bid_qty = sum(bid.get('quantity', 0) for bid in self.bids)
        total_bid_orders = sum(bid.get('orders', 0) for bid in self.bids)
        total_ask_qty = sum(ask.get('quantity', 0) for ask in self.asks)
        total_ask_orders = sum(ask.get('orders', 0) for ask in self.asks)
        
        summary = [
            f"Total Messages: {self.total_messages:,}".ljust(30),
            f"Total Bid Qty: {total_bid_qty:,}".ljust(25),
            f"Total Bid Orders: {total_bid_orders:,}".ljust(25),
            f"Total Ask Qty: {total_ask_qty:,}".ljust(25),
            f"Total Ask Orders: {total_ask_orders:,}".ljust(25)
        ]
        table_data.append([" ".join(summary)])
        
        # Connection status
        status = "✅ CONNECTED" if self.connected else "❌ DISCONNECTED"
        last_update = f"Last Update: {self.last_update.strftime('%H:%M:%S.%f')[:-3]}"
        table_data.append([f"{status} | {last_update}".center(120)])
        
        return table_data


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
                                
                                # Save to CSV
                                await self.monitor.save_to_csv()
                        
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


class MultiStockDisplay:
    """Display for multiple stocks with ALL 100 levels"""
    
    def __init__(self, monitors: Dict[str, DepthDataMonitor]):
        self.monitors = monitors
        self.is_running = False
        self.refresh_rate = 1.0  # Update every 1 second
        self.initialized = False
        self.display_order = list(monitors.keys())
        self.current_stock_index = 0
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_stock_data(self, stock_name: str):
        """Print depth data for a specific stock"""
        monitor = self.monitors[stock_name]
        table_data = monitor.get_formatted_table_data()
        
        # Clear screen for fresh display
        self.clear_screen()
        
        # Print header
        print("\n" + "="*120)
        print(f"📊 MULTI-STOCK DEPTH DATA MONITOR - {stock_name}")
        print("="*120)
        print(f"Press: 'N' for next stock | 'P' for previous stock | 'Q' to quit")
        print("="*120 + "\n")
        
        # Print all table data
        for row in table_data:
            print(row[0])
    
    async def run_display(self):
        """Main display loop - cycles through all stocks"""
        self.is_running = True
        
        try:
            while self.is_running:
                # Get current stock
                current_stock = self.display_order[self.current_stock_index]
                
                # Display current stock data
                self.print_stock_data(current_stock)
                
                # Wait for user input or timeout
                try:
                    # Use asyncio.wait_for to implement a timeout
                    user_input = await asyncio.wait_for(
                        self.get_user_input(),
                        timeout=self.refresh_rate
                    )
                    
                    if user_input.upper() == 'N':
                        # Next stock
                        self.current_stock_index = (self.current_stock_index + 1) % len(self.display_order)
                    elif user_input.upper() == 'P':
                        # Previous stock
                        self.current_stock_index = (self.current_stock_index - 1) % len(self.display_order)
                    elif user_input.upper() == 'Q':
                        # Quit
                        self.is_running = False
                        break
                        
                except asyncio.TimeoutError:
                    # Timeout reached, continue to next iteration
                    continue
                    
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Display error: {e}")
        finally:
            self.is_running = False
    
    async def get_user_input(self):
        """Get user input asynchronously"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, input, "Command [N/P/Q]: ")
    
    def stop(self):
        """Stop the display"""
        self.is_running = False


async def main():
    """Main function"""
    # Updated token
    TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY5MTQzMTAwLCJpYXQiOjE3NjkwNTY3MDAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.wW5OrbhTFdgO3sePri6ygVflN0u_Am0lnM1-0AYarPXZ5dhEIfqqYZTPBLBfSIyjs3SMKm2WLswUsVR6anzj6A"
    CLIENT_ID = "1108703565"
    
    # All stocks (no user input needed)
    all_stocks = {
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
    
    print("\n" + "="*120)
    print("📈 MULTI-STOCK DEPTH DATA MONITOR")
    print("="*120)
    print(f"Starting monitoring for {len(all_stocks)} stocks:")
    for idx, (stock_id, stock_name) in enumerate(all_stocks.items(), 1):
        print(f"{idx:2}. {stock_name}")
    print("="*120)
    print("\nInitializing... CSV files will be created for each stock.")
    print("CSV Format: Prices saved as ₹ values with 4 decimal places (e.g., 1234.5678)")
    print("="*120 + "\n")
    
    # Create monitors and WebSocket clients for all stocks
    monitors = {}
    clients = []
    
    for stock_id, stock_name in all_stocks.items():
        monitor = DepthDataMonitor(stock_id, stock_name)
        monitors[stock_name] = monitor
        
        ws_client = WebSocketDepthClient(TOKEN, CLIENT_ID, monitor)
        clients.append(ws_client)
    
    # Create display
    display = MultiStockDisplay(monitors)
    
    try:
        # Wait a moment before starting
        await asyncio.sleep(2)
        
        # Start all WebSocket clients
        client_tasks = [asyncio.create_task(client.start()) for client in clients]
        
        # Wait a bit for data to start flowing
        await asyncio.sleep(3)
        
        # Start display
        display_task = asyncio.create_task(display.run_display())
        
        # Wait for display task to complete
        await display_task
        
        # Stop all clients
        for client in clients:
            await client.stop()
        
        # Cancel client tasks
        for task in client_tasks:
            task.cancel()
        
        # Wait for cancellation
        await asyncio.gather(*client_tasks, return_exceptions=True)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
    finally:
        # Cleanup
        print("\n\n✅ Application terminated")
        print("="*120)
        print("CSV Files Created (with 4 decimal place prices):")
        for stock_name in monitors:
            print(f"  • {stock_name}_depth_data.csv")
        print("="*120)
        
        # Show sample CSV format
        print("\nCSV Column Sample (first 5 columns shown):")
        print("Timestamp, Stock, Bid_1_Price(₹), Bid_1_Quantity, Bid_1_Orders, ...")
        print("Example: 2024-01-01T12:00:00, RELIANCE, 1234.5678, 1000, 5, ...")
        print("="*120)


if __name__ == "__main__":
    try:
        import websockets
        import aiofiles
    except ImportError:
        print("Installing required packages: websockets, aiofiles")
        import subprocess
        subprocess.check_call([sys.executable, "-m", "pip", "install", "websockets", "aiofiles"])
        import websockets
        import aiofiles
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
