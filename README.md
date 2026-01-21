import asyncio
import websockets
import json
import struct
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional, Tuple
import os
from dataclasses import dataclass
import threading
import random
import uuid
from enum import Enum
import sys

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ANSI color codes
class Colors:
    RESET = '\033[0m'
    BOLD = '\033[1m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    YELLOW = '\033[33m'
    BLUE = '\033[34m'
    CYAN = '\033[36m'
    BRIGHT_GREEN = '\033[92m'
    BRIGHT_RED = '\033[91m'
    BRIGHT_YELLOW = '\033[93m'
    
    @staticmethod
    def color(text: str, color_code: str) -> str:
        return f"{color_code}{text}{Colors.RESET}"

@dataclass
class ETFData:
    """Data for individual ETF"""
    security_id: str
    name: str
    bids: List[Dict] = None
    asks: List[Dict] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    current_price: Optional[float] = None
    last_update: datetime = None
    connection_status: str = "DISCONNECTED"
    
    def __post_init__(self):
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []
        if self.last_update is None:
            self.last_update = datetime.now()
    
    def update_price(self):
        """Update current price from market data"""
        if self.best_bid and self.best_ask:
            self.current_price = (self.best_bid + self.best_ask) / 2
        elif self.best_bid:
            self.current_price = self.best_bid
        elif self.best_ask:
            self.current_price = self.best_ask
        else:
            self.current_price = None

class ETFMonitor:
    """Monitor for ETFs with real-time depth data"""
    
    def __init__(self):
        self.etfs: Dict[str, ETFData] = {}
        
        # ETF configurations
        self.etf_names = {
            "12032": "GOLDBEES",
            "15051": "HDFCGOLD",
            "27305": "KOTAKGOLD",
            "34848": "AXISGOLD",
            "26569": "SILVERBEES",
            "28751": "HDFCSILVER",
            "28769": "UTISILVER",
            "12713": "MIRAEGOLD",
            "34866": "AXISSILVER",
            "15052": "HDFCMFGOLD"
        }
        
        self._lock = threading.RLock()
        self.selected_etfs = list(self.etf_names.keys())
        self.total_updates = 0
        self.start_time = datetime.now()
        
        # Initialize ETFs
        for etf_id in self.selected_etfs:
            self.etfs[etf_id] = ETFData(
                security_id=etf_id,
                name=self.etf_names[etf_id]
            )
    
    def update_etf(self, security_id: str, bids: List[Dict], asks: List[Dict]):
        """Update ETF with new depth data"""
        with self._lock:
            if security_id not in self.etfs:
                return
            
            etf = self.etfs[security_id]
            
            # Update market data
            etf.bids = bids
            etf.asks = asks
            etf.last_update = datetime.now()
            self.total_updates += 1
            
            # Get best bid/ask
            if bids:
                etf.best_bid = max(bid['price'] for bid in bids)
            if asks:
                etf.best_ask = min(ask['price'] for ask in asks)
            
            # Update current price
            etf.update_price()
    
    def update_status(self, security_id: str, status: str):
        """Update connection status"""
        with self._lock:
            if security_id in self.etfs:
                self.etfs[security_id].connection_status = status
    
    def get_etf_list(self) -> List[ETFData]:
        """Get list of all ETFs"""
        with self._lock:
            return [self.etfs[etf_id] for etf_id in self.selected_etfs]
    
    def get_stats(self) -> Dict:
        """Get monitoring statistics"""
        runtime = (datetime.now() - self.start_time).total_seconds()
        connected = sum(1 for etf in self.etfs.values() 
                       if etf.connection_status == "CONNECTED")
        
        return {
            "connected": connected,
            "total": len(self.selected_etfs),
            "updates": self.total_updates,
            "updates_per_sec": self.total_updates / runtime if runtime > 0 else 0,
            "runtime": runtime
        }

class CompactTableDisplay:
    """Single-table display that updates in-place"""
    
    def __init__(self, monitor: ETFMonitor):
        self.monitor = monitor
        self.is_running = False
        self.refresh_rate = 0.5
        
        # Table dimensions
        self.header_height = 2
        self.row_height = len(monitor.selected_etfs)
        self.footer_height = 2
        self.total_height = self.header_height + self.row_height + self.footer_height
        
        # Initialize display
        self.initialized = False
    
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def move_cursor(self, row: int, col: int = 0):
        """Move cursor to specific position"""
        print(f"\033[{row};{col}H", end="")
    
    def save_cursor(self):
        """Save cursor position"""
        print("\033[s", end="")
    
    def restore_cursor(self):
        """Restore cursor position"""
        print("\033[u", end="")
    
    def get_connection_icon(self, status: str) -> str:
        """Get icon for connection status"""
        icons = {
            "CONNECTED": "●",
            "CONNECTING": "○",
            "DISCONNECTED": "✗"
        }
        return icons.get(status, "?")
    
    def get_connection_color(self, status: str) -> str:
        """Get color for connection status"""
        colors = {
            "CONNECTED": Colors.GREEN,
            "CONNECTING": Colors.YELLOW,
            "DISCONNECTED": Colors.RED
        }
        return colors.get(status, Colors.RESET)
    
    def format_price(self, price: float) -> str:
        """Format price compactly"""
        if price is None:
            return "N/A"
        if price >= 100:
            return f"₹{price:,.0f}"
        return f"₹{price:.2f}"
    
    def get_etf_abbr(self, name: str) -> str:
        """Get abbreviation for ETF name"""
        abbr_map = {
            "GOLDBEES": "GLDB",
            "SILVERBEES": "SLVB",
            "HDFCGOLD": "HGLD",
            "KOTAKGOLD": "KGLD",
            "AXISGOLD": "AGLD",
            "HDFCSILVER": "HSLV",
            "UTISILVER": "USLV",
            "MIRAEGOLD": "MGLD",
            "AXISSILVER": "ASLV",
            "HDFCMFGOLD": "HMFG"
        }
        return abbr_map.get(name, name[:4])
    
    def print_table_header(self):
        """Print static table header once"""
        # Top border
        print("┌" + "─" * 78 + "┐")
        
        # Title row
        title = f"📈 ETF Depth Monitor - Real-time 200-Level Data"
        stats = self.monitor.get_stats()
        status_text = f"📡 {stats['connected']}/{stats['total']} ⚡ {stats['updates_per_sec']:.0f}/s"
        
        title_row = f"│ {Colors.color(title, Colors.BOLD)}"
        padding = 78 - len(title.replace('\033', '')) - len(status_text.replace('\033', '')) - 10
        title_row += " " * padding + f"{status_text} │"
        print(title_row)
        
        # Column headers
        headers = ["#", "ETF", "C", "Bid", "Ask", "Orders", "Qty", "LTP", "Status"]
        header_row = "│ " + " │ ".join(f"{h:^7}" for h in headers) + " │"
        print("├" + "─" * 78 + "┤")
        print(header_row)
        print("├" + "─" * 78 + "┤")
    
    def print_etf_row(self, index: int, etf: ETFData):
        """Print a single ETF row with updated data"""
        # ETF abbreviation
        abbr = self.get_etf_abbr(etf.name)
        
        # Connection status with color
        conn_icon = self.get_connection_icon(etf.connection_status)
        conn_color = self.get_connection_color(etf.connection_status)
        
        # Get depth data (top 3 levels)
        bid_info = self.get_bid_info(etf)
        ask_info = self.get_ask_info(etf)
        
        # Format row
        row_parts = [
            f"{index:2d}",
            f"{Colors.color(abbr, Colors.BOLD):5s}",
            f"{Colors.color(conn_icon, conn_color):^3s}",
            f"{bid_info['price']:>7s}",
            f"{ask_info['price']:>7s}",
            f"{bid_info['orders']:>7s}",
            f"{bid_info['quantity']:>7s}",
            f"{self.format_price(etf.current_price):>7s}",
            f"{self.get_update_status(etf):>7s}"
        ]
        
        row_text = "│ " + " │ ".join(row_parts) + " │"
        print(row_text)
    
    def get_bid_info(self, etf: ETFData) -> Dict:
        """Get top bid information"""
        if not etf.bids or len(etf.bids) == 0:
            return {"price": "N/A", "orders": "N/A", "quantity": "N/A"}
        
        # Get best bid (highest price)
        best_bid = max(etf.bids, key=lambda x: x['price'])
        return {
            "price": f"{best_bid['price']:.2f}",
            "orders": str(best_bid['orders']),
            "quantity": f"{best_bid['quantity']/1000:.0f}K"
        }
    
    def get_ask_info(self, etf: ETFData) -> Dict:
        """Get top ask information"""
        if not etf.asks or len(etf.asks) == 0:
            return {"price": "N/A", "orders": "N/A", "quantity": "N/A"}
        
        # Get best ask (lowest price)
        best_ask = min(etf.asks, key=lambda x: x['price'])
        return {
            "price": f"{best_ask['price']:.2f}",
            "orders": str(best_ask['orders']),
            "quantity": f"{best_ask['quantity']/1000:.0f}K"
        }
    
    def get_update_status(self, etf: ETFData) -> str:
        """Get status based on last update"""
        if etf.connection_status != "CONNECTED":
            return "OFF"
        
        age = (datetime.now() - etf.last_update).total_seconds()
        if age < 1:
            return "LIVE"
        elif age < 5:
            return f"{age:.0f}s"
        else:
            return f"{age:.0f}s!"
    
    def print_footer(self):
        """Print footer with time and controls"""
        current_time = datetime.now().strftime("%H:%M:%S")
        runtime = self.monitor.get_stats()["runtime"]
        
        print("├" + "─" * 78 + "┤")
        
        time_info = f"🕒 {current_time} | ⏱️ {runtime:.0f}s | 📊 {self.monitor.total_updates:,} updates"
        controls = f"{Colors.color('Ctrl+C', Colors.YELLOW)} to exit"
        
        footer = f"│ {time_info}"
        padding = 78 - len(time_info.replace('\033', '')) - len(controls.replace('\033', '')) - 2
        footer += " " * padding + f"{controls} │"
        print(footer)
        
        print("└" + "─" * 78 + "┘")
    
    async def run_display(self):
        """Run the single-table display"""
        self.is_running = True
        
        # Clear and setup display
        self.clear_screen()
        
        try:
            while self.is_running:
                # Move to top-left
                self.move_cursor(0, 0)
                
                # Print header once
                if not self.initialized:
                    self.print_table_header()
                    self.initialized = True
                
                # Print ETF rows
                etfs = self.monitor.get_etf_list()
                for i, etf in enumerate(etfs, 1):
                    # Move to correct row (header = 4 lines, + i)
                    self.move_cursor(4 + i, 0)
                    self.print_etf_row(i, etf)
                
                # Print footer
                self.move_cursor(4 + len(etfs) + 1, 0)
                self.print_footer()
                
                # Clear any leftover lines below
                print("\033[J", end="")
                
                # Flush output
                sys.stdout.flush()
                
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

class WebSocketManager:
    """Manages WebSocket connections for ETF data"""
    
    def __init__(self, token: str, client_id: str, monitor: ETFMonitor):
        self.token = token
        self.client_id = client_id
        self.monitor = monitor
        self.is_running = False
    
    async def connect_etf(self, security_id: str):
        """Maintain connection for a single ETF"""
        uri = f"wss://full-depth-api.dhan.co/twohundreddepth?token={self.token}&clientId={self.client_id}&authType=2"
        
        while self.is_running:
            try:
                # Update status
                self.monitor.update_status(security_id, "CONNECTING")
                
                # Connect
                websocket = await asyncio.wait_for(
                    websockets.connect(uri, ping_interval=20, ping_timeout=10),
                    timeout=5
                )
                
                # Send subscription
                subscription = {
                    "RequestCode": 23,
                    "ExchangeSegment": "NSE_EQ",
                    "SecurityId": security_id
                }
                await websocket.send(json.dumps(subscription))
                
                # Update status
                self.monitor.update_status(security_id, "CONNECTED")
                
                # Process messages
                last_message = time.time()
                while self.is_running:
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=1
                        )
                        
                        last_message = time.time()
                        
                        if isinstance(message, bytes):
                            depth_data = self.parse_depth_message(message)
                            if depth_data:
                                self.monitor.update_etf(
                                    security_id,
                                    depth_data['bids'],
                                    depth_data['asks']
                                )
                        
                        # Check for inactivity
                        if time.time() - last_message > 30:
                            break
                            
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        break
                
                # Close connection
                try:
                    await websocket.close()
                except:
                    pass
                    
            except Exception as e:
                logger.debug(f"Connection error {security_id}: {e}")
            
            # Update status and retry
            self.monitor.update_status(security_id, "DISCONNECTED")
            
            if self.is_running:
                await asyncio.sleep(2)
    
    def parse_depth_message(self, message: bytes):
        """Parse depth data message"""
        if len(message) < 12:
            return None
        
        try:
            header = message[:12]
            msg_length, feed_code, exchange_seg, security_id, num_rows = struct.unpack('<HBBII', header)
            
            if len(message) < msg_length:
                return None
            
            data = message[12:msg_length]
            row_size = 16
            total_rows = min(num_rows, 200)
            
            bids = []
            asks = []
            
            for i in range(total_rows):
                start = i * row_size
                end = start + row_size
                row_data = data[start:end]
                
                price_raw, quantity, orders, flags = struct.unpack('<qihh', row_data)
                price = price_raw / 100.0
                
                depth_item = {
                    'price': price,
                    'quantity': quantity,
                    'orders': orders,
                    'flags': flags
                }
                
                if i < 100:  # First 100 are bids
                    bids.append(depth_item)
                else:  # Next 100 are asks
                    asks.append(depth_item)
            
            return {'bids': bids, 'asks': asks}
            
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            return None
    
    async def start(self):
        """Start all connections"""
        self.is_running = True
        
        tasks = []
        for etf_id in self.monitor.selected_etfs:
            task = asyncio.create_task(self.connect_etf(etf_id))
            tasks.append(task)
            await asyncio.sleep(0.1)  # Stagger connections
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
    
    async def stop(self):
        """Stop all connections"""
        self.is_running = False

async def main():
    """Main function"""
    # Updated token
    TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY5MDUyNzI3LCJpYXQiOjE3Njg5NjYzMjcsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.YDC_iXbY2nx0SrfHnxdTB8NoWEL-Ff6eTMQ4-Evhfl4-H5VUrvJKPhVk7neyxkNcZhXQ0XyBcGqLBln11c8M9g'
    CLIENT_ID = '1108703565'
    
    print("Initializing ETF Depth Monitor...")
    print(f"Monitoring {len(ETFMonitor().selected_etfs)} ETFs with 200-level depth")
    print("Press Ctrl+C to exit\n")
    
    # Create monitor and display
    monitor = ETFMonitor()
    display = CompactTableDisplay(monitor)
    
    # Create WebSocket manager
    ws_manager = WebSocketManager(TOKEN, CLIENT_ID, monitor)
    
    try:
        # Run display and WebSocket connections
        display_task = asyncio.create_task(display.run_display())
        await asyncio.sleep(1)
        
        # Start connections
        print("Connecting to ETFs...")
        await ws_manager.start()
        
    except KeyboardInterrupt:
        print("\n\nStopping...")
    except Exception as e:
        print(f"\nError: {e}")
    finally:
        # Cleanup
        display.stop()
        await ws_manager.stop()
        
        # Clear screen and show final message
        os.system('cls' if os.name == 'nt' else 'clear')
        print("\nETF Depth Monitor stopped")
        stats = monitor.get_stats()
        print(f"\nFinal Stats:")
        print(f"  Runtime: {stats['runtime']:.0f}s")
        print(f"  Total Updates: {stats['updates']:,}")
        print(f"  Avg Rate: {stats['updates_per_sec']:.1f}/s")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nApplication stopped by user")
