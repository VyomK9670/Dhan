import asyncio
import websockets
import json
import struct
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional
import os
import threading
import sys
import random

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockData:
    """Data class for individual stock"""
    def __init__(self, security_id: str, name: str):
        self.security_id = security_id
        self.name = name
        self.bids: List[Dict] = []
        self.asks: List[Dict] = []
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.ltp: Optional[float] = None
        self.total_updates = 0
        self.last_update = datetime.now()
        self.connection_status = "DISCONNECTED"
        
    def update_data(self, bids: List[Dict], asks: List[Dict]):
        """Update stock data"""
        self.bids = bids[:20]  # Show top 20 bids
        self.asks = asks[:20]  # Show top 20 asks
        self.last_update = datetime.now()
        self.total_updates += 1
        
        # Get best bid/ask
        if self.bids:
            sorted_bids = sorted(self.bids, key=lambda x: x['price'], reverse=True)
            self.best_bid = sorted_bids[0]['price']
        else:
            self.best_bid = None
            
        if self.asks:
            sorted_asks = sorted(self.asks, key=lambda x: x['price'])
            self.best_ask = sorted_asks[0]['price']
        else:
            self.best_ask = None
            
        # Calculate LTP
        if self.best_bid and self.best_ask:
            self.ltp = (self.best_bid + self.best_ask) / 2
        elif self.best_bid:
            self.ltp = self.best_bid
        elif self.best_ask:
            self.ltp = self.best_ask
        else:
            self.ltp = None

class DepthDataMonitor:
    """Monitor for real-time depth data"""
    
    def __init__(self):
        self.stocks: Dict[str, StockData] = {}
        
        # Define stocks to monitor
        self.stock_list = {
            "12032": "GOLDBEES",
            "15051": "HDFCMFGETF", 
            "27305": "KOTAKGOLD",
            "34848": "AXISGOLD",
            "26569": "SILVERBEES",
            "28751": "HDFCMFSILVERETF",
            "28769": "UTISILVERETF",
            "12713": "MIRAEASSETGOLD",
            "34866": "AXISSILVER",
            "15052": "HDFCMFGOLDETF"
        }
        
        self._lock = threading.RLock()
        self.selected_stocks = list(self.stock_list.keys())
        self.total_messages = 0
        self.start_time = datetime.now()
        
        # Initialize all stocks
        for stock_id in self.selected_stocks:
            stock_name = self.stock_list.get(stock_id, stock_id)
            self.stocks[stock_id] = StockData(stock_id, stock_name)
    
    def update_stock(self, security_id: str, bids: List[Dict], asks: List[Dict]):
        """Update stock data"""
        with self._lock:
            if security_id in self.stocks:
                self.stocks[security_id].update_data(bids, asks)
                self.total_messages += 1
    
    def update_status(self, security_id: str, status: str):
        """Update connection status"""
        with self._lock:
            if security_id in self.stocks:
                self.stocks[security_id].connection_status = status
    
    def get_display_data(self) -> Dict:
        """Get formatted display data"""
        with self._lock:
            display_data = {}
            for stock_id, stock in self.stocks.items():
                if stock_id not in self.selected_stocks:
                    continue
                    
                display_data[stock_id] = {
                    'name': stock.name,
                    'connection': stock.connection_status,
                    'ltp': stock.ltp,
                    'best_bid': stock.best_bid,
                    'best_ask': stock.best_ask,
                    'bids': stock.bids[:5],  # Top 5 bids
                    'asks': stock.asks[:5],  # Top 5 asks
                    'total_orders': sum(bid['orders'] for bid in stock.bids[:5]) + sum(ask['orders'] for ask in stock.asks[:5]),
                    'total_quantity': sum(bid['quantity'] for bid in stock.bids[:5]) + sum(ask['quantity'] for ask in stock.asks[:5]),
                    'update_age': (datetime.now() - stock.last_update).total_seconds()
                }
            
            runtime = (datetime.now() - self.start_time).total_seconds()
            
            return {
                'stocks': display_data,
                'stats': {
                    'total_stocks': len(self.selected_stocks),
                    'connected_stocks': sum(1 for s in self.stocks.values() 
                                          if s.security_id in self.selected_stocks 
                                          and s.connection_status == "CONNECTED"),
                    'total_messages': self.total_messages,
                    'messages_per_second': self.total_messages / runtime if runtime > 0 else 0,
                    'runtime': runtime
                },
                'timestamp': datetime.now()
            }

class CleanDashboard:
    """Clean, minimal dashboard for depth data"""
    
    def __init__(self, monitor: DepthDataMonitor):
        self.monitor = monitor
        self.is_running = False
        self.refresh_rate = 1.0  # Update every second
        
        # ANSI colors
        self.COLORS = {
            'RESET': '\033[0m',
            'GREEN': '\033[32m',
            'RED': '\033[31m',
            'YELLOW': '\033[33m',
            'CYAN': '\033[36m',
            'BLUE': '\033[34m',
            'MAGENTA': '\033[35m',
            'WHITE': '\033[37m',
            'BOLD': '\033[1m',
            'DIM': '\033[2m'
        }
    
    def color(self, text: str, color: str = 'RESET') -> str:
        """Colorize text"""
        return f"{self.COLORS.get(color, '')}{text}{self.COLORS['RESET']}"
    
    def clear_screen(self):
        """Clear screen"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_header(self, display_data: Dict):
        """Print header"""
        timestamp = display_data['timestamp'].strftime("%H:%M:%S")
        stats = display_data['stats']
        
        print(f"{self.color('┌────────────────────────────────────────────────────────────┐', 'CYAN')}")
        print(f"{self.color('│', 'CYAN')} {self.color('📈 REAL-TIME DEPTH DATA MONITOR', 'BOLD')} "
              f"{self.color('│', 'CYAN')} {self.color('🕒', 'YELLOW')} {timestamp} {self.color('│', 'CYAN')}")
        print(f"{self.color('├────────────────────────────────────────────────────────────┤', 'CYAN')}")
        print(f"{self.color('│', 'CYAN')} {self.color('Connected:', 'DIM')} {stats['connected_stocks']}/{stats['total_stocks']} "
              f"{self.color('│', 'CYAN')} {self.color('Updates:', 'DIM')} {stats['messages_per_second']:.1f}/s "
              f"{self.color('│', 'CYAN')} {self.color('Runtime:', 'DIM')} {int(stats['runtime'])}s {self.color('│', 'CYAN')}")
        print(f"{self.color('└────────────────────────────────────────────────────────────┘', 'CYAN')}")
        print()
    
    def print_stock_summary(self, display_data: Dict):
        """Print stock summary table"""
        stocks = display_data['stocks']
        
        # Header
        print(f"{self.color('┌──────┬──────────────────┬──────────┬──────────┬──────────┬──────────┐', 'CYAN')}")
        print(f"{self.color('│', 'CYAN')} {self.color('#', 'DIM')} {self.color('│', 'CYAN')} "
              f"{self.color('Stock', 'DIM'):<16} {self.color('│', 'CYAN')} "
              f"{self.color('Status', 'DIM'):<8} {self.color('│', 'CYAN')} "
              f"{self.color('LTP', 'DIM'):<8} {self.color('│', 'CYAN')} "
              f"{self.color('Best Bid', 'DIM'):<8} {self.color('│', 'CYAN')} "
              f"{self.color('Best Ask', 'DIM'):<8} {self.color('│', 'CYAN')}")
        print(f"{self.color('├──────┼──────────────────┼──────────┼──────────┼──────────┼──────────┤', 'CYAN')}")
        
        # Stock rows
        for i, (stock_id, data) in enumerate(stocks.items(), 1):
            # Status indicator
            if data['connection'] == "CONNECTED":
                status_disp = f"{self.color('● CON', 'GREEN')}"
            elif data['connection'] == "CONNECTING":
                status_disp = f"{self.color('● CON', 'YELLOW')}"
            else:
                status_disp = f"{self.color('✗ DIS', 'RED')}"
            
            # Format prices
            ltp_str = f"₹{data['ltp']:,.2f}" if data['ltp'] else "N/A"
            bid_str = f"₹{data['best_bid']:,.2f}" if data['best_bid'] else "N/A"
            ask_str = f"₹{data['best_ask']:,.2f}" if data['best_ask'] else "N/A"
            
            # Abbreviate stock name
            stock_name = data['name']
            if len(stock_name) > 16:
                stock_name = stock_name[:13] + "..."
            
            print(f"{self.color('│', 'CYAN')} {self.color(str(i), 'DIM'):<4} {self.color('│', 'CYAN')} "
                  f"{self.color(stock_name, 'BOLD'):<16} {self.color('│', 'CYAN')} "
                  f"{status_disp:<8} {self.color('│', 'CYAN')} "
                  f"{self.color(ltp_str, 'YELLOW'):<8} {self.color('│', 'CYAN')} "
                  f"{self.color(bid_str, 'GREEN'):<8} {self.color('│', 'CYAN')} "
                  f"{self.color(ask_str, 'RED'):<8} {self.color('│', 'CYAN')}")
        
        print(f"{self.color('└──────┴──────────────────┴──────────┴──────────┴──────────┴──────────┘', 'CYAN')}")
        print()
    
    def print_depth_details(self, display_data: Dict):
        """Print depth details for first connected stock"""
        stocks = display_data['stocks']
        
        # Find first connected stock with data
        selected_stock = None
        for stock_id, data in stocks.items():
            if data['connection'] == "CONNECTED" and data['ltp']:
                selected_stock = (stock_id, data)
                break
        
        if not selected_stock:
            return
        
        stock_id, data = selected_stock
        
        print(f"{self.color('┌─────────────────────────────────────────────────────────────────────┐', 'BLUE')}")
        print(f"{self.color('│', 'BLUE')} {self.color('DEPTH DETAILS: ' + data['name'], 'BOLD'):<59} {self.color('│', 'BLUE')}")
        print(f"{self.color('├──────────────────┬──────────────────┬──────────────────┬────────────┤', 'BLUE')}")
        print(f"{self.color('│', 'BLUE')} {self.color('Bids (Top 5)', 'GREEN'):<16} {self.color('│', 'BLUE')} "
              f"{self.color('Price', 'DIM'):<16} {self.color('│', 'BLUE')} "
              f"{self.color('Asks (Top 5)', 'RED'):<16} {self.color('│', 'BLUE')} "
              f"{self.color('Price', 'DIM'):<10} {self.color('│', 'BLUE')}")
        print(f"{self.color('├──────────────────┼──────────────────┼──────────────────┼────────────┤', 'BLUE')}")
        
        # Print bid/ask ladder
        for i in range(5):
            bid_data = data['bids'][i] if i < len(data['bids']) else None
            ask_data = data['asks'][i] if i < len(data['asks']) else None
            
            # Bid side
            if bid_data:
                bid_qty = f"{bid_data['quantity']:,}"
                bid_orders = f"({bid_data['orders']})"
                bid_price = f"₹{bid_data['price']:,.2f}"
                bid_str = f"{self.color(bid_qty, 'GREEN'):>8} {self.color(bid_orders, 'DIM'):>6}"
            else:
                bid_str = " " * 16
                bid_price = " " * 16
            
            # Ask side
            if ask_data:
                ask_qty = f"{ask_data['quantity']:,}"
                ask_orders = f"({ask_data['orders']})"
                ask_price = f"₹{ask_data['price']:,.2f}"
                ask_str = f"{self.color(ask_qty, 'RED'):>8} {self.color(ask_orders, 'DIM'):>6}"
            else:
                ask_str = " " * 16
                ask_price = " " * 10
            
            print(f"{self.color('│', 'BLUE')} {bid_str} {self.color('│', 'BLUE')} "
                  f"{self.color(bid_price, 'GREEN') if bid_data else '':<16} {self.color('│', 'BLUE')} "
                  f"{ask_str} {self.color('│', 'BLUE')} "
                  f"{self.color(ask_price, 'RED') if ask_data else '':<10} {self.color('│', 'BLUE')}")
        
        print(f"{self.color('├──────────────────┴──────────────────┴──────────────────┴────────────┤', 'BLUE')}")
        
        # Summary
        total_orders = data['total_orders']
        total_qty = data['total_quantity']
        spread = data['best_ask'] - data['best_bid'] if data['best_bid'] and data['best_ask'] else 0
        
        print(f"{self.color('│', 'BLUE')} {self.color('Total Orders:', 'DIM')} {total_orders:,} "
              f"{self.color('│', 'BLUE')} {self.color('Total Qty:', 'DIM')} {total_qty:,} "
              f"{self.color('│', 'BLUE')} {self.color('Spread:', 'DIM')} ₹{spread:.2f} "
              f"{self.color('│', 'BLUE')}")
        
        print(f"{self.color('└─────────────────────────────────────────────────────────────────────┘', 'BLUE')}")
    
    def print_footer(self):
        """Print footer"""
        print()
        print(f"{self.color('─' * 70, 'CYAN')}")
        print(f"{self.color('ℹ️', 'DIM')} {self.color('Press Ctrl+C to exit', 'DIM')} | "
              f"{self.color('Data updates in real-time', 'DIM')} | "
              f"{self.color('Display refreshes every second', 'DIM')}")
    
    async def run_dashboard(self):
        """Run the dashboard"""
        self.is_running = True
        
        # Clear screen initially
        self.clear_screen()
        
        # Calculate total display height
        total_height = 25  # Approximate lines needed
        
        try:
            while self.is_running:
                # Get display data
                display_data = self.monitor.get_display_data()
                
                # Move cursor to top
                print(f"\033[{total_height}A", end="")
                
                # Print all sections
                self.print_header(display_data)
                self.print_stock_summary(display_data)
                self.print_depth_details(display_data)
                self.print_footer()
                
                # Flush output
                sys.stdout.flush()
                
                # Wait for next refresh
                await asyncio.sleep(self.refresh_rate)
                
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Dashboard error: {e}")
        finally:
            self.is_running = False

class WebSocketManager:
    """Manages WebSocket connections"""
    
    def __init__(self, token: str, client_id: str, monitor: DepthDataMonitor):
        self.token = token
        self.client_id = client_id
        self.monitor = monitor
        self.is_running = False
        self.connection_tasks = {}
    
    def parse_depth_message(self, message: bytes):
        """Parse binary depth message"""
        if len(message) < 12:
            return None
        
        try:
            header = message[:12]
            msg_length, feed_code, exchange_seg, security_id, num_rows = struct.unpack('<HBBII', header)
            
            if len(message) < msg_length:
                return None
            
            data = message[12:msg_length]
            row_size = 16
            
            bids = []
            asks = []
            
            for i in range(num_rows):
                start = i * row_size
                end = start + row_size
                row_data = data[start:end]
                
                price_raw, quantity, orders, flags = struct.unpack('<qihh', row_data)
                price = price_raw / 100.0
                
                if i < 100:  # First 100 are bids
                    bids.append({
                        'price': price,
                        'quantity': quantity,
                        'orders': orders,
                        'flags': flags
                    })
                else:  # Next 100 are asks
                    asks.append({
                        'price': price,
                        'quantity': quantity,
                        'orders': orders,
                        'flags': flags
                    })
            
            return str(security_id), bids, asks
            
        except Exception as e:
            logger.error(f"Parse error: {e}")
            return None
    
    async def maintain_connection(self, security_id: str):
        """Maintain connection to a stock"""
        uri = f"wss://full-depth-api.dhan.co/twohundreddepth?token={self.token}&clientId={self.client_id}&authType=2"
        
        while self.is_running:
            try:
                # Update status
                self.monitor.update_status(security_id, "CONNECTING")
                
                # Connect
                websocket = await asyncio.wait_for(
                    websockets.connect(
                        uri, 
                        ping_interval=20,
                        ping_timeout=10
                    ),
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
                last_ping = time.time()
                
                while self.is_running:
                    try:
                        # Receive message
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=1
                        )
                        
                        if isinstance(message, bytes):
                            result = self.parse_depth_message(message)
                            if result:
                                stock_id, bids, asks = result
                                self.monitor.update_stock(stock_id, bids, asks)
                        
                        # Send ping periodically
                        if time.time() - last_ping > 15:
                            await websocket.ping()
                            last_ping = time.time()
                            
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        break
                    except Exception as e:
                        logger.error(f"Message error for {security_id}: {e}")
                        break
                
                # Close connection
                try:
                    await websocket.close()
                except:
                    pass
                    
            except asyncio.TimeoutError:
                logger.warning(f"Timeout for {security_id}")
            except Exception as e:
                logger.error(f"Connection error for {security_id}: {e}")
            
            # Update status
            self.monitor.update_status(security_id, "DISCONNECTED")
            
            # Retry with backoff
            if self.is_running:
                retry_delay = min(2 ** 5, 30)
                retry_delay += random.uniform(-2, 2)
                await asyncio.sleep(retry_delay)
    
    async def start(self):
        """Start all connections"""
        self.is_running = True
        
        tasks = []
        for stock_id in self.monitor.selected_stocks:
            task = asyncio.create_task(self.maintain_connection(stock_id))
            tasks.append(task)
            self.connection_tasks[stock_id] = task
            
            await asyncio.sleep(0.1)
        
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
    
    async def stop(self):
        """Stop all connections"""
        self.is_running = False
        
        for task in self.connection_tasks.values():
            task.cancel()
        
        if self.connection_tasks:
            await asyncio.gather(*self.connection_tasks.values(), return_exceptions=True)

async def main():
    """Main function"""
    TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY5MDUyNzI3LCJpYXQiOjE3Njg5NjYzMjcsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.YDC_iXbY2nx0SrfHnxdTB8NoWEL-Ff6eTMQ4-Evhfl4-H5VUrvJKPhVk7neyxkNcZhXQ0XyBcGqLBln11c8M9g'
    CLIENT_ID = '1108703565'
    
    print("\n" + "="*70)
    print("📊 REAL-TIME STOCK DEPTH DATA MONITOR")
    print("="*70)
    print("• Monitors 10 Gold/Silver ETFs")
    print("• Displays LTP, Best Bid/Ask")
    print("• Shows top 5 bid/ask levels with orders & quantity")
    print("• Updates in real-time")
    print("\nInitializing...")
    
    # Create monitor and dashboard
    monitor = DepthDataMonitor()
    dashboard = CleanDashboard(monitor)
    
    # Create WebSocket manager
    ws_manager = WebSocketManager(TOKEN, CLIENT_ID, monitor)
    
    try:
        # Run dashboard
        dashboard_task = asyncio.create_task(dashboard.run_dashboard())
        
        # Wait a moment
        await asyncio.sleep(1)
        
        # Start WebSocket connections
        print("\nConnecting to stocks...")
        await ws_manager.start()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        logger.error(f"Main error: {e}")
        print(f"\n❌ Error: {e}")
    finally:
        # Cleanup
        print("\n\nCleaning up...")
        dashboard.is_running = False
        await ws_manager.stop()
        
        # Clear screen and show final stats
        os.system('cls' if os.name == 'nt' else 'clear')
        
        stats = monitor.get_display_data()['stats']
        print("\n" + "="*70)
        print("📈 FINAL STATISTICS:")
        print("="*70)
        print(f"Total Runtime: {int(stats['runtime'])} seconds")
        print(f"Total Messages Received: {stats['total_messages']:,}")
        print(f"Average Messages/Second: {stats['messages_per_second']:.1f}")
        print(f"Stocks Monitored: {stats['total_stocks']}")
        print("="*70)
        print("\nThank you for using the Depth Data Monitor!")

if __name__ == "__main__":
    try:
        import websockets
    except ImportError:
        print("Installing websockets...")
        import subprocess
        subprocess.check_call(["pip", "install", "websockets"])
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
