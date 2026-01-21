import asyncio
import websockets
import json
import struct
from datetime import datetime
import logging
import time
from typing import Dict, List, Optional, Tuple
import os
import sys

# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(message)s'
)
logger = logging.getLogger(__name__)

class StockMonitor:
    """Monitor for real-time 200-depth market data for a specific stock"""
    
    def __init__(self, security_id: str, stock_name: str):
        self.security_id = security_id
        self.stock_name = stock_name
        
        # Market data storage (store all 100 levels for calculations)
        self.all_bids: List[Dict] = []  # All 100 bid levels
        self.all_asks: List[Dict] = []  # All 100 ask levels
        self.best_bid: Optional[float] = None
        self.best_ask: Optional[float] = None
        self.ltp: Optional[float] = None
        self.last_update: datetime = datetime.now()
        
        # Column sums from ALL 100 levels
        self.bid_qty_sum: int = 0
        self.bid_orders_sum: int = 0
        self.bid_total_value: float = 0  # Sum of (price * quantity)
        
        self.ask_qty_sum: int = 0
        self.ask_orders_sum: int = 0
        self.ask_total_value: float = 0  # Sum of (price * quantity)
        
        # Statistics
        self.total_messages = 0
        self.message_rate = 0
        self.message_timestamps = []
        
        # Connection status
        self.connected = False
        
        # Price statistics
        self.bid_avg_price: float = 0
        self.ask_avg_price: float = 0
        self.bid_max_price: float = 0
        self.bid_min_price: float = 0
        self.ask_max_price: float = 0
        self.ask_min_price: float = 0
    
    def update_market_data(self, bids: List[Dict], asks: List[Dict]):
        """Update market depth data and calculate sums"""
        # Store all 100 levels
        self.all_bids = bids
        self.all_asks = asks
        
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
        
        # Calculate column sums from ALL 100 levels
        self.calculate_column_sums()
        
        # Track update rate
        self.last_update = datetime.now()
        self.total_messages += 1
        current_time = time.time()
        self.message_timestamps.append(current_time)
        
        # Keep only last 5 seconds of timestamps
        cutoff = current_time - 5
        self.message_timestamps = [t for t in self.message_timestamps if t > cutoff]
        self.message_rate = len(self.message_timestamps) / 5 if self.message_timestamps else 0
    
    def calculate_column_sums(self):
        """Calculate sums of all columns from all 100 levels"""
        # Reset sums
        self.bid_qty_sum = 0
        self.bid_orders_sum = 0
        self.bid_total_value = 0
        self.ask_qty_sum = 0
        self.ask_orders_sum = 0
        self.ask_total_value = 0
        
        # Initialize price statistics
        if self.all_bids:
            bid_prices = [bid['price'] for bid in self.all_bids]
            self.bid_max_price = max(bid_prices)
            self.bid_min_price = min(bid_prices)
        else:
            self.bid_max_price = 0
            self.bid_min_price = 0
            
        if self.all_asks:
            ask_prices = [ask['price'] for ask in self.all_asks]
            self.ask_max_price = max(ask_prices)
            self.ask_min_price = min(ask_prices)
        else:
            self.ask_max_price = 0
            self.ask_min_price = 0
        
        # Sum all bid levels (100 levels)
        for bid in self.all_bids:
            self.bid_qty_sum += bid.get('quantity', 0)
            self.bid_orders_sum += bid.get('orders', 0)
            self.bid_total_value += bid.get('price', 0) * bid.get('quantity', 0)
        
        # Sum all ask levels (100 levels)
        for ask in self.all_asks:
            self.ask_qty_sum += ask.get('quantity', 0)
            self.ask_orders_sum += ask.get('orders', 0)
            self.ask_total_value += ask.get('price', 0) * ask.get('quantity', 0)
        
        # Calculate average prices
        if self.bid_qty_sum > 0:
            self.bid_avg_price = self.bid_total_value / self.bid_qty_sum
        else:
            self.bid_avg_price = 0
            
        if self.ask_qty_sum > 0:
            self.ask_avg_price = self.ask_total_value / self.ask_qty_sum
        else:
            self.ask_avg_price = 0
    
    def get_summary_data(self) -> Dict:
        """Get summary data for this stock"""
        return {
            'stock_name': self.stock_name,
            'ltp': self.ltp or 0,
            'best_bid': self.best_bid or 0,
            'best_ask': self.best_ask or 0,
            'bid_qty_sum': self.bid_qty_sum,
            'bid_orders_sum': self.bid_orders_sum,
            'bid_avg_price': self.bid_avg_price,
            'ask_qty_sum': self.ask_qty_sum,
            'ask_orders_sum': self.ask_orders_sum,
            'ask_avg_price': self.ask_avg_price,
            'total_qty': self.bid_qty_sum + self.ask_qty_sum,
            'total_orders': self.bid_orders_sum + self.ask_orders_sum,
            'bid_total_value': self.bid_total_value,
            'ask_total_value': self.ask_total_value,
            'message_rate': self.message_rate,
            'connected': self.connected,
            'last_update': (datetime.now() - self.last_update).total_seconds()
        }
    
    def update_connection_status(self, status: bool):
        """Update connection status"""
        self.connected = status


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
    
    def __init__(self, token: str, client_id: str, monitor: StockMonitor):
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
                            if depth_data and str(depth_data['security_id']) == str(self.monitor.security_id):
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


class AllStocksSummaryDisplay:
    """Display showing summary data for ALL stocks in a static table"""
    
    def __init__(self, monitors: List[StockMonitor]):
        self.monitors = monitors
        self.is_running = False
        self.refresh_rate = 0.5  # Update every 0.5 seconds
        
        # Fixed table dimensions
        self.total_height = 0  # Will be calculated based on number of stocks
        self.header_line = 5    # Line for header info
        self.table_start = 7    # Start of table rows
        self.status_line = 0    # Will be calculated
        
        # Fixed column widths for consistent layout
        self.column_widths = {
            'stock': 12,
            'ltp': 10,
            'bid': 10,
            'ask': 10,
            'spread': 10,
            'bid_qty': 15,
            'ask_qty': 15,
            'bid_orders': 15,
            'ask_orders': 15,
            'bid_avg': 12,
            'ask_avg': 12,
            'total_qty': 15,
            'status': 8
        }
        
        self.initialized = False
        self.last_update_time = datetime.now()
        
    def clear_screen(self):
        """Clear terminal screen - called only once at start"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def print_static_table(self):
        """Print the static summary table ONE TIME ONLY"""
        # Clear screen once
        self.clear_screen()
        
        # Calculate table height based on number of stocks
        num_stocks = len(self.monitors)
        self.total_height = num_stocks + 10  # Header + stocks + summary + status
        self.status_line = self.total_height - 2
        
        # Print header section
        print("\n" + "="*180)
        print("📊 ALL STOCKS MARKET DEPTH SUMMARY (Data from ALL 100 Levels)")
        print("="*180)
        print("Auto-monitoring all 10 stocks | Press Ctrl+C to exit")
        print("="*180 + "\n")
        
        # Information line
        print("SUMMARY DATA - Hiding individual bid/ask levels, showing only column sums from 100 depth levels")
        print("─" * 180)
        
        # Table header with fixed column widths
        header = f"{'Stock':>{self.column_widths['stock']}} | " \
                 f"{'LTP':>{self.column_widths['ltp']}} | " \
                 f"{'Best Bid':>{self.column_widths['bid']}} | " \
                 f"{'Best Ask':>{self.column_widths['ask']}} | " \
                 f"{'Spread':>{self.column_widths['spread']}} | " \
                 f"{'Bid Qty SUM':>{self.column_widths['bid_qty']}} | " \
                 f"{'Ask Qty SUM':>{self.column_widths['ask_qty']}} | " \
                 f"{'Bid Orders SUM':>{self.column_widths['bid_orders']}} | " \
                 f"{'Ask Orders SUM':>{self.column_widths['ask_orders']}} | " \
                 f"{'Bid Avg':>{self.column_widths['bid_avg']}} | " \
                 f"{'Ask Avg':>{self.column_widths['ask_avg']}} | " \
                 f"{'Total Qty':>{self.column_widths['total_qty']}} | " \
                 f"{'Status':>{self.column_widths['status']}}"
        print(header)
        print("─" * 180)
        
        # Stock rows with placeholders
        for i, monitor in enumerate(self.monitors):
            row = f"{monitor.stock_name:>{self.column_widths['stock']}} | " \
                  f"{'-':>{self.column_widths['ltp']}} | " \
                  f"{'-':>{self.column_widths['bid']}} | " \
                  f"{'-':>{self.column_widths['ask']}} | " \
                  f"{'-':>{self.column_widths['spread']}} | " \
                  f"{'-':>{self.column_widths['bid_qty']}} | " \
                  f"{'-':>{self.column_widths['ask_qty']}} | " \
                  f"{'-':>{self.column_widths['bid_orders']}} | " \
                  f"{'-':>{self.column_widths['ask_orders']}} | " \
                  f"{'-':>{self.column_widths['bid_avg']}} | " \
                  f"{'-':>{self.column_widths['ask_avg']}} | " \
                  f"{'-':>{self.column_widths['total_qty']}} | " \
                  f"{'⏳':>{self.column_widths['status']}}"
            print(row)
        
        # Separator
        print("─" * 180)
        
        # Grand totals row (will be updated)
        print(f"{'GRAND TOTALS':>{self.column_widths['stock']}} | " \
              f"{' ':>{self.column_widths['ltp']}} | " \
              f"{' ':>{self.column_widths['bid']}} | " \
              f"{' ':>{self.column_widths['ask']}} | " \
              f"{' ':>{self.column_widths['spread']}} | " \
              f"{'-':>{self.column_widths['bid_qty']}} | " \
              f"{'-':>{self.column_widths['ask_qty']}} | " \
              f"{'-':>{self.column_widths['bid_orders']}} | " \
              f"{'-':>{self.column_widths['ask_orders']}} | " \
              f"{'-':>{self.column_widths['bid_avg']}} | " \
              f"{'-':>{self.column_widths['ask_avg']}} | " \
              f"{'-':>{self.column_widths['total_qty']}} | " \
              f"{' ':>{self.column_widths['status']}}")
        
        # Status line (will be updated)
        print("─" * 180)
        print(" " * 180)
        
        # Save cursor position at the bottom for later restoration
        print("\033[s", end="")
        sys.stdout.flush()
        
        self.initialized = True
    
    def update_table_value(self, line_num: int, text: str):
        """Update a specific line in the existing table using cursor positioning"""
        # Move cursor to specific line, clear it, and print new content
        print("\033[u", end="")  # Restore cursor to saved position
        print(f"\033[{self.total_height - line_num}A", end="")  # Move up to target line
        print("\033[2K", end="")  # Clear entire line
        print(text)  # Print new content
        print("\033[s", end="")  # Save cursor position again
        sys.stdout.flush()
    
    def update_stock_rows(self):
        """Update all stock rows with current summary data"""
        for i, monitor in enumerate(self.monitors):
            summary = monitor.get_summary_data()
            
            # Calculate spread
            spread = summary['best_ask'] - summary['best_bid'] if summary['best_bid'] and summary['best_ask'] else 0
            spread_pct = (spread / summary['ltp'] * 100) if summary['ltp'] > 0 else 0
            
            # Format spread display
            if spread > 0:
                spread_display = f"{spread:.2f}"
            else:
                spread_display = f"{spread:.2f}"
            
            # Determine status indicator
            if not summary['connected']:
                status = "🔴"
            elif summary['last_update'] > 5:
                status = "🟡"
            else:
                status = "🟢"
            
            # Format the row
            row_text = f"{summary['stock_name']:>{self.column_widths['stock']}} | " \
                      f"{summary['ltp']:>{self.column_widths['ltp']}.2f} | " \
                      f"{summary['best_bid']:>{self.column_widths['bid']}.2f} | " \
                      f"{summary['best_ask']:>{self.column_widths['ask']}.2f} | " \
                      f"{spread_display:>{self.column_widths['spread']}} | " \
                      f"{summary['bid_qty_sum']:>{self.column_widths['bid_qty']},d} | " \
                      f"{summary['ask_qty_sum']:>{self.column_widths['ask_qty']},d} | " \
                      f"{summary['bid_orders_sum']:>{self.column_widths['bid_orders']},d} | " \
                      f"{summary['ask_orders_sum']:>{self.column_widths['ask_orders']},d} | " \
                      f"{summary['bid_avg_price']:>{self.column_widths['bid_avg']}.2f} | " \
                      f"{summary['ask_avg_price']:>{self.column_widths['ask_avg']}.2f} | " \
                      f"{summary['total_qty']:>{self.column_widths['total_qty']},d} | " \
                      f"{status:>{self.column_widths['status']}}"
            
            # Update this row (starting from line 7)
            self.update_table_value(self.table_start + i, row_text)
    
    def update_grand_totals(self):
        """Update the grand totals row with sums from all stocks"""
        # Calculate grand totals
        total_bid_qty = 0
        total_ask_qty = 0
        total_bid_orders = 0
        total_ask_orders = 0
        total_qty = 0
        total_bid_value = 0
        total_ask_value = 0
        
        connected_stocks = 0
        total_messages = 0
        
        for monitor in self.monitors:
            summary = monitor.get_summary_data()
            total_bid_qty += summary['bid_qty_sum']
            total_ask_qty += summary['ask_qty_sum']
            total_bid_orders += summary['bid_orders_sum']
            total_ask_orders += summary['ask_orders_sum']
            total_qty += summary['total_qty']
            total_bid_value += summary['bid_total_value']
            total_ask_value += summary['ask_total_value']
            total_messages += monitor.total_messages
            
            if summary['connected']:
                connected_stocks += 1
        
        # Calculate weighted averages
        if total_bid_qty > 0:
            grand_bid_avg = total_bid_value / total_bid_qty
        else:
            grand_bid_avg = 0
            
        if total_ask_qty > 0:
            grand_ask_avg = total_ask_value / total_ask_qty
        else:
            grand_ask_avg = 0
        
        # Format grand totals row
        totals_text = f"{'GRAND TOTALS':>{self.column_widths['stock']}} | " \
                     f"{' ':>{self.column_widths['ltp']}} | " \
                     f"{' ':>{self.column_widths['bid']}} | " \
                     f"{' ':>{self.column_widths['ask']}} | " \
                     f"{' ':>{self.column_widths['spread']}} | " \
                     f"{total_bid_qty:>{self.column_widths['bid_qty']},d} | " \
                     f"{total_ask_qty:>{self.column_widths['ask_qty']},d} | " \
                     f"{total_bid_orders:>{self.column_widths['bid_orders']},d} | " \
                     f"{total_ask_orders:>{self.column_widths['ask_orders']},d} | " \
                     f"{grand_bid_avg:>{self.column_widths['bid_avg']}.2f} | " \
                     f"{grand_ask_avg:>{self.column_widths['ask_avg']}.2f} | " \
                     f"{total_qty:>{self.column_widths['total_qty']},d} | " \
                     f"{'Σ':>{self.column_widths['status']}}"
        
        # Update grand totals row (line after all stock rows)
        self.update_table_value(self.table_start + len(self.monitors), totals_text)
    
    def update_status_line(self):
        """Update the status line"""
        current_time = datetime.now()
        update_age = (current_time - self.last_update_time).total_seconds()
        self.last_update_time = current_time
        
        # Count connected stocks
        connected = sum(1 for monitor in self.monitors if monitor.connected)
        total_messages = sum(monitor.total_messages for monitor in self.monitors)
        
        # Calculate average message rate
        avg_rate = sum(monitor.message_rate for monitor in self.monitors) / max(len(self.monitors), 1)
        
        status = f"🔄 Live Update | Connected: {connected}/{len(self.monitors)} stocks | " \
                f"Total Messages: {total_messages:,} | Avg Rate: {avg_rate:.1f}/s | " \
                f"Last Update: {update_age:.2f}s ago | {datetime.now().strftime('%H:%M:%S')}"
        
        self.update_table_value(self.status_line, status)
    
    async def run_display(self):
        """Main display loop - table printed ONCE, values updated in-place"""
        self.is_running = True
        
        try:
            # STEP 1: Print static table ONE TIME ONLY
            if not self.initialized:
                self.print_static_table()
            
            # STEP 2: Continuous update loop - ONLY updates values
            while self.is_running:
                # Update all dynamic values in the static table
                self.update_stock_rows()    # Updates all stock summary rows
                self.update_grand_totals()  # Updates grand totals
                self.update_status_line()   # Updates status
                
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
    """Main function - automatically monitors ALL stocks"""
    # Updated token
    TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY4OTgxNzQwLCJpYXQiOjE3Njg4OTUzNDAsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.YSsmuldr5miDdk7DFLMyuJjIiyfIu4DzMoe2Gp8KmqRI6i6RJBK7giRMaJlOs7iRy_Sxr3BhUy5FIFTDOAvi1Q"
    CLIENT_ID = "1108703565"
    
    # All popular Indian Stocks (automatically selected - no user input)
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
    
    print("\n" + "="*180)
    print("📊 AUTOMATIC ALL-STOCKS MARKET DEPTH MONITOR")
    print("="*180)
    print(f"Starting automatic monitoring of ALL {len(available_stocks)} stocks...")
    print("="*180 + "\n")
    
    try:
        # Create monitors and clients for ALL stocks
        monitors = []
        clients = []
        
        print("Initializing stock monitors...")
        for stock_id, stock_name in available_stocks.items():
            monitor = StockMonitor(stock_id, stock_name)
            client = WebSocketDepthClient(TOKEN, CLIENT_ID, monitor)
            monitors.append(monitor)
            clients.append(client)
            print(f"  ✓ {stock_name}")
        
        print(f"\n✅ Successfully initialized {len(monitors)} stock monitors")
        print("Starting WebSocket connections and display...\n")
        
        # Create display for all stocks
        display = AllStocksSummaryDisplay(monitors)
        
        # Wait a moment before starting display
        await asyncio.sleep(1)
        
        # Run all WebSocket clients and display concurrently
        display_task = asyncio.create_task(display.run_display())
        
        # Start all WebSocket clients
        client_tasks = []
        for client in clients:
            task = asyncio.create_task(client.start())
            client_tasks.append(task)
        
        # Wait for all tasks
        await asyncio.gather(display_task, *client_tasks)
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
    finally:
        # Cleanup
        print("\n\n✅ Application terminated")
        if 'monitors' in locals():
            total_messages = sum(monitor.total_messages for monitor in monitors)
            total_bid_qty = sum(monitor.bid_qty_sum for monitor in monitors)
            total_ask_qty = sum(monitor.ask_qty_sum for monitor in monitors)
            print(f"Total updates received: {total_messages:,}")
            print(f"Total Bid Quantity (all stocks): {total_bid_qty:,}")
            print(f"Total Ask Quantity (all stocks): {total_ask_qty:,}")
            print(f"Grand Total Quantity: {total_bid_qty + total_ask_qty:,}")


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
