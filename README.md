import asyncio
import websockets
import json
import struct
from datetime import datetime, timedelta, time as dt_time
import logging
import time
from typing import Dict, List, Optional, Tuple, Set, Any
import os
from dataclasses import dataclass
import threading
from collections import defaultdict
import random
import uuid
from enum import Enum
import sys
import csv


# Configure logging
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class OrderStatus(Enum):
    PENDING = "PENDING"
    PLACED = "PLACED"
    FILLED = "FILLED"
    EXECUTED = "EXECUTED"
    CANCELLED = "CANCELLED"
    ERROR = "ERROR"


class OrderType(Enum):
    BUY = "BUY"
    SELL = "SELL"


@dataclass
class PaperOrder:
    """Paper trading order"""
    order_id: str
    security_id: str
    name: str
    order_type: OrderType
    quantity: int
    entry_price: Optional[float] = None
    exit_price: Optional[float] = None
    target_price: Optional[float] = None
    status: OrderStatus = OrderStatus.PENDING
    placed_time: Optional[datetime] = None
    filled_time: Optional[datetime] = None
    exit_time: Optional[datetime] = None
    profit_loss: Optional[float] = None
    profit_loss_percent: Optional[float] = None
    
    def __post_init__(self):
        if self.order_type == OrderType.BUY and self.entry_price and self.target_price is None:
            self.target_price = self.entry_price * 1.01  # 1% profit target
    
    def calculate_pnl(self, current_price: float):
        """Calculate P&L based on current price"""
        if self.order_type == OrderType.BUY and self.entry_price:
            if self.status == OrderStatus.FILLED:
                self.profit_loss = (current_price - self.entry_price) * self.quantity
                self.profit_loss_percent = ((current_price - self.entry_price) / self.entry_price) * 100
                return self.profit_loss, self.profit_loss_percent
        return None, None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for saving"""
        return {
            'order_id': self.order_id,
            'security_id': self.security_id,
            'name': self.name,
            'order_type': self.order_type.value,
            'quantity': self.quantity,
            'entry_price': self.entry_price,
            'exit_price': self.exit_price,
            'target_price': self.target_price,
            'status': self.status.value,
            'placed_time': self.placed_time.isoformat() if self.placed_time else None,
            'filled_time': self.filled_time.isoformat() if self.filled_time else None,
            'exit_time': self.exit_time.isoformat() if self.exit_time else None,
            'profit_loss': self.profit_loss,
            'profit_loss_percent': self.profit_loss_percent
        }


@dataclass
class ETFData:
    """Data class for individual ETF"""
    security_id: str
    name: str
    bids: List[Dict] = None
    asks: List[Dict] = None
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    ltp: Optional[float] = None
    spread: Optional[float] = None
    bpi: Optional[float] = None
    bid_pressure: float = 0.0
    ask_pressure: float = 0.0
    bpi_signal: str = "NEUTRAL"
    last_update: datetime = None
    connection_status: str = "DISCONNECTED"
    connection_start_time: Optional[datetime] = None
    total_updates: int = 0
    current_price: Optional[float] = None
    
    def __post_init__(self):
        if self.bids is None:
            self.bids = []
        if self.asks is None:
            self.asks = []
        if self.last_update is None:
            self.last_update = datetime.now()
    
    @property
    def uptime(self) -> Optional[float]:
        """Get connection uptime in seconds"""
        if self.connection_start_time:
            return (datetime.now() - self.connection_start_time).total_seconds()
        return None
    
    @property
    def update_rate(self) -> float:
        """Get updates per second"""
        uptime = self.uptime
        if uptime and uptime > 0:
            return self.total_updates / uptime
        return 0.0
    
    def update_current_price(self):
        """Update current price from market data"""
        if self.best_bid and self.best_ask:
            self.current_price = (self.best_bid + self.best_ask) / 2
        elif self.best_bid:
            self.current_price = self.best_bid
        elif self.best_ask:
            self.current_price = self.best_ask
        else:
            self.current_price = None


class BPIETFMonitor:
    """Monitor for gold and silver ETFs with BPI calculation and paper trading"""
    
    def __init__(self):
        self.etfs: Dict[str, ETFData] = {}
        
        # Gold and Silver ETFs on NSE India
        self.etf_names = {
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
        self.selected_etfs = list(self.etf_names.keys())
        self.total_messages_received = 0
        self.start_time = datetime.now()
        
        # Paper trading variables
        self.active_order: Optional[PaperOrder] = None
        self.order_history: List[PaperOrder] = []
        self.trading_enabled: bool = True
        self.order_placed_today: bool = False
        self.target_hour: int = 9
        self.target_minute: int = 7
        self.target_second: int = 5
        self.quantity_to_trade: int = 10  # Default quantity
        
        # Trade saving
        self.trade_save_path = "paper_trades"
        if not os.path.exists(self.trade_save_path):
            os.makedirs(self.trade_save_path)
        
        # CSV data saving
        self.csv_save_path = "depth_data"
        if not os.path.exists(self.csv_save_path):
            os.makedirs(self.csv_save_path)
        
        # Initialize CSV files
        self.csv_files: Dict[str, csv.writer] = {}
        self.csv_file_handles: Dict[str, Any] = {}
        self.initialize_csv_files()
        
        # Display variables
        self.display_update_counter = 0
        self.last_display_update = datetime.now()
        
        # Initialize all ETFs
        for etf_id in self.selected_etfs:
            etf_name = self.etf_names.get(etf_id, etf_id)
            self.etfs[etf_id] = ETFData(
                security_id=etf_id,
                name=etf_name
            )
    
    def initialize_csv_files(self):
        """Initialize CSV files for each ETF"""
        for etf_id, etf_name in self.etf_names.items():
            filename = os.path.join(self.csv_save_path, f"{etf_name}_{etf_id}_depth_data.csv")
            
            # Create file if it doesn't exist
            if not os.path.exists(filename):
                with open(filename, 'w', newline='') as csvfile:
                    writer = csv.writer(csvfile)
                    # Write headers
                    writer.writerow([
                        'timestamp', 'security_id', 'etf_name',
                        'bid_level', 'bid_price', 'bid_quantity', 'bid_orders',
                        'ask_level', 'ask_price', 'ask_quantity', 'ask_orders',
                        'best_bid', 'best_ask', 'ltp', 'spread'
                    ])
            
            # Open file for appending
            file_handle = open(filename, 'a', newline='')
            writer = csv.writer(file_handle)
            
            self.csv_file_handles[etf_id] = file_handle
            self.csv_files[etf_id] = writer
    
    def save_depth_data_to_csv(self, etf: ETFData):
        """Save depth data to CSV file"""
        try:
            if etf.security_id not in self.csv_files:
                return
            
            writer = self.csv_files[etf.security_id]
            timestamp = datetime.now()
            
            # Get the maximum number of levels to save
            max_levels = max(len(etf.bids), len(etf.asks))
            
            for level in range(max_levels):
                row = [timestamp.isoformat(), etf.security_id, etf.name]
                
                # Add bid data if available
                if level < len(etf.bids):
                    bid = etf.bids[level]
                    row.extend([bid['level'], bid['price'], bid['quantity'], bid['orders']])
                else:
                    row.extend(['', '', '', ''])
                
                # Add ask data if available
                if level < len(etf.asks):
                    ask = etf.asks[level]
                    row.extend([ask['level'], ask['price'], ask['quantity'], ask['orders']])
                else:
                    row.extend(['', '', '', ''])
                
                # Add summary data (only for first row)
                if level == 0:
                    row.extend([
                        etf.best_bid if etf.best_bid else '',
                        etf.best_ask if etf.best_ask else '',
                        etf.ltp if etf.ltp else '',
                        etf.spread if etf.spread else ''
                    ])
                else:
                    row.extend(['', '', '', ''])
                
                writer.writerow(row)
            
            # Flush to ensure data is written
            self.csv_file_handles[etf.security_id].flush()
            
        except Exception as e:
            logger.error(f"Error saving depth data for {etf.security_id} to CSV: {e}")
    
    def update_etf(self, security_id: str, bids: List[Dict], asks: List[Dict]):
        """Update ETF data and calculate BPI"""
        with self._lock:
            if security_id not in self.etfs:
                return
            
            etf = self.etfs[security_id]
            
            # Update ETF data
            etf.bids = bids
            etf.asks = asks
            etf.last_update = datetime.now()
            etf.total_updates += 1
            self.total_messages_received += 1
            
            # Sort bids and asks
            if bids:
                sorted_bids = sorted(bids, key=lambda x: x['price'], reverse=True)
                etf.best_bid = sorted_bids[0]['price']
            else:
                etf.best_bid = None
            
            if asks:
                sorted_asks = sorted(asks, key=lambda x: x['price'])
                etf.best_ask = sorted_asks[0]['price']
            else:
                etf.best_ask = None
            
            # Calculate LTP as midpoint
            if etf.best_bid and etf.best_ask:
                etf.ltp = (etf.best_bid + etf.best_ask) / 2
                etf.spread = etf.best_ask - etf.best_bid
            elif etf.best_bid:
                etf.ltp = etf.best_bid
                etf.spread = 0
            elif etf.best_ask:
                etf.ltp = etf.best_ask
                etf.spread = 0
            else:
                etf.ltp = None
                etf.spread = None
            
            # Update current price
            etf.update_current_price()
            
            # Calculate BPI
            self.calculate_etf_bpi(etf)
            
            # Save depth data to CSV
            self.save_depth_data_to_csv(etf)
            
            # Check if we need to execute paper trading
            self.check_paper_trading(etf)
            
            # Increment display counter
            self.display_update_counter += 1
    
    def update_etf_status(self, security_id: str, status: str):
        """Update connection status for an ETF"""
        with self._lock:
            if security_id in self.etfs:
                etf = self.etfs[security_id]
                etf.connection_status = status
                if status == "CONNECTED" and etf.connection_start_time is None:
                    etf.connection_start_time = datetime.now()
                elif status == "DISCONNECTED":
                    etf.connection_start_time = None
    
    def calculate_etf_bpi(self, etf: ETFData):
        """Calculate BPI for a single ETF"""
        try:
            # Calculate bid pressure: Σ(BidPrice × BidOrders × BidQuantity)
            etf.bid_pressure = sum(
                bid['price'] * bid['orders'] * bid['quantity']
                for bid in etf.bids
            ) / 1000000  # Normalize to millions
            
            # Calculate ask pressure: Σ(AskPrice × AskOrders × AskQuantity)
            etf.ask_pressure = sum(
                ask['price'] * ask['orders'] * ask['quantity']
                for ask in etf.asks
            ) / 1000000  # Normalize to millions
            
            # Calculate BPI
            if etf.ask_pressure > 0:
                etf.bpi = etf.bid_pressure / etf.ask_pressure
            else:
                etf.bpi = float('inf') if etf.bid_pressure > 0 else 1.0
            
            # Determine BPI signal
            if etf.bpi is None:
                etf.bpi_signal = "ERROR"
            elif etf.bpi > 1.2:
                etf.bpi_signal = "STRONG BUY"
            elif etf.bpi > 1.05:
                etf.bpi_signal = "BUY"
            elif etf.bpi > 0.95:
                etf.bpi_signal = "NEUTRAL"
            elif etf.bpi > 0.8:
                etf.bpi_signal = "SELL"
            else:
                etf.bpi_signal = "STRONG SELL"
                
        except Exception as e:
            logger.error(f"Error calculating BPI for {etf.security_id}: {e}")
            etf.bpi = None
            etf.bpi_signal = "ERROR"
    
    def get_highest_bpi_etf(self) -> Optional[ETFData]:
        """Get ETF with highest BPI"""
        with self._lock:
            connected_etfs = [
                etf for etf in self.etfs.values() 
                if etf.connection_status == "CONNECTED" 
                and etf.bpi is not None
                and etf.current_price is not None
            ]
            
            if not connected_etfs:
                return None
            
            # Sort by BPI (highest first)
            sorted_etfs = sorted(
                connected_etfs,
                key=lambda x: x.bpi if x.bpi != float('inf') else float('inf'),
                reverse=True
            )
            
            return sorted_etfs[0] if sorted_etfs else None
    
    def check_time_for_order(self) -> bool:
        """Check if current time is 09:07:05 AM"""
        now = datetime.now()
        return (
            now.hour == self.target_hour and
            now.minute == self.target_minute and
            now.second == self.target_second
        )
    
    def check_paper_trading(self, etf: ETFData):
        """Check and execute paper trading logic"""
        with self._lock:
            # Check if we should place an order
            if (self.trading_enabled and 
                not self.active_order and 
                not self.order_placed_today and
                self.check_time_for_order()):
                
                highest_bpi_etf = self.get_highest_bpi_etf()
                if highest_bpi_etf and highest_bpi_etf.current_price:
                    self.place_paper_order(highest_bpi_etf)
                    self.order_placed_today = True
                    logger.info(f"Paper order placed for {highest_bpi_etf.name} at {highest_bpi_etf.current_price}")
            
            # Check if we have an active order to monitor
            if self.active_order and self.active_order.security_id == etf.security_id:
                self.monitor_active_order(etf)
    
    def place_paper_order(self, etf: ETFData):
        """Place a paper buy order"""
        order_id = str(uuid.uuid4())[:8]
        
        self.active_order = PaperOrder(
            order_id=order_id,
            security_id=etf.security_id,
            name=etf.name,
            order_type=OrderType.BUY,
            quantity=self.quantity_to_trade,
            entry_price=etf.current_price,
            status=OrderStatus.PLACED,
            placed_time=datetime.now()
        )
        
        # Simulate order fill (immediate for paper trading)
        self.active_order.status = OrderStatus.FILLED
        self.active_order.filled_time = datetime.now()
        
        logger.info(f"Paper BUY order filled for {etf.name}: {self.quantity_to_trade} shares at ₹{etf.current_price:.2f}")
    
    def monitor_active_order(self, etf: ETFData):
        """Monitor active order for exit conditions"""
        if not self.active_order or self.active_order.status != OrderStatus.FILLED:
            return
        
        if not etf.current_price:
            return
        
        # Calculate current P&L
        current_pnl, current_pnl_percent = self.active_order.calculate_pnl(etf.current_price)
        
        # Check for 1% profit target
        if (current_pnl_percent and current_pnl_percent >= 1.0 and
            self.active_order.status == OrderStatus.FILLED):
            
            # Place sell order to exit
            self.exit_paper_order(etf)
    
    def exit_paper_order(self, etf: ETFData):
        """Exit the paper order at current price"""
        if not self.active_order:
            return
        
        self.active_order.exit_price = etf.current_price
        self.active_order.exit_time = datetime.now()
        self.active_order.status = OrderStatus.EXECUTED
        
        # Calculate final P&L
        if self.active_order.entry_price and self.active_order.exit_price:
            profit = (self.active_order.exit_price - self.active_order.entry_price) * self.active_order.quantity
            profit_percent = ((self.active_order.exit_price - self.active_order.entry_price) / self.active_order.entry_price) * 100
            
            self.active_order.profit_loss = profit
            self.active_order.profit_loss_percent = profit_percent
            
            # Add to history
            self.order_history.append(self.active_order)
            
            # Save trade to file
            self.save_trade(self.active_order)
            
            logger.info(f"Paper SELL order executed for {self.active_order.name}:")
            logger.info(f"   Entry: ₹{self.active_order.entry_price:.2f}")
            logger.info(f"   Exit: ₹{self.active_order.exit_price:.2f}")
            logger.info(f"   P&L: ₹{profit:.2f} ({profit_percent:.2f}%)")
            logger.info(f"   Holding Time: {(self.active_order.exit_time - self.active_order.filled_time).total_seconds():.1f}s")
        
        # Clear active order
        self.active_order = None
    
    def save_trade(self, order: PaperOrder):
        """Save paper trade to text file"""
        try:
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"paper_trade_{timestamp}_{order.order_id}.txt"
            filepath = os.path.join(self.trade_save_path, filename)
            
            with open(filepath, 'w') as f:
                f.write("=" * 60 + "\n")
                f.write(f"PAPER TRADE EXECUTION REPORT\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"TRADE DETAILS:\n")
                f.write(f"  Order ID:     {order.order_id}\n")
                f.write(f"  ETF:          {order.name} ({order.security_id})\n")
                f.write(f"  Type:         {order.order_type.value}\n")
                f.write(f"  Quantity:     {order.quantity} shares\n")
                f.write(f"  Status:       {order.status.value}\n\n")
                
                f.write(f"EXECUTION DETAILS:\n")
                f.write(f"  Entry Price:  ₹{order.entry_price:.2f}\n")
                f.write(f"  Exit Price:   ₹{order.exit_price:.2f}\n")
                f.write(f"  Target Price: ₹{order.target_price:.2f}\n\n")
                
                f.write(f"TIMING:\n")
                f.write(f"  Placed:       {order.placed_time}\n")
                f.write(f"  Filled:       {order.filled_time}\n")
                f.write(f"  Exit:         {order.exit_time}\n")
                
                if order.filled_time and order.exit_time:
                    holding_time = (order.exit_time - order.filled_time).total_seconds()
                    f.write(f"  Holding Time: {holding_time:.1f} seconds\n\n")
                
                f.write(f"PERFORMANCE:\n")
                f.write(f"  P&L:          ₹{order.profit_loss:+.2f}\n")
                f.write(f"  Return:       {order.profit_loss_percent:+.2f}%\n\n")
                
                f.write(f"ADDITIONAL INFO:\n")
                f.write(f"  Trade Time:   {datetime.now()}\n")
                f.write(f"  Trade Rule:   Buy highest BPI at 09:07:05, exit at 1% profit\n")
                
                f.write("\n" + "=" * 60 + "\n")
                f.write("END OF REPORT\n")
                f.write("=" * 60 + "\n")
            
            logger.info(f"Trade saved to: {filepath}")
            
            # Also save to a consolidated log file
            self.save_to_consolidated_log(order)
            
        except Exception as e:
            logger.error(f"Error saving trade: {e}")
    
    def save_to_consolidated_log(self, order: PaperOrder):
        """Save trade to consolidated log file"""
        try:
            log_file = os.path.join(self.trade_save_path, "paper_trades_log.txt")
            
            # Append to log file
            with open(log_file, 'a') as f:
                timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                f.write(f"{timestamp} | {order.order_id} | {order.name} | "
                       f"Entry: ₹{order.entry_price:.2f} | "
                       f"Exit: ₹{order.exit_price:.2f} | "
                       f"P&L: ₹{order.profit_loss:+.2f} | "
                       f"Return: {order.profit_loss_percent:+.2f}%\n")
            
        except Exception as e:
            logger.error(f"Error saving to consolidated log: {e}")
    
    def get_sorted_etfs(self) -> List[ETFData]:
        """Get ETFs sorted by BPI (highest first)"""
        with self._lock:
            valid_etfs = [
                etf for etf in self.etfs.values() 
                if etf.security_id in self.selected_etfs
            ]
            
            return sorted(
                valid_etfs,
                key=lambda x: (x.bpi is None, -x.bpi if x.bpi is not None and x.bpi != float('inf') else float('-inf')),
                reverse=False
            )
    
    def get_etf_count(self) -> tuple:
        """Get counts of connected and total ETFs"""
        with self._lock:
            total = len(self.selected_etfs)
            connected = sum(1 for s in self.etfs.values() 
                          if s.security_id in self.selected_etfs and s.connection_status == "CONNECTED")
            return connected, total
    
    def get_etf_type(self, etf_name: str) -> str:
        """Determine if ETF is Gold or Silver based on name"""
        etf_name_lower = etf_name.lower()
        if "gold" in etf_name_lower:
            return "GOLD"
        elif "silver" in etf_name_lower:
            return "SILVER"
        else:
            return "OTHER"
    
    def get_statistics(self) -> Dict:
        """Get overall statistics"""
        with self._lock:
            runtime = (datetime.now() - self.start_time).total_seconds()
            
            connected_etfs = [s for s in self.etfs.values() 
                             if s.security_id in self.selected_etfs 
                             and s.connection_status == "CONNECTED"]
            
            avg_update_rate = sum(etf.update_rate for etf in connected_etfs) / len(connected_etfs) if connected_etfs else 0
            
            return {
                "runtime_seconds": runtime,
                "total_messages": self.total_messages_received,
                "messages_per_second": self.total_messages_received / runtime if runtime > 0 else 0,
                "connected_count": len(connected_etfs),
                "avg_update_rate": avg_update_rate,
                "start_time": self.start_time,
                "paper_trading_enabled": self.trading_enabled,
                "active_order": bool(self.active_order),
                "orders_today": len(self.order_history),
                "display_updates": self.display_update_counter
            }
    
    def get_display_data(self) -> Dict:
        """Get formatted display data"""
        with self._lock:
            sorted_etfs = self.get_sorted_etfs()
            stats = self.get_statistics()
            highest_etf = self.get_highest_bpi_etf()
            
            # Format ETF data for display
            etf_display_data = []
            for etf in sorted_etfs:
                etf_type = self.get_etf_type(etf.name)
                
                # Calculate target price
                target_price = etf.current_price * 1.01 if etf.current_price else None
                
                etf_display_data.append({
                    "security_id": etf.security_id,
                    "name": etf.name,
                    "type": etf_type,
                    "connection_status": etf.connection_status,
                    "bpi": etf.bpi,
                    "bpi_signal": etf.bpi_signal,
                    "current_price": etf.current_price,
                    "target_price": target_price,
                    "bid_pressure": etf.bid_pressure,
                    "ask_pressure": etf.ask_pressure,
                    "update_age": (datetime.now() - etf.last_update).total_seconds(),
                    "update_rate": etf.update_rate,
                    "uptime": etf.uptime,
                    "is_highest_bpi": highest_etf and etf.security_id == highest_etf.security_id
                })
            
            # Get active order data
            active_order_data = None
            if self.active_order:
                active_order_data = {
                    "order_id": self.active_order.order_id,
                    "name": self.active_order.name,
                    "entry_price": self.active_order.entry_price,
                    "target_price": self.active_order.target_price,
                    "quantity": self.active_order.quantity,
                    "status": self.active_order.status.value,
                    "placed_time": self.active_order.placed_time,
                    "filled_time": self.active_order.filled_time
                }
                
                # Calculate current P&L if we have current price
                etf = self.etfs.get(self.active_order.security_id)
                if etf and etf.current_price:
                    current_pnl, current_pnl_percent = self.active_order.calculate_pnl(etf.current_price)
                    active_order_data["current_price"] = etf.current_price
                    active_order_data["current_pnl"] = current_pnl
                    active_order_data["current_pnl_percent"] = current_pnl_percent
                    
                    # Progress to target
                    if self.active_order.entry_price and self.active_order.target_price:
                        progress = ((etf.current_price - self.active_order.entry_price) / 
                                   (self.active_order.target_price - self.active_order.entry_price)) * 100
                        active_order_data["progress_to_target"] = progress
            
            # Get last order data
            last_order_data = None
            if self.order_history:
                last_order = self.order_history[-1]
                last_order_data = {
                    "name": last_order.name,
                    "entry_price": last_order.entry_price,
                    "exit_price": last_order.exit_price,
                    "profit_loss": last_order.profit_loss,
                    "profit_loss_percent": last_order.profit_loss_percent,
                    "filled_time": last_order.filled_time,
                    "exit_time": last_order.exit_time
                }
            
            # Update display timestamp
            self.last_display_update = datetime.now()
            
            return {
                "etfs": etf_display_data,
                "stats": stats,
                "active_order": active_order_data,
                "last_order": last_order_data,
                "trading_config": {
                    "target_hour": self.target_hour,
                    "target_minute": self.target_minute,
                    "target_second": self.target_second,
                    "quantity_to_trade": self.quantity_to_trade,
                    "order_placed_today": self.order_placed_today
                },
                "current_time": datetime.now(),
                "highest_etf": {
                    "security_id": highest_etf.security_id if highest_etf else None,
                    "name": highest_etf.name if highest_etf else None,
                    "bpi": highest_etf.bpi if highest_etf else None
                }
            }
    
    def cleanup(self):
        """Cleanup resources"""
        for file_handle in self.csv_file_handles.values():
            try:
                file_handle.close()
            except:
                pass


class CleanDashboard:
    """Clean dashboard without color coding"""
    
    def __init__(self, monitor: BPIETFMonitor):
        self.monitor = monitor
        self.is_running = False
        self.refresh_rate = 0.5  # Update every 0.5 seconds
        self.last_display_lines = 0
        
    def clear_screen(self):
        """Clear screen and move cursor to top"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def move_cursor_up(self, lines: int):
        """Move cursor up by specified number of lines"""
        print(f"\033[{lines}A", end="")
    
    def get_connection_icon(self, status: str) -> str:
        """Get compact connection status"""
        icons = {
            "CONNECTED": "[+]",
            "CONNECTING": "[~]",
            "DISCONNECTED": "[-]"
        }
        return icons.get(status, "[?]")
    
    def get_etf_abbreviation(self, name: str) -> str:
        """Get compact ETF abbreviation"""
        if name == "GOLDBEES":
            return "GLDB"
        elif name == "SILVERBEES":
            return "SLVB"
        elif "HDFC" in name and "GOLD" in name:
            return "HGLD"
        elif "KOTAK" in name:
            return "KGLD"
        elif "AXIS" in name and "GOLD" in name:
            return "AGLD"
        elif "AXIS" in name and "SILVER" in name:
            return "ASLV"
        elif "MIRAE" in name:
            return "MGLD"
        elif "UTI" in name:
            return "USLV"
        elif "HDFC" in name and "SILVER" in name:
            return "HSLV"
        else:
            return name[:4]
    
    def print_header(self, display_data: Dict):
        """Print clean header"""
        current_time = display_data["current_time"].strftime("%H:%M:%S")
        stats = display_data["stats"]
        
        connected = stats["connected_count"]
        total = len(self.monitor.selected_etfs)
        
        print("=" * 80)
        print(f"ETF BPI PAPER TRADING MONITOR - {current_time}")
        print(f"Connected: {connected}/{total} | Msg/sec: {stats['messages_per_second']:.0f}")
        print("=" * 80)
    
    def print_etf_table(self, display_data: Dict):
        """Print clean ETF table"""
        # Header
        print(f"{'#':<2} {'ETF':<6} {'Conn':<5} {'BPI':<7} {'Signal':<10} {'Price':<10} {'Target':<10}")
        print("-" * 80)
        
        # ETF rows
        for i, etf in enumerate(display_data["etfs"], 1):
            # ETF abbreviation
            etf_abbr = self.get_etf_abbreviation(etf["name"])
            
            # Connection status
            conn_icon = self.get_connection_icon(etf["connection_status"])
            
            # BPI and signal
            if etf["bpi"] is None:
                bpi_str = "N/A"
                signal_str = "N/A"
            else:
                if etf["bpi"] == float('inf'):
                    bpi_str = "∞"
                else:
                    bpi_str = f"{etf['bpi']:.2f}"
                signal_str = etf["bpi_signal"]
            
            # Price and target
            if etf["current_price"]:
                price_str = f"₹{etf['current_price']:.2f}"
                target_str = f"₹{etf['target_price']:.2f}" if etf["target_price"] else "N/A"
            else:
                price_str = "N/A"
                target_str = "N/A"
            
            # Highlight if it's the highest BPI
            row_prefix = "▶ " if etf["is_highest_bpi"] else "  "
            
            # Print row
            print(f"{row_prefix}{i:<2} {etf_abbr:<6} {conn_icon:<5} {bpi_str:<7} "
                  f"{signal_str:<10} {price_str:<10} {target_str:<10}")
        
        print("-" * 80)
    
    def print_trading_status(self, display_data: Dict):
        """Print clean trading status"""
        print("TRADING STATUS")
        print("-" * 40)
        
        # Active trade
        if display_data["active_order"]:
            order = display_data["active_order"]
            
            status_line = f"▶ {order['name']} BUY @ ₹{order['entry_price']:.2f}"
            
            if "current_price" in order:
                pnl_sign = "+" if order["current_pnl"] >= 0 else ""
                status_line += f" | P&L: ₹{pnl_sign}{order['current_pnl']:.2f} "
                status_line += f"({pnl_sign}{order['current_pnl_percent']:.2f}%)"
                
                if "progress_to_target" in order:
                    progress = min(max(order["progress_to_target"], 0), 100)
                    status_line += f" [{progress:.1f}%]"
            
            print(status_line)
        
        # Last completed trade
        elif display_data["last_order"]:
            order = display_data["last_order"]
            
            pnl_sign = "+" if order["profit_loss"] >= 0 else ""
            print(f"✓ {order['name']} CLOSED @ ₹{order['entry_price']:.2f} → ₹{order['exit_price']:.2f} | "
                  f"P&L: ₹{pnl_sign}{order['profit_loss']:.2f} "
                  f"({pnl_sign}{order['profit_loss_percent']:.2f}%)")
        
        # Waiting for trade time
        elif not display_data["trading_config"]["order_placed_today"]:
            target_time = f"{display_data['trading_config']['target_hour']:02d}:{display_data['trading_config']['target_minute']:02d}:{display_data['trading_config']['target_second']:02d}"
            current_time = display_data["current_time"]
            target_dt = current_time.replace(
                hour=display_data['trading_config']['target_hour'],
                minute=display_data['trading_config']['target_minute'],
                second=display_data['trading_config']['target_second'],
                microsecond=0
            )
            
            if current_time < target_dt:
                time_diff = (target_dt - current_time).total_seconds()
                print(f"⏳ Next trade at {target_time} (in {time_diff:.0f}s)")
            else:
                print(f"✓ Trade time ({target_time}) passed")
        
        # Today's summary
        if display_data["stats"]["orders_today"] > 0:
            total_pnl = sum(order.profit_loss for order in self.monitor.order_history if order.profit_loss)
            total_trades = display_data["stats"]["orders_today"]
            
            pnl_sign = "+" if total_pnl >= 0 else ""
            print(f"📊 Today: {total_trades} trade{'s' if total_trades != 1 else ''} | "
                  f"Total P&L: ₹{pnl_sign}{total_pnl:.2f}")
        
        print("-" * 80)
    
    def print_system_info(self, display_data: Dict):
        """Print clean system info"""
        stats = display_data["stats"]
        
        print(f"System: Runtime {stats['runtime_seconds']:.0f}s | "
              f"Updates: {stats['display_updates']:,} | "
              f"Press Ctrl+C to exit")
        print("=" * 80)
    
    async def run_dashboard(self):
        """Run the clean dashboard"""
        self.is_running = True
        
        # Clear screen initially
        self.clear_screen()
        
        # Calculate total lines for the display
        total_etfs = len(self.monitor.selected_etfs)
        header_height = 4
        table_height = total_etfs + 3  # Header + ETF rows + separator
        trading_height = 6
        system_height = 2
        
        total_height = header_height + table_height + trading_height + system_height
        
        try:
            while self.is_running:
                # Get display data
                display_data = self.monitor.get_display_data()
                
                # Move cursor to top
                print(f"\033[{total_height}A", end="")
                
                # Print all sections
                self.print_header(display_data)
                self.print_etf_table(display_data)
                self.print_trading_status(display_data)
                self.print_system_info(display_data)
                
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
    
    def stop(self):
        """Stop the dashboard"""
        self.is_running = False


class WebSocketManager:
    """Manages WebSocket connections for multiple ETFs with high reliability"""
    
    def __init__(self, token: str, client_id: str, monitor: BPIETFMonitor):
        self.token = token
        self.client_id = client_id
        self.monitor = monitor
        self.is_running = False
        self.connection_tasks = {}
        
        # Performance tracking
        self.total_connections = 0
        self.successful_connections = 0
        self.failed_connections = 0
        
    async def maintain_connection(self, security_id: str):
        """Maintain persistent connection to a single ETF"""
        uri = f"wss://full-depth-api.dhan.co/twohundreddepth?token={self.token}&clientId={self.client_id}&authType=2"
        
        while self.is_running:
            try:
                # Update status to connecting
                self.monitor.update_etf_status(security_id, "CONNECTING")
                
                # Connect with aggressive timeout settings
                websocket = await asyncio.wait_for(
                    websockets.connect(
                        uri, 
                        ping_interval=20,
                        ping_timeout=10,
                        close_timeout=5,
                        max_size=20*1024*1024,
                        compression=None
                    ),
                    timeout=5
                )
                
                self.total_connections += 1
                self.successful_connections += 1
                
                logger.info(f"Connected to {security_id}")
                
                # Send subscription request
                subscription = {
                    "RequestCode": 23,
                    "ExchangeSegment": "NSE_EQ",
                    "SecurityId": security_id
                }
                
                await websocket.send(json.dumps(subscription))
                logger.debug(f"Subscribed to {security_id}")
                
                # Update status to connected
                self.monitor.update_etf_status(security_id, "CONNECTED")
                
                # Heartbeat and message processing
                last_ping = time.time()
                last_message = time.time()
                
                while self.is_running:
                    try:
                        # Receive message with timeout
                        message = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=1
                        )
                        
                        last_message = time.time()
                        
                        if isinstance(message, bytes):
                            depth_data = parse_market_depth_message(message)
                            if depth_data:
                                self.monitor.update_etf(
                                    security_id,
                                    depth_data['market_depth']['bids'],
                                    depth_data['market_depth']['asks']
                                )
                        
                        # Send ping every 15 seconds
                        if time.time() - last_ping > 15:
                            try:
                                await websocket.ping()
                                last_ping = time.time()
                            except:
                                break
                                
                        # Check if we're getting data
                        if time.time() - last_message > 30:
                            logger.warning(f"No data from {security_id} for 30 seconds, reconnecting...")
                            break
                            
                    except asyncio.TimeoutError:
                        continue
                    except websockets.exceptions.ConnectionClosed:
                        logger.warning(f"Connection closed for {security_id}, reconnecting...")
                        break
                    except Exception as e:
                        logger.error(f"Error processing message for {security_id}: {e}")
                        break
                
                # Close connection gracefully
                try:
                    await websocket.close()
                except:
                    pass
                    
            except asyncio.TimeoutError:
                logger.warning(f"Connection timeout for {security_id}")
                self.failed_connections += 1
            except Exception as e:
                logger.error(f"Connection error for {security_id}: {e}")
                self.failed_connections += 1
            
            # Update status to disconnected
            self.monitor.update_etf_status(security_id, "DISCONNECTED")
            
            # Exponential backoff with jitter
            retry_delay = min(2 ** min(self.failed_connections, 5), 30)
            jitter = retry_delay * 0.1
            retry_delay += random.uniform(-jitter, jitter)
            
            if self.is_running:
                logger.info(f"Reconnecting {security_id} in {retry_delay:.1f} seconds...")
                await asyncio.sleep(retry_delay)
    
    async def start(self):
        """Start maintaining connections to all ETFs"""
        self.is_running = True
        
        logger.info(f"Starting connections to {len(self.monitor.selected_etfs)} ETFs...")
        
        # Create connection tasks for all ETFs
        tasks = []
        for etf_id in self.monitor.selected_etfs:
            task = asyncio.create_task(self.maintain_connection(etf_id))
            tasks.append(task)
            self.connection_tasks[etf_id] = task
            
            # Small delay to avoid overwhelming the server
            await asyncio.sleep(0.1)
        
        # Wait for all tasks
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
    
    async def stop(self):
        """Stop all connections"""
        self.is_running = False
        
        logger.info("Stopping all connections...")
        
        # Cancel all tasks
        for task in self.connection_tasks.values():
            task.cancel()
        
        # Wait for cancellation
        if self.connection_tasks:
            await asyncio.gather(*self.connection_tasks.values(), return_exceptions=True)


def parse_market_depth_message(message: bytes):
    """Parse the binary market depth message from DhanHQ"""
    if len(message) < 12:
        return None
    
    try:
        header = message[:12]
        msg_length, feed_code, exchange_seg, security_id, num_rows = struct.unpack('<HBBII', header)
        
        if len(message) < msg_length:
            return None
        
        data = message[12:msg_length]
        row_size = 16
        total_expected_bytes = num_rows * row_size
        
        if len(data) < total_expected_bytes:
            return None
        
        bids = []
        asks = []
        
        bids = [None] * min(100, num_rows)
        ask_count = max(0, num_rows - 100)
        asks = [None] * ask_count
        
        for i in range(num_rows):
            start = i * row_size
            end = start + row_size
            row_data = data[start:end]
            
            price_raw, quantity, orders, flags = struct.unpack('<qihh', row_data)
            price = price_raw / 100.0
            
            if i < 100:
                bids[i] = {
                    'level': i + 1,
                    'price': price,
                    'quantity': quantity,
                    'orders': orders,
                    'flags': flags
                }
            else:
                asks[i - 100] = {
                    'level': i - 100 + 1,
                    'price': price,
                    'quantity': quantity,
                    'orders': orders,
                    'flags': flags
                }
        
        bids = [b for b in bids if b is not None]
        asks = [a for a in asks if a is not None]
        
        return {
            'header': {
                'message_length': msg_length,
                'feed_code': feed_code,
                'exchange_segment': exchange_seg,
                'security_id': str(security_id),
                'num_rows': num_rows
            },
            'market_depth': {
                'bids': bids,
                'asks': asks
            }
        }
        
    except Exception as e:
        logger.error(f"Parse error: {e}")
        return None


async def main():
    """Main function"""
    TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY5MDUyNzI3LCJpYXQiOjE3Njg5NjYzMjcsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.YDC_iXbY2nx0SrfHnxdTB8NoWEL-Ff6eTMQ4-Evhfl4-H5VUrvJKPhVk7neyxkNcZhXQ0XyBcGqLBln11c8M9g'
    CLIENT_ID = '1108703565'
    
    print("\n" + "="*80)
    print("ETF BPI PAPER TRADING MONITOR")
    print("="*80)
    print("• Monitors 10 ETFs in real-time")
    print("• Paper trade at 09:07:05 AM (highest BPI)")
    print("• Auto-exit at 1% profit")
    print("• Trades saved to text files")
    print("• Depth data saved to CSV files")
    print("\nInitializing...")
    
    # Create monitor and dashboard
    monitor = BPIETFMonitor()
    dashboard = CleanDashboard(monitor)
    
    # Create WebSocket manager
    ws_manager = WebSocketManager(TOKEN, CLIENT_ID, monitor)
    
    try:
        # Run dashboard and WebSocket connections concurrently
        dashboard_task = asyncio.create_task(dashboard.run_dashboard())
        
        # Wait a moment for dashboard to initialize
        await asyncio.sleep(1)
        
        # Start WebSocket connections
        print("\nStarting WebSocket connections...")
        await ws_manager.start()
        
    except KeyboardInterrupt:
        print("\n\nStopped by user")
    except Exception as e:
        logger.error(f"Main error: {e}")
        print(f"\nError: {e}")
    finally:
        # Cleanup
        print("\n\nCleaning up...")
        dashboard.stop()
        await ws_manager.stop()
        monitor.cleanup()
        
        # Clear screen and show final report
        os.system('cls' if os.name == 'nt' else 'clear')
        
        # Print final statistics
        stats = monitor.get_statistics()
        print("\n" + "="*80)
        print("FINAL TRADING REPORT:")
        print("="*80)
        print(f"Total Runtime: {stats['runtime_seconds']:.0f}s")
        print(f"Total Messages: {stats['total_messages']:,}")
        print(f"Average Msg/sec: {stats['messages_per_second']:.1f}")
        print(f"Paper Trades Today: {len(monitor.order_history)}")
        
        if monitor.order_history:
            total_pnl = sum(order.profit_loss for order in monitor.order_history if order.profit_loss)
            print(f"Total P&L: ₹{total_pnl:.2f}")
            print(f"Trades saved to: {monitor.trade_save_path}/")
            
            for i, order in enumerate(monitor.order_history, 1):
                print(f"\nTrade {i}: {order.name}")
                print(f"  Entry: ₹{order.entry_price:.2f}")
                print(f"  Exit: ₹{order.exit_price:.2f}")
                print(f"  P&L: ₹{order.profit_loss:.2f} ({order.profit_loss_percent:.2f}%)")
        
        print(f"\nDepth data saved to: {monitor.csv_save_path}/")
        print("CSV files contain bid/ask prices, quantities, orders up to 200 levels")
        print("="*80)


if __name__ == "__main__":
    try:
        import websockets
    except ImportError:
        print("Installing required packages...")
        import subprocess
        subprocess.check_call(["pip", "install", "websockets"])
    
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nApplication stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\nError: {e}")
