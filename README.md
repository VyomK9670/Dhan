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
    target_price: Optional[float] = None
    pnl_percent: float = 0.0
    progress_to_target: float = 0.0
    
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
        
        # Update target price and P&L
        if self.current_price and self.target_price:
            self.pnl_percent = ((self.current_price - self.target_price) / self.target_price * 100) if self.target_price > 0 else 0
            if self.target_price > self.current_price:
                self.progress_to_target = ((self.target_price - self.current_price) / self.current_price * 100)
            else:
                self.progress_to_target = 100.0


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
            
            # Update current price and related calculations
            etf.update_current_price()
            
            # Set target price (1% above current for trading simulation)
            if etf.current_price:
                etf.target_price = etf.current_price * 1.01
            
            # Calculate BPI
            self.calculate_etf_bpi(etf)
            
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
                    logger.info(f"📈 Paper order placed for {highest_bpi_etf.name} at {highest_bpi_etf.current_price}")
            
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
        
        logger.info(f"✅ Paper BUY order filled for {etf.name}: {self.quantity_to_trade} shares at ₹{etf.current_price:.2f}")
    
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
            
            logger.info(f"✅ Paper SELL order executed for {self.active_order.name}:")
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
            
            logger.info(f"💾 Trade saved to: {filepath}")
            
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
        """Get formatted display data for single table"""
        with self._lock:
            sorted_etfs = self.get_sorted_etfs()
            stats = self.get_statistics()
            
            # Format ETF data for compact display
            etf_display_data = []
            for etf in sorted_etfs:
                # Calculate progress bar
                progress_percent = min(max(etf.progress_to_target, 0), 100)
                progress_bar = self.get_progress_bar(progress_percent)
                
                # Format BPI signal abbreviation
                signal_abbr = self.get_signal_abbreviation(etf.bpi_signal)
                
                etf_display_data.append({
                    "security_id": etf.security_id,
                    "name": etf.name,
                    "abbreviation": self.get_etf_abbreviation(etf.name),
                    "connection_status": etf.connection_status,
                    "bpi": etf.bpi,
                    "bpi_signal": etf.bpi_signal,
                    "signal_abbr": signal_abbr,
                    "current_price": etf.current_price,
                    "target_price": etf.target_price,
                    "pnl_percent": etf.pnl_percent,
                    "progress_bar": progress_bar,
                    "progress_percent": progress_percent,
                    "update_age": (datetime.now() - etf.last_update).total_seconds(),
                    "update_rate": etf.update_rate,
                    "uptime": etf.uptime
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
            
            return {
                "etfs": etf_display_data,
                "stats": stats,
                "active_order": active_order_data,
                "current_time": datetime.now(),
                "connected_count": stats["connected_count"],
                "total_etfs": len(self.selected_etfs)
            }
    
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
    
    def get_signal_abbreviation(self, signal: str) -> str:
        """Get compact signal abbreviation"""
        abbreviations = {
            "STRONG BUY": "B+",
            "BUY": "B",
            "NEUTRAL": "N",
            "SELL": "S",
            "STRONG SELL": "S+",
            "ERROR": "E"
        }
        return abbreviations.get(signal, "?")
    
    def get_progress_bar(self, percentage: float, width: int = 10) -> str:
        """Get text-based progress bar"""
        filled = int(width * percentage / 100)
        bar = "█" * filled + "░" * (width - filled)
        return bar


class SingleTableDisplay:
    """Single compact table display that updates in-place"""
    
    def __init__(self, monitor: BPIETFMonitor):
        self.monitor = monitor
        self.is_running = False
        self.refresh_rate = 0.5  # Update every 0.5 seconds
        self.initialized = False
        self.table_height = 0
        
        # ANSI color codes
        self.COLORS = {
            'RESET': '\033[0m',
            'BOLD': '\033[1m',
            'RED': '\033[31m',
            'GREEN': '\033[32m',
            'YELLOW': '\033[33m',
            'BLUE': '\033[34m',
            'MAGENTA': '\033[35m',
            'CYAN': '\033[36m',
            'WHITE': '\033[37m',
            'BRIGHT_GREEN': '\033[92m',
            'BRIGHT_RED': '\033[91m',
            'BRIGHT_YELLOW': '\033[93m',
            'BG_BLUE': '\033[44m',
            'BG_GREEN': '\033[42m',
            'BG_YELLOW': '\033[43m',
            'BG_RED': '\033[41m',
            'REVERSE': '\033[7m',
            'DIM': '\033[2m'
        }
        
        # Calculate table height
        self.total_etfs = len(monitor.selected_etfs)
        self.table_height = 5 + self.total_etfs  # Header + ETFs + Footer
    
    def clear_screen(self):
        """Clear screen completely"""
        os.system('cls' if os.name == 'nt' else 'clear')
    
    def move_cursor(self, row: int, col: int = 0):
        """Move cursor to specific position"""
        print(f"\033[{row};{col}H", end="")
    
    def colorize(self, text: str, color: str = 'RESET') -> str:
        """Add color to text"""
        return f"{self.COLORS.get(color, '')}{text}{self.COLORS['RESET']}"
    
    def format_price(self, price: float) -> str:
        """Format price compactly"""
        if price is None:
            return "N/A"
        if price >= 100000:
            return f"₹{price/100000:.1f}L"
        elif price >= 1000:
            return f"₹{price/1000:.1f}K"
        return f"₹{price:.0f}"
    
    def get_connection_icon(self, status: str) -> str:
        """Get connection icon"""
        icons = {
            "CONNECTED": "●",
            "CONNECTING": "○",
            "DISCONNECTED": "✗"
        }
        return icons.get(status, "?")
    
    def get_connection_color(self, status: str) -> str:
        """Get color for connection status"""
        colors = {
            "CONNECTED": "GREEN",
            "CONNECTING": "YELLOW",
            "DISCONNECTED": "RED"
        }
        return colors.get(status, "WHITE")
    
    def get_bpi_color(self, bpi: float) -> str:
        """Get color for BPI value"""
        if bpi is None:
            return "DIM"
        elif bpi == float('inf'):
            return "BRIGHT_GREEN"
        elif bpi > 1.2:
            return "BRIGHT_GREEN"
        elif bpi > 1.05:
            return "GREEN"
        elif bpi > 0.95:
            return "YELLOW"
        elif bpi > 0.8:
            return "RED"
        else:
            return "BRIGHT_RED"
    
    def get_signal_color(self, signal: str) -> str:
        """Get color for signal"""
        colors = {
            "STRONG BUY": "BRIGHT_GREEN",
            "BUY": "GREEN",
            "NEUTRAL": "YELLOW",
            "SELL": "RED",
            "STRONG SELL": "BRIGHT_RED",
            "ERROR": "MAGENTA"
        }
        return colors.get(signal, "WHITE")
    
    def print_table_header(self, display_data: Dict):
        """Print the static table header (only once)"""
        current_time = display_data["current_time"].strftime("%H:%M:%S")
        stats = display_data["stats"]
        
        # Top border with title
        print(f"{self.colorize('┌', 'BLUE')}{self.colorize('─' * 78, 'BLUE')}{self.colorize('┐', 'BLUE')}")
        
        # Title row
        title = f"{self.colorize('│', 'BLUE')} "
        title += f"{self.colorize('📈 ETF BPI MONITOR', 'BOLD')} "
        title += f"{self.colorize('│', 'BLUE')} "
        title += f"{self.colorize('🕒', 'YELLOW')} {current_time} "
        title += f"{self.colorize('│', 'BLUE')} "
        title += f"{self.colorize('📡', 'GREEN')} {display_data['connected_count']}/{display_data['total_etfs']} "
        title += f"{self.colorize('│', 'BLUE')} "
        title += f"{self.colorize('⚡', 'MAGENTA')} {stats['messages_per_second']:.0f}/s"
        title += " " * (78 - len(title) + 30)  # Padding
        title += f"{self.colorize('│', 'BLUE')}"
        print(title)
        
        # Separator
        print(f"{self.colorize('├', 'BLUE')}{self.colorize('─' * 78, 'BLUE')}{self.colorize('┤', 'BLUE')}")
        
        # Column headers
        headers = f"{self.colorize('│', 'BLUE')} "
        headers += f"{self.colorize('#', 'DIM'):<2} "
        headers += f"{self.colorize('ETF', 'DIM'):<5} "
        headers += f"{self.colorize('C', 'DIM'):<2} "
        headers += f"{self.colorize('BPI', 'DIM'):<6} "
        headers += f"{self.colorize('Sig', 'DIM'):<3} "
        headers += f"{self.colorize('Price', 'DIM'):<10} "
        headers += f"{self.colorize('Target', 'DIM'):<10} "
        headers += f"{self.colorize('Progress', 'DIM'):<12} "
        headers += f"{self.colorize('P&L%', 'DIM'):<8}"
        headers += f"{self.colorize('│', 'BLUE')}"
        print(headers)
        
        # Separator
        print(f"{self.colorize('├', 'BLUE')}{self.colorize('─' * 78, 'BLUE')}{self.colorize('┤', 'BLUE')}")
    
    def update_etf_rows(self, display_data: Dict):
        """Update ETF data rows"""
        etfs = display_data["etfs"]
        
        for i, etf in enumerate(etfs):
            row_num = 5 + i  # Starting row for ETF data
            
            # Move cursor to this row
            self.move_cursor(row_num, 0)
            
            # Connection icon and color
            conn_icon = self.get_connection_icon(etf["connection_status"])
            conn_color = self.get_connection_color(etf["connection_status"])
            
            # BPI value and color
            bpi_color = self.get_bpi_color(etf["bpi"])
            if etf["bpi"] is None:
                bpi_str = "N/A"
            elif etf["bpi"] == float('inf'):
                bpi_str = "∞"
            else:
                bpi_str = f"{etf['bpi']:.2f}"
            
            # Signal color
            signal_color = self.get_signal_color(etf["bpi_signal"])
            
            # Price formatting
            price_str = self.format_price(etf["current_price"])
            target_str = self.format_price(etf["target_price"])
            
            # P&L color
            if etf["pnl_percent"] > 0:
                pnl_color = "GREEN"
            elif etf["pnl_percent"] < 0:
                pnl_color = "RED"
            else:
                pnl_color = "YELLOW"
            
            # Construct row
            row = f"{self.colorize('│', 'BLUE')} "
            row += f"{self.colorize(str(i+1), 'CYAN'):<2} "
            row += f"{self.colorize(etf['abbreviation'], 'BOLD'):<5} "
            row += f"{self.colorize(conn_icon, conn_color):<2} "
            row += f"{self.colorize(bpi_str, bpi_color):<6} "
            row += f"{self.colorize(etf['signal_abbr'], signal_color):<3} "
            row += f"{self.colorize(price_str, 'YELLOW'):<10} "
            row += f"{self.colorize(target_str, 'CYAN'):<10} "
            row += f"{etf['progress_bar']:<12} "
            row += f"{self.colorize(f'{etf["pnl_percent"]:+.1f}%', pnl_color):<8}"
            row += f"{self.colorize('│', 'BLUE')}"
            
            print(row, end="")
    
    def print_table_footer(self, display_data: Dict):
        """Print the table footer with trading info"""
        footer_row = self.table_height - 1
        
        # Move to footer position
        self.move_cursor(footer_row, 0)
        
        # Bottom border
        print(f"{self.colorize('└', 'BLUE')}{self.colorize('─' * 78, 'BLUE')}{self.colorize('┘', 'BLUE')}")
        
        # Move to status line (below table)
        self.move_cursor(footer_row + 1, 0)
        
        # Trading status
        status_line = ""
        if display_data["active_order"]:
            order = display_data["active_order"]
            status_line = f"{self.colorize('💰 ACTIVE:', 'BOLD')} "
            status_line += f"{self.colorize(order['name'], 'BRIGHT_GREEN')} "
            status_line += f"{self.colorize('@', 'DIM')} {self.colorize(f'₹{order["entry_price"]:.2f}', 'YELLOW')}"
            
            if "current_pnl_percent" in order:
                pnl_color = "GREEN" if order["current_pnl_percent"] >= 0 else "RED"
                status_line += f" {self.colorize('|', 'DIM')} {self.colorize(f'{order["current_pnl_percent"]:+.2f}%', pnl_color)}"
        
        elif len(self.monitor.order_history) > 0:
            last_order = self.monitor.order_history[-1]
            status_line = f"{self.colorize('📊 LAST:', 'BOLD')} "
            status_line += f"{self.colorize(last_order.name, 'YELLOW')} "
            status_line += f"{self.colorize(f'₹{last_order.profit_loss:+.2f}', 'GREEN' if last_order.profit_loss >= 0 else 'RED')}"
            status_line += f" ({self.colorize(f'{last_order.profit_loss_percent:+.2f}%', 'GREEN' if last_order.profit_loss_percent >= 0 else 'RED')})"
        
        else:
            # Show next trade time
            now = datetime.now()
            target_dt = now.replace(
                hour=self.monitor.target_hour,
                minute=self.monitor.target_minute,
                second=self.monitor.target_second,
                microsecond=0
            )
            
            if now < target_dt:
                time_diff = (target_dt - now).total_seconds()
                status_line = f"{self.colorize('⏳ NEXT TRADE:', 'MAGENTA')} {time_diff:.0f}s"
            else:
                status_line = f"{self.colorize('✅ TRADE TIME:', 'GREEN')} PASSED"
        
        # Add system info
        stats = display_data["stats"]
        status_line += f" {self.colorize('|', 'DIM')} "
        status_line += f"{self.colorize('🔄', 'DIM')} {stats['display_updates']:,} "
        status_line += f"{self.colorize('|', 'DIM')} "
        status_line += f"{self.colorize('⏱️', 'DIM')} {stats['runtime_seconds']:.0f}s "
        status_line += f"{self.colorize('|', 'DIM')} "
        status_line += f"{self.colorize('Ctrl+C', 'YELLOW')} to exit"
        
        print(status_line)
    
    async def run_display(self):
        """Run the single table display"""
        self.is_running = True
        
        # Clear screen initially
        self.clear_screen()
        
        # Hide cursor for cleaner display
        print("\033[?25l", end="")
        
        try:
            while self.is_running:
                # Get display data
                display_data = self.monitor.get_display_data()
                
                if not self.initialized:
                    # Print static header once
                    self.print_table_header(display_data)
                    self.initialized = True
                
                # Update ETF rows
                self.update_etf_rows(display_data)
                
                # Update footer
                self.print_table_footer(display_data)
                
                # Move cursor out of the way (below everything)
                self.move_cursor(self.table_height + 3, 0)
                
                # Flush output
                sys.stdout.flush()
                
                # Wait for next refresh
                await asyncio.sleep(self.refresh_rate)
                
        except KeyboardInterrupt:
            pass
        except Exception as e:
            logger.error(f"Display error: {e}")
        finally:
            # Show cursor again
            print("\033[?25h", end="")
            self.is_running = False
    
    def stop(self):
        """Stop the display"""
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
                
                logger.info(f"✅ Connected to {security_id}")
                
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
                logger.warning(f"⏱️ Connection timeout for {security_id}")
                self.failed_connections += 1
            except Exception as e:
                logger.error(f"❌ Connection error for {security_id}: {e}")
                self.failed_connections += 1
            
            # Update status to disconnected
            self.monitor.update_etf_status(security_id, "DISCONNECTED")
            
            # Exponential backoff with jitter
            retry_delay = min(2 ** min(self.failed_connections, 5), 30)
            jitter = retry_delay * 0.1
            retry_delay += random.uniform(-jitter, jitter)
            
            if self.is_running:
                logger.info(f"🔄 Reconnecting {security_id} in {retry_delay:.1f} seconds...")
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
    TOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY2MDI1MjQzLCJpYXQiOjE3NjU5Mzg4NDMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.sWz5K6POWQYFiqef5SwsFOGpkjl2gnKsNUL2hCBT6dtO3iRLhfwdFn3z0L0kHPdam-xHbJUz4HPleRHopz4N7A'
    CLIENT_ID = '1108703565'
    
    print("\n" + "="*80)
    print("📈 SINGLE TABLE ETF BPI MONITOR")
    print("="*80)
    print("• 10 ETFs in single compact table")
    print("• Real-time updates in-place")
    print("• No screen flickering")
    print("• Paper trading at 09:07:05 AM")
    print("\nInitializing...")
    
    # Create monitor and single table display
    monitor = BPIETFMonitor()
    display = SingleTableDisplay(monitor)
    
    # Create WebSocket manager
    ws_manager = WebSocketManager(TOKEN, CLIENT_ID, monitor)
    
    try:
        # Run display and WebSocket connections concurrently
        display_task = asyncio.create_task(display.run_display())
        
        # Wait a moment for display to initialize
        await asyncio.sleep(1)
        
        # Start WebSocket connections
        print("\nConnecting to WebSocket feeds...")
        await ws_manager.start()
        
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
    except Exception as e:
        logger.error(f"Main error: {e}")
        print(f"\n❌ Error: {e}")
    finally:
        # Cleanup
        print("\n\nCleaning up...")
        display.stop()
        await ws_manager.stop()
        
        # Show final report
        print("\n" + "="*80)
        print("📊 FINAL REPORT:")
        print("="*80)
        
        stats = monitor.get_statistics()
        print(f"Runtime: {stats['runtime_seconds']:.0f}s")
        print(f"Messages: {stats['total_messages']:,}")
        print(f"Updates: {stats['display_updates']:,}")
        print(f"Paper Trades: {len(monitor.order_history)}")
        
        if monitor.order_history:
            total_pnl = sum(order.profit_loss for order in monitor.order_history if order.profit_loss)
            print(f"Total P&L: ₹{total_pnl:.2f}")
            print(f"Trades saved to: {monitor.trade_save_path}/")
        
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
        print("\n\n🛑 Application stopped by user")
    except Exception as e:
        logger.error(f"Application error: {e}")
        print(f"\n❌ Error: {e}")
