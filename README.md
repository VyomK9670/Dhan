!pip install dhanhq==2.2.0rc1

# # For Jupyter/Notebook/IPython
# import asyncio
# import nest_asyncio

# # Apply patch for Jupyter
# nest_asyncio.apply()

# # Now you can run async code directly
# market_feed = MarketFeed()
# result = await market_feed.connect()  # Direct await works in Jupyter
# print(result)


from dhanhq import DhanContext, MarketFeed

# Define and use your dhan_context if you haven't already done so like below:
dhan_context = DhanContext("1108703565",
                           "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzY5OTkwMjAzLCJpYXQiOjE3Njk5MDM4MDMsInRva2VuQ29uc3VtZXJUeXBlIjoiU0VMRiIsIndlYmhvb2tVcmwiOiIiLCJkaGFuQ2xpZW50SWQiOiIxMTA4NzAzNTY1In0.R31oouNalu9EfBrvV71Wwif5IXpRmua6H2raw4_PAjtHe2spP-LIdmy0TDCy_FvhrD96hOWqwrhudB_3p1kMZA")

# Structure for subscribing is (exchange_segment, "security_id", subscription_type)

instruments = [(MarketFeed.NSE, "21401", MarketFeed.Ticker),   # Ticker - Ticker Data
                (MarketFeed.NSE, "21401", MarketFeed.Quote),     # Quote - Quote Data
                (MarketFeed.NSE, "21401", MarketFeed.Full),      # Full - Full Packet
                (MarketFeed.NSE, "21428", MarketFeed.Ticker),
                (MarketFeed.NSE, "21428", MarketFeed.Full),
                (MarketFeed.NSE, "1333", MarketFeed.Ticker),
                (MarketFeed.NSE, "1333", MarketFeed.Full),
                (MarketFeed.NSE, "1313", MarketFeed.Ticker),
                (MarketFeed.NSE, "1313", MarketFeed.Full)]

version = "v2"          # Mention Version and set to latest version 'v2'

# In case subscription_type is left as blank, by default Ticker mode will be subscribed.

import time
from IPython.display import clear_output

try:
    data = MarketFeed(dhan_context, instruments, version)
    
    # Initialize stock data cache
    stock_data_cache = {
        21401.0: {'bq': 176651418.0, 'bp': 14.75, 'sq': 4553217.0, 'sp': 14.83, 'bpi': 97.42},
        21428.0: {'bq': 0.0, 'bp': 0.00, 'sq': 1065173592630.0, 'sp': 27.52, 'bpi': 0.00}
    }
    
    # Track last update times for each stock
    last_update_times = {
        21401.0: 0,
        21428.0: 0
    }
    
    # Initial display
    print("\n" + "=" * 100)
    print(f"{'stock_Code':<12} | {'Buying_Quantity':<20} | {'Buying_Price':<15} | {'Selling_Quantity':<20} | {'Selling_Price':<15} | {'BPI':<10}")
    print("-" * 100)
    
    while True:
        clear_output(wait=True)  # Clear previous output
        current_time = time.time()
        
        # Run market data collection
        data.run_forever()
        response = data.get_data()
        
        if response:
            stock_code = float(response.get('security_id', 0))
            depth_data = response.get('depth', [])
            last_price = float(response.get('LTP', 0))
            
            # Check if it's time to update this stock (every 0.5 seconds)
            if stock_code in [21401.0, 21428.0] and depth_data and current_time - last_update_times[stock_code] >= 0.5:
                
                # Calculate new values (your existing calculation logic)
                lower = last_price * 0.98
                upper = last_price * 1.02
                
                bids = []
                asks = []
                
                for level in depth_data:
                    # Bids
                    bp = float(level.get('bid_price', 0))
                    if lower <= bp <= upper:
                        bq = int(level.get('bid_quantity', 0))
                        bo = int(level.get('bid_orders', 0))
                        bids.append({'price': bp, 'val': bq * bo})
                    
                    # Asks
                    ap = float(level.get('ask_price', 0))
                    if lower <= ap <= upper:
                        aq = int(level.get('ask_quantity', 0))
                        ao = int(level.get('ask_orders', 0))
                        asks.append({'price': ap, 'val': aq * ao})
                
                # Update values
                buy_qty = float(sum(b['val'] for b in bids)) if bids else 0.0
                buy_price = max(bids, key=lambda x: x['val'])['price'] if bids else 0.0
                sell_qty = float(sum(a['val'] for a in asks)) if asks else 0.0
                sell_price = max(asks, key=lambda x: x['val'])['price'] if asks else 0.0
                bpi = (1 - (sell_qty / buy_qty)) * 100 if buy_qty > 0 else 0.0
                
                # Update cache
                stock_data_cache[stock_code] = {
                    'bq': buy_qty, 'bp': buy_price, 
                    'sq': sell_qty, 'sp': sell_price, 
                    'bpi': bpi
                }
                
                # Update timestamp
                last_update_times[stock_code] = current_time
        
        # Redisplay the table with cached data
        print("\n" + "=" * 100)
        print(f"{'stock_Code':<12} | {'Buying_Quantity':<20} | {'Buying_Price':<15} | {'Selling_Quantity':<20} | {'Selling_Price':<15} | {'BPI':<10}")
        print("-" * 100)
        
        # Display all rows from cache (maintaining order)
        print(f"{21401.0:>12.1f} | {stock_data_cache[21401.0]['bq']:>20.1f} | {stock_data_cache[21401.0]['bp']:>15.2f} | "
              f"{stock_data_cache[21401.0]['sq']:>20.1f} | {stock_data_cache[21401.0]['sp']:>15.2f} | {stock_data_cache[21401.0]['bpi']:>10.2f}")
        
        print(f"{21428.0:>12.1f} | {stock_data_cache[21428.0]['bq']:>20.1f} | {stock_data_cache[21428.0]['bp']:>15.2f} | "
              f"{stock_data_cache[21428.0]['sq']:>20.1f} | {stock_data_cache[21428.0]['sp']:>15.2f} | {stock_data_cache[21428.0]['bpi']:>10.2f}")
        
        print("-" * 100)
        
        # Show next update times (optional - from first method)
        print("\n" + "-" * 100)
        for stock_code in [21401.0, 21428.0]:
            time_since_update = current_time - last_update_times[stock_code]
            next_update_in = max(0, 0.5 - time_since_update)
            print(f"Stock {stock_code}: Updated {time_since_update:.2f}s ago | Next update in {next_update_in:.2f}s")
        
        # Small delay to prevent high CPU usage
        time.sleep(0.1)
        
except KeyboardInterrupt:
    print("\n\nProgram terminated.")
except Exception as e:
    print(f"\nError: {e}")




