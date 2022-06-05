# Import required libraries
import datetime
import time
from polygon import RESTClient
from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd
from math import sqrt
from math import isnan
import matplotlib.pyplot as plt
from numpy import mean
from numpy import std
from math import floor

from polygon_forex_aggregate import *




# This main function repeatedly calls the polygon api every 1 seconds for 24 hours
# and stores the results.
def main(currency_pairs):
    print("entering main")
    # The api key given by the professor
    key = ""

    # Number of list iterations - each one should last about 1 second
    count = 0
    agg_count = 0

    # Create an engine to connect to the database; setting echo to false should stop it from logging in std.out
    engine = create_engine("sqlite+pysqlite:///sqlite/final2.db", echo=False, future=True)

    # Create the needed tables in the database
    initialize_raw_data_tables(engine, currency_pairs)
    initialize_aggregated_tables(engine, currency_pairs)

    # Open a RESTClient for making the api calls
    client = RESTClient(key)
    # Loop that runs until the total duration of the program hits 24 hours.
    while count < 100:  # 86400 seconds = 24 hours

        # Make a check to see if 6 minutes has been reached or not
        if agg_count == 10:
            # Aggregate the data and clear the raw data tables
            aggregate_raw_data_tables(engine, currency_pairs)
            reset_raw_data_tables(engine, currency_pairs)
            agg_count = 0

        # Only call the api every 1 second, so wait here for 0.75 seconds, because the
        # code takes about .15 seconds to run
        time.sleep(0.75)

        # Increment the counters
        count += 1
        agg_count += 1
        print(count)
        # Loop through each currency pair
        for currency in currency_pairs:
            print(currency)
            # Set the input variables to the API
            from_ = currency[0]
            to = currency[1]

            # Call the API with the required parameters
            try:
                ##resp = client.forex_currencies_real_time_currency_conversion(from_, to, amount=100, precision=2)
                ###resp = client.get_aggs("AAPL", 1, "day", "2022-04-04", "2022-04-04")
                resp = client.get_real_time_currency_conversion(from_, to, amount=100, precision=2)
            except Exception as e:
                print(f"get error, count = {count} agg_count = {agg_count} error is \n {e}")
                continue

            # This gets the Last Trade object defined in the API Resource
            last_trade = resp.last
            print(last_trade)

            # Format the timestamp from the result
            dt = ts_to_datetime(last_trade.timestamp)

            # Get the current time and format it
            insert_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            print(insert_time)

            # Calculate the price by taking the average of the bid and ask prices
            avg_price = (last_trade.bid + last_trade.ask) / 2

            # Write the data to the SQLite database, raw data tables
            with engine.begin() as conn:
                conn.execute(text(
                    "INSERT INTO " + from_ + to + "_raw(ticktime, fxrate, inserttime) VALUES (:ticktime, :fxrate, :inserttime)"),
                             [{"ticktime": dt, "fxrate": avg_price, "inserttime": insert_time}])
if __name__ == '__main__':
    # A dictionary defining the set of currency pairs we will be pulling data for
    currency_pairs = [["AUD", "USD", [], portfolio("AUD", "USD")],
                      #   ["GBP","EUR",[],portfolio("GBP","EUR")],
                      #   ["USD","CAD",[],portfolio("USD","CAD")],
                      #   ["USD","JPY",[],portfolio("USD","JPY")],
                      #   ["USD","MXN",[],portfolio("USD","MXN")],
                      #   ["EUR","USD",[],portfolio("EUR","USD")],
                      ["USD", "CNY", [], portfolio("USD", "CNY")],
                      #   ["USD","CZK",[],portfolio("USD","CZK")],
                      #   ["USD","PLN",[],portfolio("USD","PLN")],
                      ["USD", "INR", [], portfolio("USD", "INR")]]

    # Run the main data collection loop
    main(currency_pairs)
