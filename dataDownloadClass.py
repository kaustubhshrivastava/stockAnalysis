import yfinance as yf
import numpy as np
import pandas as pd
import datetime
import pickle
from dateutil import parser

class dataIO:
    def __init__(self):
        self.fileName = "stockData.pickle"
        pass

    def get_single_ticker_info(self,str_ticker,str_period):
        tickerName = yf.Ticker(str_ticker)
        tickerHistory = tickerName.history(period=str_period)
        return tickerHistory

    def prepare_time_sequence(self,sequence, n_steps, n_features):
        X, y = [], []
        for i in range(0, len(sequence)-n_steps):
            X.append(sequence[i:i+n_steps].values)
            y.append(sequence[i+n_steps])
        np_array_X = np.array(X)
        return np_array_X.reshape(np_array_X.shape[0],np_array_X.shape[1],n_features), np.array(y)

    def downloadData_optimized(self, startDate='2015-01-01', endDate='2022-08-18'):
        full_data_set = self.retrieveData(mode = 0)
        #writing logic to avoid downloading downloaded data
        start_date_date = parser.parse(startDate)
        end_date_date = parser.parse(endDate)
        existing_data_start_date = full_data_set.Close.axes[0][0].to_pydatetime()
        existing_data_end_date =full_data_set.Close.axes[0][-1].to_pydatetime()
        if(start_date_date <= existing_data_end_date):
            start_date_date = existing_data_end_date + datetime.timedelta(days = 1)
            startDate = start_date_date.strftime('%Y-%m-%d')
        if(end_date_date >= existing_data_start_date):
            end_date_date = existing_data_start_date + datetime.timedelta(days = -1)
            endDate = end_date_date.strftime('%Y-%m-%d')
        
        print("Download Data from : " + startDate + " to: " + endDate)
        if(start_date_date < end_date_date):
            stockMarketName = pd.read_csv("stockMarketName.csv")
            list_stock_name = list(stockMarketName["CompanySymbol"])
            all_stock_df = yf.download(list_stock_name, 
                        start=startDate, 
                        end=endDate, 
                        progress=False,
                        )
            all_stock_df = full_data_set.append(all_stock_df)
            try:
                with open(self.fileName,"wb") as f:
                    pickle.dump(all_stock_df,f,protocol=pickle.HIGHEST_PROTOCOL)
            except Exception as ex:
                print("Issues with saving pickle. Possible error: ", ex)
        else:
            print('Start date is after end date')
    
    def downloadData(self, startDate='2015-01-01', endDate='2023-07-14'):
        stockMarketName = pd.read_csv("stockMarketName.csv")
        list_stock_name = list(stockMarketName["CompanySymbol"])
        all_stock_df = yf.download(list_stock_name, 
                    start=startDate, 
                    end=endDate, 
                    progress=False,
                    )
        try:
            with open(self.fileName,"wb") as f:
                pickle.dump(all_stock_df,f,protocol=pickle.HIGHEST_PROTOCOL)
        except Exception as ex:
            print("Issues with saving pickle. Possible error: ", ex)

    def retrieveData(self, mode = 1):
        try:
            with open(self.fileName,"rb") as f:
                full_data_set =  pickle.load(f)
                if mode == 0:
                    return full_data_set
                elif mode == 1:
                    return full_data_set.Close
                elif mode == 2:
                    return full_data_set.High
                elif mode == 3:
                    return full_data_set.Low
                elif mode ==4:
                    return full_data_set.Open
                elif mode == 5:
                    return full_data_set.Volume                
        except Exception as ex:
            print("Error during unpickling object: ", ex)

        

