import streamlit as st
import pandas as pd
from pytz import timezone

def utc_to_est_bulk(df):      
    # Convert the timestamp column to datetime type
    df['time_stamp'] = pd.to_datetime(df['time_stamp'])
        
    # Convert the timestamp column to Eastern Standard Time (EST)
    df['time_stamp'] = df['time_stamp'].dt.tz_convert('America/New_York')
          
    return df

def bulk_pm25_correction(df): 
    # Calculate pm2.5_corr column
    df['pm2.5_corr'] = (0.524 * df['pm2.5_atm']) - (0.0862 * df['humidity']) + 5.75
    
    return df

def continuous_range_hourly_wifi_bulk(df):
    # Ensure datetime is in EST and timezone-aware
    df['time_stamp'] = pd.to_datetime(df['time_stamp'], utc=True).dt.tz_convert('America/New_York')
    df.set_index('time_stamp', inplace=True)
    # Generate continuous datetime index
    # Generate a continuous datetime index in EST
    full_range = pd.date_range(start=df.index.min(), end=df.index.max(), freq='H', tz='America/New_York')
        
    # Reindex the dataframe with the continuous index
    df_reindexed = df.reindex(full_range)
    df_reindexed_no_duplicates = df_reindexed[~df_reindexed.index.duplicated(keep='first')]
    
    df_reindexed_no_duplicates = df_reindexed_no_duplicates.reset_index().rename(columns={'index': 'time_stamp'})
    # Filter out rows with dates before 2024
    df_reindexed_no_duplicates = df_reindexed_no_duplicates[df_reindexed_no_duplicates['time_stamp'].dt.year > 2023]
    df_reindexed_no_duplicates
    return df_reindexed_no_duplicates

def continuous_range_days_wifi_bulk(df):
    # Set 'time_stamp' as the index
    df.set_index('time_stamp', inplace=True)
    
    # Resample to daily data, aggregating by mean
    daily_df = df.resample('D').mean()
    
    # Generate a full date range and reindex
    full_range = pd.date_range(start=daily_df.index.min(), end=daily_df.index.max(), freq='D')
    daily_df = daily_df.reindex(full_range)
    
    # Remove duplicates if any (should not be necessary with resampling but included for safety)
    daily_df = daily_df[~daily_df.index.duplicated(keep='first')]
    
    # Reset index for saving
    daily_df.reset_index(inplace=True)
    daily_df.rename(columns={'index': 'time_stamp'}, inplace=True)
    return daily_df

# def filter_data_to_specified_date_range(df):
#     # Implement the logic from Filter Data to Specified Date Range Step 5
#     # Placeholder function
#     return df

def main():
    st.title('PurpleAir Wifi Data Processing App')

    uploaded_files = st.file_uploader("Upload your CSV files", type=["csv"], accept_multiple_files=True)
    
    if uploaded_files:
        for uploaded_file in uploaded_files:
            df = pd.read_csv(uploaded_file)
            original_filename = uploaded_file.name.split(".")[0]

           
            # Step 1: UTC to EST Bulk
            df = utc_to_est_bulk(df)
           

            # Step 2: Bulk PM2.5 Correction
            df = bulk_pm25_correction(df)
            

            # Step 3: Continuous Range Hourly Wifi Bulk
            df = continuous_range_hourly_wifi_bulk(df)
            

            # Step 4: Continuous Range Days Wifi Bulk
            df = continuous_range_days_wifi_bulk(df)
            
            # # Step 5: Filter Data to Specified Date Range
            # df = filter_data_to_specified_date_range(df)
            # st.write(f"### After Filtering Data to Specified Date Range for {uploaded_file.name}")
            # st.write(df.head())

            processed_filename = f"{original_filename}_processed_data.csv"
            st.download_button(f"Download Processed Data for {uploaded_file.name}", df.to_csv(index=False), processed_filename)

if __name__ == "__main__":
    main()
