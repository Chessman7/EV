import pandas as pd


if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test




@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.


    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.


    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)


    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)


    """
    # Define a reusable function to handle invalid date formats
    def handle_invalid_dates(dates_dim, invalid_dates_column="transaction_date"):
        invalid_dates = dates_dim[dates_dim[invalid_dates_column].isna()][invalid_dates_column].tolist()
        if invalid_dates:
            print(f"Warning: Found invalid dates: {invalid_dates}")
        # Handle invalid dates here (e.g., drop rows, replace with specific value)
        # ... (Add your specific handling logic here)
    def map_station_name(station_name, station_dim):
        return station_dim.loc[station_dim["station_name"] == station_name, "station_id"].values[0]


    stations_dim = df[
    [
        "Station_Name",
        "Address_1",
        "City",
        "State/Province",
        "Postal_Code",
        "Country",
        "Latitude",
        "Longitude",
        "Org_Name",
    ]
    ].copy()
    stations_dim.insert(0, "station_id", range(1, len(stations_dim) + 1))
    stations_dim.rename(columns={"Station_Name": "station_name","State/Province":"state_province"}, inplace=True)
    stations_dim_reset = stations_dim.drop_duplicates(subset="station_name").reset_index(drop=True)


# Ports Dimension Table
    ports_dim = df[
        ["Station_Name", "Port_Type", "Port_Number", "Plug_Type"]
    ].copy()
    ports_dim.insert(0, "port_id", range(1, len(ports_dim) + 1))
    ports_dim["station_id"] = ports_dim["Station_Name"].apply(lambda name: map_station_name(name, stations_dim_reset))
    ports_dim_reset = ports_dim.drop_duplicates(subset="Port_Number").reset_index(drop=True)


# Users Dimension Table
    users_dim = df[["Driver_Postal_Code"]].copy()
    users_dim.insert(0, "user_id", range(1, len(users_dim) + 1))
    users_dim.rename(columns={"Driver_Postal_Code": "driver_postal_code"}, inplace=True)
    users_dim_reset = users_dim.drop_duplicates(subset="driver_postal_code").reset_index(drop=True)


# Dates Dimension Table
    dates_dim = df[
        ["Transaction_Date_(Pacific_Time)", "Start_Time_Zone", "Year", "Month", "Hour", "Minute", "Day_of_Week"]
    ].copy()
    dates_dim.insert(0, "date_id", range(1, len(dates_dim) + 1))
    # Rename the column to comply with BigQuery schema
    dates_dim.rename(columns={"Transaction_Date_(Pacific_Time)": "transaction_date"}, inplace=True)
    dates_dim["transaction_date"] = pd.to_datetime(dates_dim["transaction_date"], format="%m/%d/%Y %H:%M")


    handle_invalid_dates(dates_dim)
    dates_dim_reset = dates_dim.drop_duplicates(subset="transaction_date").reset_index(drop=True)


# Fact Table: Charging Sessions
    charging_sessions_fact = df.copy()
    charging_sessions_fact.insert(0, "session_id", range(1, len(charging_sessions_fact) + 1))
    charging_sessions_fact["station_id"] = charging_sessions_fact["Station_Name"].apply(lambda name: map_station_name(name, stations_dim_reset))
    charging_sessions_fact["port_id"] = ports_dim_reset.set_index("Port_Number")["port_id"]
    charging_sessions_fact["date_id"] = dates_dim_reset.set_index("transaction_date")["date_id"]
    charging_sessions_fact["user_id"] = users_dim_reset.set_index("driver_postal_code")["user_id"]


   
   
    return {"dates_dim":dates_dim_reset.to_dict(orient="dict"),
       "stations_dim":stations_dim_reset.to_dict(orient="dict"),
       "ports_dim":ports_dim_reset.to_dict(orient="dict"),
       "charging_sessions_dim":charging_sessions_fact.to_dict(orient="dict"),
       "users_dim":users_dim_reset.to_dict(orient="dict") }




@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
