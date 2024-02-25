from robocorp import workitems
from robocorp.tasks import task
from RPA.HTTP import HTTP
from RPA.JSON import JSON
from RPA.Tables import Tables

http = HTTP()
json = JSON()
table = Tables()

TRAFFIC_JSON_FILE_PATH = "output/traffic.json"

#JSON data keys
COUNTRY_KEY =  "SpatialDim"
YEAR_KEY = "TimeDim"
RATE_KEY = "NumericValue"
GENDER_KEY = "Dim1"

@task
def produce_traffic_data():
    """
    Downloads the raw traffic data.
    Transforms the raw data into business data format.
    Saves the business data as work items to be consumed.
    """
    # Downloads the raw and unfiltered data
    http.download(
        url="https://github.com/robocorp/inhuman-insurance-inc/raw/main/RS_198.json",
        target_file=TRAFFIC_JSON_FILE_PATH,
        overwrite=True
    )
    traffic_data = load_traffic_data_as_table()
    filtered_data = filter_and_sort_traffic_data(traffic_data)
    filtered_data = get_latest_data_by_country(filtered_data)
    payloads = create_work_item_payloads(filtered_data)
    save_work_item_payloads(payloads)

def load_traffic_data_as_table():
    """
    Transforms the json data into a table to enable
    sorting, filtering etc.
    Returns: table created from json data
    """
    json_data = json.load_json_from_file(TRAFFIC_JSON_FILE_PATH)
    return table.create_table(json_data["value"])

def filter_and_sort_traffic_data(traffic_data):
    """Filter and sort the raw data to get relevant data"""
    max_rate = 5.0
    both_genders = "BTSX"
    table.filter_table_by_column(traffic_data, RATE_KEY, "<", max_rate)
    table.filter_table_by_column(traffic_data, GENDER_KEY, "==", both_genders)
    table.sort_table_by_column(traffic_data, YEAR_KEY, False)
    return traffic_data

def get_latest_data_by_country(data):
    """
    Returns the data for latest date in the given group
    """
    data = table.group_table_by_column(data, COUNTRY_KEY)
    latest_data_by_country = []
    for group in data:
        first_row = table.pop_table_row(group)
        latest_data_by_country.append(first_row)
    return latest_data_by_country

def create_work_item_payloads(data):
    payloads = []
    for row in data:
        payload = dict(
            country = row[COUNTRY_KEY],
            year = row[YEAR_KEY],
            rate = row[RATE_KEY]
        )
        payloads.append(payload)
    return payloads

def save_work_item_payloads(payloads):
    for payload in payloads:
        variables = dict(traffic_data=payload)
        workitems.outputs.create(variables)
