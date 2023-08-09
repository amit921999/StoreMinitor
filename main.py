import pymysql
from flask import Flask, jsonify, request, send_file
import uuid
import pandas as pd
from datetime import datetime, timedelta
import pytz

app = Flask(__name__)

# Database configurations
user = 'root'
password = 'password'
host = '127.0.0.1'
port = 3306
database = 'storemon'

# Connect to the database
db = pymysql.connect(
    user=user,
    password=password,
    host=host,
    port=port,
    database=database
)

# Create a cursor to execute queries
cursor = db.cursor()

# Dictionary to store the report status and data
reports = {}


@app.route('/trigger_report')
def trigger_report():
    # Generate a unique report_id
    report_id = str(uuid.uuid4())

    # Set the report status to "Running"
    reports[report_id] = {"status": "Running"}

    # Calculate the uptime and downtime for all stores
    calculate_uptime_downtime(report_id)

    # Return the report_id to the user
    return jsonify({"report_id": report_id})


@app.route('/get_report')
def get_report():
    # Get the report_id from the request parameters
    report_id = request.args.get('report_id')

    # Check if the report_id is valid
    if report_id not in reports:
        return jsonify({"error": "Invalid report_id"})

    # Check the status of the report
    if reports[report_id]["status"] == "Running":
        return jsonify({"status": "Running"})
    else:
        # Create a DataFrame with the report data
        df = pd.DataFrame(reports[report_id]["data"])

        # Create a CSV file with the report data
        filename = f"{report_id}.csv"
        df.to_csv(filename, index=False)

        # Return the CSV file to the user
        return send_file(filename, as_attachment=True)


def calculate_uptime_downtime(report_id):
    # Query to get all store_ids from the store_status table
    query = "SELECT DISTINCT store_id FROM store_status"
    cursor.execute(query)
    store_ids = [row[0] for row in cursor.fetchall()]

    # Dataframe to store the report data
    df = pd.DataFrame(
        columns=["store_id", "uptime_last_hour", "uptime_last_day", "uptime_last_week", "downtime_last_hour",
                 "downtime_last_day", "downtime_last_week"])

    for store_id in store_ids:
        # Get the timezone of the store
        query = f"SELECT timezone_str FROM timezones WHERE store_id={store_id}"
        cursor.execute(query)
        result = cursor.fetchone()
        timezone_str = result[
            0] if result == 'Asia/Beirut' or result == 'America/Boise' or result == 'America/Denver' or result == 'America/Phoenix' or result == 'America/New_York' or result == 'America/Los_Angeles' else "America/Chicago"
        timezone = pytz.timezone(timezone_str)

        # Get the current time in UTC and convert it to the local time of the store
        now_utc = datetime.utcnow().replace(tzinfo=pytz.utc)
        now_local = now_utc.astimezone(timezone)

        # Calculate the start and end times for the last hour, day, and week in local time
        last_hour_start_local = now_local - timedelta(hours=1)
        last_day_start_local = now_local - timedelta(days=1)
        last_week_start_local = now_local - timedelta(weeks=1)

        # Convert the start and end times to UTC
        last_hour_start_utc = last_hour_start_local.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
        last_day_start_utc = last_day_start_local.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')
        last_week_start_utc = last_week_start_local.astimezone(pytz.utc).strftime('%Y-%m-%d %H:%M:%S')

        # Query to get the store status data for the last hour, day, and week in UTC time
        query = f"SELECT timestamp_utc, status FROM store_status WHERE store_id={store_id} AND timestamp_utc <= '{last_week_start_utc}' ORDER BY timestamp_utc"
        cursor.execute(query)
        data = cursor.fetchall()

        # Initialize variables to keep track of uptime and downtime for each time period (hour, day, week)
        uptime_last_hour = 0
        uptime_last_day = 0
        uptime_last_week = 0

        downtime_last_hour = 0
        downtime_last_day = 0
        downtime_last_week = 0

        prev_timestamp_local = None
        prev_status = None

        for row in data:
            timestamp_utc_str, status = row

            # Convert the timestamp from UTC to local time of the store
            # timestamp_utc = datetime.strptime(timestamp_utc_str, '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc)
            if ' ' in timestamp_utc_str:
                timestamp_utc = datetime.strptime(timestamp_utc_str, '%Y-%m-%d %H:%M:%S.%f %Z').replace(tzinfo=pytz.utc)
            else:
                timestamp_utc = datetime.strptime(timestamp_utc_str, '%H:%M:%S').replace(tzinfo=pytz.utc)
            timestamp_local = timestamp_utc.astimezone(timezone)

            # Check if the timestamp is within the business hours of the store
            if is_within_business_hours(store_id, timestamp_local):
                # Calculate the time difference between the current and previous timestamps
                if prev_timestamp_local:
                    time_diff = (timestamp_local - prev_timestamp_local).total_seconds() / 60.0

                    # Update the uptime and downtime for each time period based on the status
                    if prev_status == "active":
                        if timestamp_local >= last_hour_start_local:
                            uptime_last_hour += time_diff
                        if timestamp_local >= last_day_start_local:
                            uptime_last_day += time_diff
                        uptime_last_week += time_diff
                    else:
                        if timestamp_local >= last_hour_start_local:
                            downtime_last_hour += time_diff
                        if timestamp_local >= last_day_start_local:
                            downtime_last_day += time_diff
                        downtime_last_week += time_diff

                prev_timestamp_local = timestamp_local
                prev_status = status

        # Add a row to the report data with the calculated uptime and downtime for the store
        row = pd.DataFrame({
            "store_id": [store_id],
            "uptime_last_hour": [round(uptime_last_hour)],
            "uptime_last_day": [round(uptime_last_day / 60.0)],
            "uptime_last_week": [round(uptime_last_week / 60.0)],
            "downtime_last_hour": [round(downtime_last_hour)],
            "downtime_last_day": [round(downtime_last_day / 60.0)],
            "downtime_last_week": [round(downtime_last_week / 60.0)]
        })
        df = pd.concat([df, row], ignore_index=True)

    # Update the report status and data
    reports[report_id]["status"] = "Complete"
    reports[report_id]["data"] = df


def is_within_business_hours(store_id, timestamp):
    # Get the day of the week (0=Monday, 6=Sunday)
    day = timestamp.weekday()

    # Query to get the business hours of the store for the given day
    query = f"SELECT start_time_local, end_time_local FROM business_hours WHERE store_id={store_id} AND day={day}"
    cursor.execute(query)
    result = cursor.fetchone()

    # If data is missing for the store, assume it is open 24*7
    if not result:
        return True

    start_time_str, end_time_str = result

    # Convert the start and end times to datetime objects
    start_time = datetime.strptime(start_time_str, '%H:%M:%S').time()
    end_time = datetime.strptime(end_time_str, '%H:%M:%S').time()

    # Check if the timestamp is within the business hours of the store
    return start_time <= timestamp.time() <= end_time


if __name__ == '__main__':
    app.run()
