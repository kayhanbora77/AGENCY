from datetime import datetime
import pytz

# Get the current time in the local system's timezone
# (The 'astimezone()' without an argument gets the system's local timezone)
local_time = datetime.now().astimezone()

# Convert the local time object to UTC (GMT)
gmt_time = local_time.astimezone(pytz.utc)

# Calculate the time difference (a timedelta object)
# Note: For timezones, the offset is built into the aware datetime objects.
time_difference = local_time - gmt_time

print(f"Local time (aware): {local_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
print(f"GMT time (aware):   {gmt_time.strftime('%Y-%m-%d %H:%M:%S %Z%z')}")
print(f"Difference (timedelta): {time_difference}")

# You can also get the offset directly from the local timezone info
local_offset = local_time.utcoffset()
print(f"Local Offset from UTC: {local_offset}")
