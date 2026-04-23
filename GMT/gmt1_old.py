import time
from datetime import timedelta

# Get local time and GMT time as struct_time objects
local_time_struct = time.localtime()
gmt_time_struct = time.gmtime()

# Format them into readable strings
l_time_str = time.strftime("%Y-%m-%d %H:%M:%S %Z", local_time_struct)
g_time_str = time.strftime("%Y-%m-%d %H:%M:%S UTC", gmt_time_struct)

# Calculate the difference (offset in seconds)
# tm_gmtoff is the offset in seconds east of UTC.
offset_seconds = local_time_struct.tm_gmtoff
offset_timedelta = timedelta(seconds=offset_seconds)

print(f"Local time: {l_time_str}")
print(f"GMT time:   {g_time_str}")
print(f"Difference (offset from UTC): {offset_timedelta}")
