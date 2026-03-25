# Import the gmtime and strftime functions from the time module
from time import strftime

# Import the time module
import time

# Print the current time in GMT (Greenwich Mean Time) format
print("\nGMT: " + time.strftime("%a, %d %b %Y %I:%M:%S %p %Z", time.gmtime()))

# Print the current time in local time zone format
# The strftime function is used directly since it was imported earlier
print("Local: " + strftime("%a, %d %b %Y %I:%M:%S %p %Z\n"))
