from datetime import datetime
import calendar

# Input date string
input_date_str = '2024-05-06'

# Convert the string to a datetime object
date_obj = datetime.strptime(input_date_str, '%Y-%m-%d')

# Get the first date of the month
first_day = date_obj.replace(day=1)

# Get the last day of the month
last_day = date_obj.replace(day=calendar.monthrange(date_obj.year, date_obj.month)[1])

# Print the results in the desired format
print(f"First day of the month: {first_day.strftime('%Y-%m-%d')}")
print(f"Last day of the month: {last_day.strftime('%Y-%m-%d')}")
