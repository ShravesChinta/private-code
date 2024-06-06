from datetime import datetime
import calendar

def get_first_and_last_day_of_month(date_str):
    # Convert the string to a datetime object
    date_obj = datetime.strptime(date_str, '%Y-%m-%d')
    
    # Get the first date of the month
    first_day = date_obj.replace(day=1)
    
    # Get the last day of the month
    last_day = date_obj.replace(day=calendar.monthrange(date_obj.year, date_obj.month)[1])
    
    # Return the results as formatted strings
    return first_day.strftime('%Y-%m-%d'), last_day.strftime('%Y-%m-%d')

# Example usage
input_date_str = '2024-05-06'
first_day_str, last_day_str = get_first_and_last_day_of_month(input_date_str)

print(f"First day of the month: {first_day_str}")
print(f"Last day of the month: {last_day_str}")
