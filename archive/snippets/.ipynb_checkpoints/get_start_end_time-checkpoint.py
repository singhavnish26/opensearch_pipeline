from datetime import datetime, timedelta
import calendar

def generate_time_data(month: str = None, year: int = None, start_hour: int = 18, start_minute: int = 29):
    # Use current year and month if not provided
    now = datetime.now()
    if year is None:
        year = now.year
    if month is None:
        month = now.strftime('%B')
    
    month = month.capitalize()
    cur_start_date = datetime(year, list(calendar.month_name).index(month), 
                              calendar.monthrange(year, list(calendar.month_name).index(month))[1],
                              start_hour, start_minute)
    
    # Define current and previous times
    start_time_cur = cur_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_cur = (cur_start_date + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Adjust to the previous month
    prev_start_date = cur_start_date - timedelta(days=cur_start_date.day)
    start_time_prev = prev_start_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time_prev = (prev_start_date + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    
    # Construct the dictionary
    time_data = {
        "start_time_cur": start_time_cur,
        "to_time_cur": end_time_cur,
        "start_time_prev": start_time_prev,
        "to_time_prev": end_time_prev
        }
    
    return time_data

def main():
    # Ask the user for input
    month_input = input("Enter the month (e.g., January) or press Enter to use the current month: ")
    year_input = input("Enter the year (e.g., 2024) or press Enter to use the current year: ")
    
    # Convert inputs to appropriate types or use defaults
    month = month_input if month_input else None
    year = int(year_input) if year_input else None
    
    # Generate time data with constant start hour and minute
    time_data = generate_time_data(month, year)
    
    # Print the result
    print("Generated time data:")
    print(time_data)

if __name__ == "__main__":
    main()
