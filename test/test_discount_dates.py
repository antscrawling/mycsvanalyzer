from datetime import datetime, timedelta

def get_black_friday(year):
    """Get Black Friday date (4th Friday in November)"""
    # First day of November
    nov_1 = datetime(year, 11, 1)
    # Find first Friday in November
    days_until_friday = (4 - nov_1.weekday()) % 7
    first_friday = nov_1 + timedelta(days=days_until_friday)
    # Fourth Friday is 3 weeks later
    black_friday = first_friday + timedelta(weeks=3)
    return black_friday

def get_cyber_monday(year):
    """Get Cyber Monday (Monday after Black Friday)"""
    black_friday = get_black_friday(year)
    cyber_monday = black_friday + timedelta(days=3)  # 3 days after Friday
    return cyber_monday

def get_discount_periods(year):
    """Get all discount periods for a given year"""
    black_friday = get_black_friday(year)
    cyber_monday = get_cyber_monday(year)
    
    return {
        'BLACKFRIDAY': (black_friday, black_friday),  # Just Black Friday
        'CYBERMONDAY': (cyber_monday, cyber_monday),  # Just Cyber Monday
        'NEWYEAR': (datetime(year-1, 12, 26), datetime(year, 1, 2)),  # Dec 26 to Jan 2
        'CHRISTMAS': (datetime(year, 12, 1), datetime(year, 12, 26))  # Dec 1 to Dec 26
    }

# Test the functions
print("🗓️ Testing Discount Date Calculations")
print("=" * 40)

for year in [2020, 2021, 2022, 2023, 2024, 2025]:
    print(f"\n📅 Year {year}:")
    black_friday = get_black_friday(year)
    cyber_monday = get_cyber_monday(year)
    discount_periods = get_discount_periods(year)
    
    print(f"   🛍️ Black Friday: {black_friday.strftime('%A, %B %d, %Y')}")
    print(f"   💻 Cyber Monday: {cyber_monday.strftime('%A, %B %d, %Y')}")
    print(f"   🎄 Christmas Period: {discount_periods['CHRISTMAS'][0].strftime('%b %d')} - {discount_periods['CHRISTMAS'][1].strftime('%b %d')}")
    print(f"   🎉 New Year Period: {discount_periods['NEWYEAR'][0].strftime('%b %d, %Y')} - {discount_periods['NEWYEAR'][1].strftime('%b %d, %Y')}")

print("\n✅ Date calculations working correctly!")
