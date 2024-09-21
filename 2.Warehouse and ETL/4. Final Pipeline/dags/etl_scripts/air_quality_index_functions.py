# O3 (8-hour maximum) - European Breakpoints
o3_breakpoints = [
    {'low_conc': 0, 'high_conc': 60, 'low_aqi': 0, 'high_aqi': 50},
    {'low_conc': 61, 'high_conc': 120, 'low_aqi': 51, 'high_aqi': 100},
    {'low_conc': 121, 'high_conc': 180, 'low_aqi': 101, 'high_aqi': 150},
    {'low_conc': 181, 'high_conc': 240, 'low_aqi': 151, 'high_aqi': 200},
    {'low_conc': 241, 'high_conc': 300, 'low_aqi': 201, 'high_aqi': 300},
    # Add more breakpoints as needed
]

# PM10 (24-hour) - European Breakpoints
pm10_breakpoints = [
    {'low_conc': 0, 'high_conc': 20, 'low_aqi': 0, 'high_aqi': 50},
    {'low_conc': 21, 'high_conc': 35, 'low_aqi': 51, 'high_aqi': 100},
    {'low_conc': 36, 'high_conc': 50, 'low_aqi': 101, 'high_aqi': 150},
    {'low_conc': 51, 'high_conc': 100, 'low_aqi': 151, 'high_aqi': 200},
    # Add more breakpoints as needed
]

# PM2.5 (24-hour) - European Breakpoints
pm25_breakpoints = [
    {'low_conc': 0, 'high_conc': 10, 'low_aqi': 0, 'high_aqi': 50},
    {'low_conc': 11, 'high_conc': 20, 'low_aqi': 51, 'high_aqi': 100},
    {'low_conc': 21, 'high_conc': 25, 'low_aqi': 101, 'high_aqi': 150},
    {'low_conc': 26, 'high_conc': 50, 'low_aqi': 151, 'high_aqi': 200},
    # Add more breakpoints as needed
]

# NO2 (1-hour) - European Breakpoints
no2_breakpoints = [
    {'low_conc': 0, 'high_conc': 40, 'low_aqi': 0, 'high_aqi': 50},
    {'low_conc': 41, 'high_conc': 90, 'low_aqi': 51, 'high_aqi': 100},
    {'low_conc': 91, 'high_conc': 120, 'low_aqi': 101, 'high_aqi': 150},
    {'low_conc': 121, 'high_conc': 230, 'low_aqi': 151, 'high_aqi': 200},
    # Add more breakpoints as needed
]

# SO2 (1-hour) - European Breakpoints
so2_breakpoints = [
    {'low_conc': 0, 'high_conc': 100, 'low_aqi': 0, 'high_aqi': 50},
    {'low_conc': 101, 'high_conc': 200, 'low_aqi': 51, 'high_aqi': 100},
    {'low_conc': 201, 'high_conc': 350, 'low_aqi': 101, 'high_aqi': 150},
    {'low_conc': 351, 'high_conc': 500, 'low_aqi': 151, 'high_aqi': 200},
    # Add more breakpoints as needed
]

def calculate_aqi(concentration, breakpoints):
    for bp in breakpoints:
        if bp['low_conc'] <= concentration <= bp['high_conc']:
            aqi = ((bp['high_aqi'] - bp['low_aqi']) / (bp['high_conc'] - bp['low_conc'])) * (concentration - bp['low_conc']) + bp['low_aqi']
            return aqi
    return None  # If concentration is outside defined ranges

def classify_air_quality(aqi):
    if aqi <= 50:
        return 'Good'
    elif 51 <= aqi <= 100:
        return 'Moderate'
    elif 101 <= aqi <= 150:
        return 'Unhealthy for Sensitive Groups'
    elif 151 <= aqi <= 200:
        return 'Unhealthy'
    elif 201 <= aqi <= 300:
        return 'Very Unhealthy'
    else:
        return 'Hazardous'




