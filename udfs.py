from pyflink.table import DataTypes
from pyflink.table.udf import udf
import datetime

@udf(input_types=[DataTypes.DOUBLE(), DataTypes.DOUBLE()], result_type=DataTypes.STRING())
def parse_location_from_coords(long, lat):
    try:
        return f"POINT({long} {lat})"
    except Exception as e:
        return None  # Handle any exceptions by returning None



@udf(input_types=[DataTypes.INT()], result_type=DataTypes.STRING())
def format_time_duration(duration_seconds):
    try:
        # Calculate hours, minutes, and seconds
        hours = duration_seconds // 3600
        minutes = (duration_seconds % 3600) // 60
        seconds = duration_seconds % 60
        
        # Format as hh:mm:ssZ
        return f"{hours}:{minutes}:{seconds}"
    except Exception as e:
        return None 
    
