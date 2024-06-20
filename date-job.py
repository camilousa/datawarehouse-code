import json
from datetime import date
from dateutil.rrule import rrule, DAILY
from datetime import datetime, timedelta
import holidays
import boto3

def lambda_handler(event, context):
    
    client = boto3.client("s3", "us-east-1")
    us_holidays = holidays.US()
    start_date = date(2005, 1, 1) #datetime.now()
    end_date = date(2007, 1, 1) #start_date + timedelta(days=365)
    days = ['L', 'M', 'C', 'J', 'V', 'S', 'D']
    i = 0
    
    
    s = f'date_id,rental_date,day_of_week,is_weekend,is_holiday\n'
    for d in rrule(DAILY, dtstart=start_date, until=end_date):
        date_id = d.strftime("%YX%m%d").replace('X0','')
        rental_date = d.strftime("%Y-%m-%d")
        day_of_week = days[d.weekday()]
        is_weekend = int(d.weekday() >= 5)
        is_holiday = int(d in us_holidays)

    
        s += f"{date_id},{rental_date},{day_of_week},{is_weekend},{is_holiday}\n"
    
 
    client.put_object(Body=s, Bucket="datos255423w", Key="dim_date/dates20025_2006.csv")
    # TODO implement
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
