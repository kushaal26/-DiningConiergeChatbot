from collections import defaultdict
import boto3
from requests_aws4auth import AWS4Auth
import requests
import time
from datetime import datetime
from decimal import *
import simplejson as json
import json



def empty_field_check(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')

session = boto3.session.Session()
credentials = session.get_credentials()

region = 'us-east-1'



# define api key, define the endpoint, and define the header
API_KEY = 'ugW0nEjaISqn3jYFFW8YWTRpwndfLY1PT_nnWU9MCuwnW9IgfUI7bOLcyIFJO2VQn9NSIsFMFVJ7vwjqSqQJdywRyCkF3RQ5cPOVoAHalkP1SNhIOWeQJD1yjQEAZHYx'
ENDPOINT = 'https://api.yelp.com/v3/businesses/search'
ENDPOINT_ID = 'https://api.yelp.com/v3/businesses/'  # + {id}
HEADERS = {'Authorization': 'bearer %s' % API_KEY}

# set parameters

#request parameter details from https://docs.developer.yelp.com/reference/v3_business_search
PARAMETERS = {'term': 'food',
              'limit': 50,
              'radius': 15000,
              'offset': 200,
              'location': 'Manhattan'}

cuisines1 = ['italian', 'chinese', 'mexican', 'indian', 'french', 'japanese', 'spanish', 'thai', 'korean']
cuisines2 = ['pakistani', 'german', 'indonesian', 'korean', 'taiwanese', 'south indian', 'swedish', 'latvian', 'american', 'scottish', 'british', 'canadian', 'russian', 'jewish', 'polish', 'german', 'hawaiian', 'brazillian', 'cuban', 'irish', 'turkish', 'kenyan']

cuisines = cuisines1 + cuisines2

# locations_around_manhattan = ['Lower East Side, Manhattan',
#                               'Upper East Side, Manhattan',
#                               'Brooklyn',
#                               'Queens',
#                               'Bronx']

locations_around_manhattan = ['Midtown', 'Wall Street', 'Tribeca']

start_time = time.time()
for location in locations_around_manhattan:
    PARAMETERS['location'] = location
    for cuisine in cuisines:
        PARAMETERS['term'] = cuisine
        response = requests.get(url=ENDPOINT, params=PARAMETERS, headers=HEADERS)
        business_data = response.json()['businesses']
        for business in business_data:
            current_timestamp = datetime.now()
            timestamp_string = current_timestamp.strftime("%d/%m/%Y %H:%M:%S")
            table.put_item(
                Item={
                    'Business_ID': empty_field_check(business['id']),
                    'insertedAtTimestamp': empty_field_check(timestamp_string),
                    'Name': empty_field_check(business['name']),
                    'Cuisine': empty_field_check(cuisine),
                    'Rating': empty_field_check(Decimal(business['rating'])),
                    'Number of Reviews': empty_field_check(Decimal(business['review_count'])),
                    'Address': empty_field_check(business['location']['address1']),
                    'Zip Code': empty_field_check(business['location']['zip_code']),
                    'Latitude': empty_field_check(str(business['coordinates']['latitude'])),
                    'Longitude': empty_field_check(str(business['coordinates']['longitude'])),
                }
            )

    print(f'Finished fetching results for {location} . Time taken: {time.time() - start_time}')
