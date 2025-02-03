from faker import Faker
import csv
import random
from decimal import Decimal
from datetime import datetime
import time

fake_element = Faker()
no_of_records = 10000

date_now = datetime.now().strftime('%Y%m%d%H%M%S')

def create_csv():
    with open(f'fake_data/user_{date_now}.csv','w',newline='') as file:
        columns = ['customer_id','first_name','last_name','email','street','city','state','country']
        write_csv = csv.DictWriter(file,fieldnames=columns)
        write_csv.writeheader()
        for i in range(no_of_records):
            write_csv.writerow(
            {
              'customer_id' : i,
                'first_name' : fake_element.first_name(),
                'last_name' : fake_element.last_name(),
                'email' : fake_element.email(),
                'street' : fake_element.street_address(),
                'city' : fake_element.city(),
                'state' : fake_element.state(),
                'country' : fake_element.country()
                
            })

if __name__ == '__main__':
    while True:
        create_csv()
        time.sleep(120)
    