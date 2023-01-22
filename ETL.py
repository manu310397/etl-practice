import glob
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime


tempFile = 'dealership.temp'
logFile = 'dealership.txt'
targetFile = 'data.csv'


def extractFromCSV(file):
    data = pd.read_csv(file)

    return data


def extractFromJSON(file):
    data = pd.read_json(file, lines=True)

    return data


def extractFromXML(file):
    data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])
    tree = ET.parse(file)
    root = tree.getroot()

    for car in root:
        car_model = car.find('car_model').text
        year_of_manufacture = car.find('year_of_manufacture').text
        price = float(car.find('price').text)
        fuel = car.find('fuel').text
        data.append({"car_model": car_model, "year_of_manufacture": year_of_manufacture, "price": price, "fuel": fuel}, ignore_index=True)

    return data


def extract():
    extracted_data = pd.DataFrame(columns=["car_model", "year_of_manufacture", "price", "fuel"])

    for csv in glob.glob('datasource/*.csv'):
        extracted_data = extracted_data.append(extractFromCSV(csv), ignore_index=True)

    for json in glob.glob('datasource/*.json'):
        extracted_data = extracted_data.append(extractFromJSON(json), ignore_index=True)

    for xml in glob.glob('datasource/*.xml'):
        extracted_data = extracted_data.append(extractFromXML(xml), ignore_index=True)

    return extracted_data


def transform(data):
    data['price'] = round(data['price'], 2)

    return data


def load(file, data):
    data.to_csv(file)


def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')


log('ETL Job started')

log('Extract phase started')
extracted_data = extract()
log("Extract phase Ended")
print(extracted_data)

log("Transform phase Started")
transformed_data = transform(extracted_data)
log("Transform phase Ended")
print(transformed_data)

log("Load phase Started")
load(targetFile,transformed_data)
log("Load phase Ended")

log("ETL Job Ended")