import pandas as pd
import glob
import xml.etree.ElementTree as ET 
from datetime import datetime 

log_file = "log_file.txt" 
target_file = "transformed_data.csv" 

def extract():
    data = pd.DataFrame(columns=['name','height','weight'])
    for fileCSV in glob.glob('*.csv'):
        data = pd.concat([data, pd.DataFrame(extract_from_csv(fileCSV))], ignore_index = True)
    for fileJson in glob.glob('*.json'):
        data = pd.concat([data, pd.DataFrame(extract_from_json(fileJson))], ignore_index = True)
    for filexml in glob.glob('*.xml'):
        data = pd.concat([data, pd.DataFrame(extract_from_xml(filexml))], ignore_index = True)
    return data

def extract_from_csv(file):
    dataframe = pd.read_csv(file)
    return dataframe
def extract_from_json(file):
    dataframe = pd.read_json(file, lines=True)
    return dataframe
def extract_from_xml(file):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file)
    root = tree.getroot()
    for person in root:
        name = person.find('name').text
        height = float(person.find('height').text)
        weight = float(person.find('weight').text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{'name':name, 'height':height, 'weight':weight}])], ignore_index=True)
    return dataframe

def transform(data): 
    '''Convert inches to meters and round off to two decimals 
    1 inch is 0.0254 meters '''
    data['height'] = round(data.height * 0.0254,2) 
 
    '''Convert pounds to kilograms and round off to two decimals 
    1 pound is 0.45359237 kilograms '''
    data['weight'] = round(data.weight * 0.45359237,2) 
    
    return data 


def load(targetfile, data_to_load):
    data_to_load.to_csv(targetfile)

def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ', ' + message + '\n')

# Log the initialization of the ETL process 
log_progress("ETL Job Started") 
 
# Log the beginning of the Extraction process 
log_progress("Extract phase Started") 
extracted_data = extract() 
 
# Log the completion of the Extraction process 
log_progress("Extract phase Ended") 
 
# Log the beginning of the Transformation process 
log_progress("Transform phase Started") 
transformed_data = transform(extracted_data) 
print("Transformed Data") 
print(transformed_data) 
 
# Log the completion of the Transformation process 
log_progress("Transform phase Ended") 
 
# Log the beginning of the Loading process 
log_progress("Load phase Started") 
load(target_file,transformed_data) 
 
# Log the completion of the Loading process 
log_progress("Load phase Ended") 
 
# Log the completion of the ETL process 
log_progress("ETL Job Ended") 