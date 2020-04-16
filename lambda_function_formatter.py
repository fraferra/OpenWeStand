import json
import boto3
import os
from bs4 import BeautifulSoup
import re

client = boto3.client("sns")

s3 = boto3.resource('s3')

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('StayHomeOrders')

lambda_client = boto3.client('lambda')

keys = ['Submission Time', 'Full Name', 'Email', 'Phone',
        'Contact First Name', 'Delivery Address',
        'Delivery Address/Delivery Ins', '***Please Select Up To 5 Item',
        '(1) Half Gallon Milk', '(1) Half Gallon Orange Juice',
        '(1) Half Dozen Eggs ', '(1) Bunch of Bananas', '(1) Half Dozen Oranges',
        '(1) Half Dozen Apples', '(1) Bag of Lettuce(Spinach or', 'Oatmeal and Brown Sugar',
        '(1) Jar of Peanut Butter', '(1) Loaf of Bread(White or Wh', '(2) Sticks of Butter',
        '(1) Bag of Rice', '(1) Bottle of Hand Sanitizer ', '(1) Bottle of Hand Soap *Subj',
        '(2) Rolls of Toilet Paper *Su', '(1) Disinfectant Wipes *Subje', '(1) Container Prunes',
        '(1) Package of Depends(*Pleas', '0', 'I accept terms & conditions', 'ID', 'Owner',
        'Created Date', 'Updated Date', '(1) Jar Low Sodium Tomato Sau',
        '(1) Package Whole Grain Pasta', '(1) Bag of Potatoes(Sweet or ',
        '(2) Cans of Beans(Pinto or Bl', '(1) Container of Plain Yogurt',
        '(1) Bunch of Tomatoes', '(2) Cans of Tuna', 'Other(*Specify in notes, and ',
        'copy of (1) Loaf of Bread(White or Wh', 'copy of (2) Sticks of Butter',
        'Choose your pizza topping', '(1) Box Cereal', 'Bottled Water',
        '(1) Bottle Cooking Oil', '(1) Bottle Baby Powder', 'Zip Code', 'Delivery Instructions/Gate Co']

regex_street_address = re.compile('\d{1,4} [\w\s]{1,20}(?:street|st|avenue|ave|road|rd|highway|hwy|square|sq|trail|trl|drive|dr|court|ct|park|parkway|pkwy|circle|cir|boulevard|blvd)\W?(?=\s|$)', re.IGNORECASE)
regex_street_address_2 = re.compile("\d+ [\w\d]+ [\w\d]+", re.IGNORECASE)
regex_phone = re.compile('''((?:(?<![\d-])(?:\+?\d{1,3}[-.\s*]?)?(?:\(?\d{3}\)?[-.\s*]?)?\d{3}[-.\s*]?\d{4}(?![\d-]))|(?:(?<![\d-])(?:(?:\(\+?\d{2}\))|(?:\+?\d{2}))\s*\d{2}\s*\d{3}\s*\d{4}(?![\d-])))''')
regex_email = re.compile("([a-z0-9!#$%&'*+\/=?^_`{|.}~-]+@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)", re.IGNORECASE)
regex_zipcode = re.compile(r'\b\d{5}(?:[-\s]\d{4})?\b')

def get_spans_text(row):
    try:
        return [x.text.replace("\n", "").replace("=", "") for x in row.find_all("span")]
    except:
        return -1

def create_dict(list_of_tables, date):
    result = {}
    result["Submission Time"] = date
    count = 0
    for table in list_of_tables:
        rows = table.find_all("tr")
        list_of_spans = [get_spans_text(row) for row in rows]
        for span in list_of_spans:
            
            previousStateKey = ""
            if len(span) == 0:
                continue
            else:
                for row in span:
                    row = (row
                               .replace("\n", "")
                               .replace("=", "")
                               .replace("</span>", "")
                          )
                    row = row.split(":")
                    if len(row) == 2:
                        if row[1] == "":
                            row = row[0]
                        else:
                            row = row[0]
                    elif len(row) == 1:
                        row = row[0]
                    elif len(row) == 3:
                        result[row[0]] = " ".join(row[1:])
                    else:
                        row = row[0]
                    try:
                        if result.get(row, -1) != -1:
                            continue
                    except:
                        print(row)
                    else:
                        if row in keys:
                            result[row] = -1
                            previousStateKey = row
                        else:
                            result[previousStateKey] = row
                            if row == "Checked" and previousStateKey != "":
                                count +=1
                                print(count, previousStateKey, row)
                            if re.match(regex_phone, row.strip()) and re.match(regex_zipcode, row.strip()) is None:
                                result["Phone"] = row
                            if re.match(regex_email, row.strip()):
                                result["Email"] = row
                            if re.match(regex_zipcode, row.strip()):
                                result["Zip Code"] = row
                            if (re.match(regex_street_address, row.strip())
                               or re.match(regex_street_address_2, row.strip())):
                                result["Delivery Address"] = row
                                
    for key in keys:
        if result.get(key,-1) == -1:
            result[key] = 'NOT_FOUND'
    result["TotalChecked"] = count
    result.pop("")
    return result
                
def cleanup_text(s):
    s = (s.replace("\n","")
             .replace("=","")
             .replace("span>", "")
             .replace("</", "")
           )
    if ":" in s:
        s = s.split(":")[0]
    return s

def format_value(value):
    if value == "Unchecked":
        return False
    elif value == "Checked":
        return True
    else:
        return value

def look_for_keys(text):
    keys = ['Submission Time', 'Full Name', 'Email', 'Phone',
        'Contact First Name', 'Delivery Address',
        'Delivery Address/Delivery Ins', '***Please Select Up To 5 Item',
        '(1) Half Gallon Milk', '(1) Half Gallon Orange Juice',
        '(1) Half Dozen Eggs ', '(1) Bunch of Bananas', '(1) Half Dozen Oranges',
        '(1) Half Dozen Apples', '(1) Bag of Lettuce(Spinach or', 'Oatmeal and Brown Sugar',
        '(1) Jar of Peanut Butter', '(1) Loaf of Bread(White or Wh', '(2) Sticks of Butter',
        '(1) Bag of Rice', '(1) Bottle of Hand Sanitizer ', '(1) Bottle of Hand Soap *Subj',
        '(2) Rolls of Toilet Paper *Su', '(1) Disinfectant Wipes *Subje', '(1) Container Prunes',
        '(1) Package of Depends(*Pleas', '0', 'I accept terms & conditions', 'ID', 'Owner',
        'Created Date', 'Updated Date', '(1) Jar Low Sodium Tomato Sau',
        '(1) Package Whole Grain Pasta', '(1) Bag of Potatoes(Sweet or ',
        '(2) Cans of Beans(Pinto or Bl', '(1) Container of Plain Yogurt',
        '(1) Bunch of Tomatoes', '(2) Cans of Tuna', 'Other(*Specify in notes, and ',
        'copy of (1) Loaf of Bread(White or Wh', 'copy of (2) Sticks of Butter',
        'Choose your pizza topping', '(1) Box Cereal', 'Bottled Water',
        '(1) Bottle Cooking Oil', '(1) Bottle Baby Powder', 'Zip Code', 'Delivery Instructions/Gate Co']
    for key in keys:
        key = (key.replace("*", "\*")
                  .replace("(", "\(")
                  .replace(")", "\)")
              )
        if re.findall(key, text):
            return True
    return False
    
def format_email(filename):
    with open(filename, "r") as f:
        contents = f.read()
    contents = contents.replace("</s=>", "</span>").replace("<s=", "<span")
    soup = BeautifulSoup(contents, 'html5lib')
    date= re.findall("\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d", soup.text)[0]
    tables = []
    for table in soup.find_all('table'):
        if look_for_keys(table.text):
            
            tables.append(table)

    return create_dict(tables, date)

def store_order(order_dict):
    newItem = order_dict
    newItem["EmailPhoneNumberDate"] = "{}_{}_{}".format(order_dict.get("Email",""),
                                                 order_dict.get("Phone",""),
                                                 order_dict.get("Submission Time",""))
    table.put_item(
           Item=newItem
        )
    return newItem

def lambda_handler(event, context):
    res = []
    try:
        for record in event['Records']:
            key = record['s3']['object']['key']
            bucket = record['s3']['bucket']['name'] 

            filename = "/tmp/{}".format(key.replace("/", "_"))
            
            s3.Bucket(bucket).download_file(key, filename)
        
            emailData = format_email(filename)
            res.append(emailData)
            emailData["EmailUrl"] = "https://{}.s3-us-west-2.amazonaws.com/{}".format(bucket, key)
            dynamoItem = store_order(emailData)

                
            response = lambda_client.invoke(
                FunctionName='augmentItem',
                InvocationType='Event',
                Payload=json.dumps(dynamoItem)
            )

            os.remove(filename)
    
        return {
            'statusCode': 200,
            'body': json.dumps('')
        }
    except Exception as e:
        print(e)
        return {
            'statusCode': 500,
            'body': json.dumps("")
        }

