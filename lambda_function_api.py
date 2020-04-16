import json
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('StayHomeFinal')



def lambda_handler(event, context):
    bagId = int(event["queryStringParameters"]['bagId'])
    token = event["queryStringParameters"]['token']
    submissiontime = event["queryStringParameters"]['submissiontime']

    if token == "stayhome":
        table.update_item(
        Key={
            'BagId': bagId,
            'SubmissionTime': submissiontime.replace("%20", " ")
        },
        UpdateExpression='SET StatusUpdate = :val1',
        ExpressionAttributeValues={
            ':val1': "DELIVERED"
        }
        )
        return {
            'statusCode': 200,
            'body': json.dumps('BAG ID {} DELIVERED'.format(bagId))
        }
    else:
        return {
            'statusCode': 500,
            'body': json.dumps("Error")
        }

