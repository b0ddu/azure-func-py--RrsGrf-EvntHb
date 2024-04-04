
import logging
import json
import azure.functions as func
# Import Azure Resource Graph library
import azure.mgmt.resourcegraph as arg
# Import specific methods and models from other libraries
from azure.mgmt.resource import SubscriptionClient
from azure.identity import DefaultAzureCredential
# Import methods for EventHub Producer
import asyncio
from azure.eventhub import EventHubProducerClient, EventData

# Get your credentials from environment CLI
credential = DefaultAzureCredential()

# Configured EventHub Values
EVENT_HUB_FULLY_QUALIFIED_NAMESPACE = "xxxxx"
EVENT_HUB_NAME = "first-eh-hub"

# Code for Timer Function 
app = func.FunctionApp()

@app.function_name(name="mytimer")
@app.timer_trigger(schedule="0 0 */6 * * *", 
              arg_name="mytimer",
              run_on_startup=True) 
def test_function(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info('Python timer trigger function ran at %s', utc_timestamp
    )
print('Executing Azure Resource Query')

# function to process Resource Query
def getresources( strQuery ):
    subsClient = SubscriptionClient(credential)
    subsRaw = []
    for sub in subsClient.subscriptions.list():
        subsRaw.append(sub.as_dict())
        print(sub)
    subsList = []
    for sub in subsRaw:
        subsList.append(sub.get('subscription_id'))

    # Create Azure Resource Graph client and set options
    argClient = arg.ResourceGraphClient(credential)
    argQueryOptions = arg.models.QueryRequestOptions(result_format="objectArray")

    # Create query
    argQuery = arg.models.QueryRequest(subscriptions=subsList, query=strQuery, options=argQueryOptions)

    # Run query
    argResults = argClient.resources(argQuery)

    return(argResults)

def send_events_to_eventhub(passedResults):
    # Create an Event Hub producer
    producer = EventHubProducerClient.from_connection_string(conn_str=EVENT_HUB_FULLY_QUALIFIED_NAMESPACE, eventhub_name=EVENT_HUB_NAME)

    # Create an event data batch
    event_data_batch = producer.create_batch()
    print("create batch done")
    event_data_batch.add(EventData(passedResults))

    print('Sending Data to EventHub')

    # Send the batch to the event hub
    producer.send_batch(event_data_batch)

    # Close the producer
    producer.close()

# Actual Resource Graph that's passed to above method.
queryOutput=getresources("Resources | project id,tags,env='DEV' | limit 3")
    
# Show Python object Array
print("Array Result : ", queryOutput)

# Show Python object
print("\n JSON Dumps: ", json.dumps(queryOutput))

# Send Resource Graph results to Event Hub 
# send_events_to_eventhub(queryOutput)
