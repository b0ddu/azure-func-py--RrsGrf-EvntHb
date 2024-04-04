
import logging
import azure.functions as func
# Import Azure Resource Graph library
import azure.mgmt.resourcegraph as arg
# Import specific methods and models from other libraries
from azure.mgmt.resource import SubscriptionClient
from azure.identity import DefaultAzureCredential

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
    # Get your credentials from environment CLI
    credential = DefaultAzureCredential()
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

    # Show Python object
    print(argResults)

getresources("Resources | project id,tags,env='DEV' | limit 10")

