#!/usr/bin/env python
__author__ = "Frédèrick Franck en Simon Swolfs"
__email__ = "simon.swolfs@student.kdg.be"
__credits__= "Frédèrick Franck / azure.microsoft.com"

__version__ = "1.3"

#Simon ID =  1287770548
#Frederick ID =  1273923095
#Dieter ID = 1143683527


import time 
import asyncio
import requests 
from azure.eventhub.aio import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblobaio import BlobCheckpointStore
import json 
#import random
URL = "https://api.telegram.org/bot1129774651:AAETvuSM6U2xVU2BMYj3iIcPYCgytpe1omI/"
SEND = 'sendMessage'
GETUPDATE = 'getUpdate'
#PARAMS = {'chat_id':1287770548, 'text': 'test + test'} 
ID_SIMON = 1287770548
ID_DIETER = 1143683527
ID_JEROEN = 878864710 
OFFSET = 0
LATEST_TEMP = "There is no more content"
 
def temp_choser(JSONOBJECT):
    JSONOBJECT = json.loads(event.body_as_str(encoding='UTF-8'))
    #numberList = TEMP
    TEMP = JSONOBJECT.get("Temperature")
    print(TEMP)



async def checkForMessages():
    global OFFSET
    response = requests.post(url = (URL + "getUpdates"),params= {"offset":OFFSET})
    data = response.json()
    if (data["ok"]):
        for i in range(len(data["result"])):
            if((data["result"][i]["update_id"]) >= OFFSET):
                OFFSET = data["result"][i]["update_id"] + 1
                #print(OFFSET)
            if(data["result"][i]["message"]["chat"]["id"] == ID_SIMON):
                text = data["result"][i]["message"]["text"]
                #print(text)
                if ("/getTemperature" in text):
                    await replyTemperature(ID_SIMON)
            if(data["result"][i]["message"]["chat"]["id"] == ID_SIMON):
                #print(text)
                if ("/getTemperature" in text):
                    await replyTemperature(ID_SIMON)

    elif(not data["ok"]):
        print("Error")

#r = requests.get(url = URL + SEND, params = PARAMS) 
#data = r.json()

async def replyTemperature(chat_id):
    global LATEST_TEMP
    reply = "Temperature is {}°C".format(LATEST_TEMP)
    requests.post(url = (URL + "sendMessage"),params = {"chat_id":chat_id,"text":reply})

async def checkUpdates():
    while True:
        await checkForMessages()
        await asyncio.sleep(5)

async def on_event(partition_context, event):
    global LATEST_TEMP
    # Print the event data.
    print("Received the event: \"{}\" from the partition with ID: \"{}\"".format(event.body_as_str(encoding='UTF-8'), partition_context.partition_id))
    JSONOBJECT = json.loads(event.body_as_str(encoding='UTF-8'))
    TEMP = JSONOBJECT.get("Temperature")
    print(TEMP)
    print(LATEST_TEMP)
    send_chat(TEMP)
    await partition_context.update_checkpoint(event)

def send_chat(CONTENT):
    PARAMS = {'chat_id':1287770548 , 'text': CONTENT}
    r = requests.get(url = URL + SEND, params = PARAMS) 
    
async def main():
    # Create an Azure blob checkpoint store to store the checkpoints.
    Checkpoint_store = BlobCheckpointStore.from_connection_string("DefaultEndpointsProtocol=https;AccountName=individstorage;AccountKey=sflIN1b46bYLMOLdDy0G7ssweyIzI5iuHSqSPV+1SsM7i0dbFZagSBt1Bi4GYTCkPFWiWQiCRzSTaSPnK3ttSA==;EndpointSuffix=core.windows.net", "individstorage")

    # Create a consumer client for the event hub.
    client = EventHubConsumerClient.from_connection_string("Endpoint=sb://kdgslimkippenhok.servicebus.windows.net/;SharedAccessKeyName=tempAnalytics_OutputTime_policy;SharedAccessKey=aQjkTANBDDWOqknzbLbnVwSqDIwlX+tMTsVt5UIJZzE=;EntityPath=skeventhub", consumer_group="$Default", eventhub_name="skeventhub") 
    async with client:
        # Call the receive method. Read from the beginning of the partition (starting_position: "-1")
        await client.receive(on_event=on_event,  starting_position="-1")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.create_task(main())
    loop.create_task(checkUpdates())
    loop.run_forever()
    # Run the main method.
    loop.run_until_complete(main())
    