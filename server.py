import asyncio
import aiohttp
import re
import sys
import argparse
import json
import time

#GLOBALS
HOST = '127.0.0.1'

PORT_ASSIGNMENTS = {'Bailey': 20608
                    , 'Bona' : 20609
                    , 'Campbell' : 20610
                    , 'Clark' : 20611
                    , 'Jaquez' : 20612
                    }

SERVER_RELATIONSHIPS = {'Bailey': ['Bona','Campbell']
                    , 'Bona' : ['Bailey','Campbell','Clark']
                    , 'Campbell' : ['Bona','Bailey','Jaquez']
                    , 'Clark' : ['Bona','Jaquez']
                    , 'Jaquez' : ['Clark','Campbell']
                    }

PLACES_API_KEY = 'AIzaSyDOj4L9-dPX0n-_1-NoH960sqbSDbHWfO8'





clients_to_latest_timestamp = dict() #keeps track of the latest timestamp for each client this server talks to to prevent sending message twice
clients_to_full_AT_message = dict() #keeps track of the latest AT message for each client

serverID = sys.argv[1] #fetch the name of the server

def handle_WHATSAT(input_line):
    terms = input_line.split()

    # timestamp_difference = time.time()-float(terms[3]) #calculate difference between received timestamp and current time
    # if timestamp_difference >= 0:
    #     timestamp_difference = f"+{timestamp_difference}" #add + if diff is positive
    # else:
    #     timestamp_difference = f"-{timestamp_difference}"

    # response = f"AT {serverID} {timestamp_difference} {terms[1]} {terms[2]} {terms[3]}"
    response = f"{clients_to_full_AT_message[terms[1]]}"
    return f"{response}"

def handle_IAMAT(input_line):
    terms = input_line.split()

    timestamp_difference = time.time()-float(terms[3]) #calculate difference between received timestamp and current time
    if timestamp_difference >= 0:
        timestamp_difference = f"+{timestamp_difference}" #add + if diff is positive
    # else:
    #     timestamp_difference = f"-{timestamp_difference}"

    response = f"AT {serverID} {timestamp_difference} {terms[1]} {terms[2]} {terms[3]}"
    return response

async def propagate_servers(message):
    #iterate thru this server's neighbors
    for neighbor in SERVER_RELATIONSHIPS[serverID]:
        try:
            reader, writer = await asyncio.open_connection('127.0.0.1', PORT_ASSIGNMENTS[neighbor]) #establish connection to neighbor
            writer.write(message.encode()) #send the message to neighbor
            await writer.drain()
            print(f"Connection made: Succeeded in propagating to {neighbor} on port {PORT_ASSIGNMENTS[neighbor]}")
            writer.close()
            await writer.wait_closed()
        except:
            print(f"Connection dropped: Failed to connect to {neighbor} on port {PORT_ASSIGNMENTS[neighbor]}. Couldn't propagate to it")

async def call_google_api(client_name, radius):
    radius = float(radius)*1000
    latest_at_message_split = clients_to_full_AT_message[client_name].split() #retrieve the latest AT message for this client
    coordinates = latest_at_message_split[4]
    latandlong = re.findall("[+-][0-9]+\\.[0-9]+", coordinates) #do regex to get the latitude and longitude in a list

    async with aiohttp.ClientSession() as session:

        API_request = f'https://maps.googleapis.com/maps/api/place/nearbysearch/json?location={latandlong[0]},{latandlong[1]}&radius={radius}&key={PLACES_API_KEY}'

        async with session.get(API_request) as resp:
            return (await resp.text())




async def handle_connection(reader, writer):
    while not reader.at_eof(): #keep continually reading in commands
        data = await reader.readline()
        line_received = data.decode()
        print(f"Input: {line_received}")


        #PARSE THE INPUT
        parsed_input = line_received.split()

        result_parsed_line = ""

        if len(parsed_input) == 0: #if the input is nothing
            result_parsed_line = f"? {line_received}"
            print(f"Output (to client): {result_parsed_line}")
            writer.write(result_parsed_line.encode())
            await writer.drain()
            

        elif parsed_input[0] == "IAMAT": #case we get IAMAT
            result_parsed_line = handle_IAMAT(line_received) #this creates an AT message

            clients_to_full_AT_message[parsed_input[1]] = result_parsed_line #set this AT message we just made as the most recent one for this client
            clients_to_latest_timestamp[parsed_input[1]] = parsed_input[3] # set the IAMAT message's time as the latest timestamp for this client

            # print(f"Acknowledged as IAMAT request. Will respond to client and attempt to propagate this: {result_parsed_line}")
            print(f"Output (to client and other servers): {result_parsed_line}")
            writer.write(result_parsed_line.encode())
            await writer.drain()
            await propagate_servers(result_parsed_line)


        elif parsed_input[0] == "WHATSAT": #case we get WHATSAT
            if (int(parsed_input[2]) > 50 or int(parsed_input[2]) < 0) or (int(parsed_input[3]) > 20 or int(parsed_input[3]) < 0): #if the kilometers or amt of information is not within bounds
                #then recognize this as an invalid command
                result_parsed_line = f"? {line_received}"
                print(f"Output (to client): {result_parsed_line}")
                writer.write(result_parsed_line.encode())
                await writer.drain()
                continue
            
            elif parsed_input[1] not in clients_to_full_AT_message: #if we've never encountered this client before, then we can't return an AT message
                #then recognize this as an invalid command
                result_parsed_line = f"? {line_received}"
                print(f"Output (to client): {result_parsed_line}")
                writer.write(result_parsed_line.encode())
                await writer.drain()
                continue

            else:
                api_call_result = await call_google_api(parsed_input[1], parsed_input[2]) #call the Google places API

                result_decoded = json.loads(api_call_result) #decode the json
                result_decoded["results"] = result_decoded["results"][:int(parsed_input[3])] #ensure that # results in results is up to specified amount
                
                result_encoded = json.dumps(result_decoded,indent=4)

                # print(result_encoded)

                result_parsed_line = f"{handle_WHATSAT(line_received)}{result_encoded}\n\n"
                # print(f"Acknowledged as WHATSAT request. Response to client: {result_parsed_line}")
                print(f"Output (to client): {result_parsed_line}")
                writer.write(result_parsed_line.encode())
                await writer.drain()
                # await propagate_servers(result_parsed_line)


        elif parsed_input[0] == "AT": #case we get AT; this means it's probably propagated from another server, but still check
            result_parsed_line = line_received
            ok_to_propagate = True

            clients_to_full_AT_message[parsed_input[3]] = line_received #set this AT message as the most recent one received from this client

            #CHECK to see if we've already propagated
            if parsed_input[3] in clients_to_latest_timestamp: #if we have encountered this client before
                
                #if the new message received has the same timestamp than the most recent saved one, then it's the same. 
                if float(clients_to_latest_timestamp[parsed_input[3]]) == float(parsed_input[5]):
                    print(f"Input (will not propagated because it's been propagated already): {line_received}")
                    ok_to_propagate = False

            if ok_to_propagate == True:
                #if we make it this far, we'll propagate the message. but we should add this to our dict to prevent future propagation
                clients_to_latest_timestamp[parsed_input[3]] = parsed_input[5]

                # print(f"Acknowledged as AT message. Will attempt to propagate this, if not already propagated: {result_parsed_line}")
                # await writer.drain()
                print(f"Output (to servers): {result_parsed_line}")
                await propagate_servers(result_parsed_line)
        
        else: #invalid command, so we will NOT propagate
            result_parsed_line = f"? {line_received}"
            print(f"Output (to client): {result_parsed_line}")
            writer.write(result_parsed_line.encode())
            await writer.drain()



    writer.close()
    


def main():
    async def forever():
        server = await asyncio.start_server(handle_connection, host='127.0.0.1', port=PORT_ASSIGNMENTS[serverID])
        
        async with server:
            await server.serve_forever()

        print("Closing server")
        server.close()



    sys.stdout=open(f'{serverID}.txt', 'w') #begin writing to this server's log file
    print(f"Started up server {serverID} on port {PORT_ASSIGNMENTS[serverID]}")
    try:
        asyncio.run(forever())
    except KeyboardInterrupt:
        pass #allow interrupt to stop the server
    sys.stdout.close() #stop writing to this server's log file



if __name__ == '__main__':
    main()
    # asyncio.run(main())
