import time
import json
from py4j.java_gateway import JavaGateway, GatewayClient
gateway = JavaGateway(GatewayClient(port=25335))

solap4py = gateway.entry_point.getSolap4py();



def process(strQuery):
    deb = time.time()
    # Converts the input string (formatted as a json text) to a byte array
    bytes = bytearray(strQuery)

    print time.time() - deb

    # Process the query and get the resulting byte array
    result = solap4py.process(bytes)

    print time.time() - deb

    # Decode the byte array with utf-8
    utf8Result = result.decode('utf-8')
    
    print time.time() - deb
    # Return the resulted string
    return utf8Result;
