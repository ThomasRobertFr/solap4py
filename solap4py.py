import socket

#from py4j.java_gateway import JavaGateway, GatewayClient
#gateway = JavaGateway(GatewayClient(port=25335))

#solap4py = gateway.entry_point.getSolap4py();


def process(strQuery):
    # Converts the input string (formatted as a json text) to a byte array
    #bytes = bytearray(strQuery)

    # Process the query and get the resulting byte array
    #result = solap4py.process(bytes)

    # Decode the byte array with utf-8
    #utf8Result = result.decode('utf-8')
    
    # Return the resulted string
    #return utf8Result;

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("localhost", 25335)) # TODO which port should we use ?

    s.send(strQuery + '\r\n') 
    data = bytearray()
    while 1:
        chunk = s.recv(65536)
        if not chunk:
            break
        data.extend(chunk)
    
    return data.decode(encoding='utf-8')
