from py4j.java_gateway import JavaGateway, GatewayClient
gateway = JavaGateway()

solap4py = gateway.entry_point.getSolap4py();

def getMetadata(param):
    return solap4py.getMetadata(param);