from py4j.java_gateway import JavaGateway, GatewayClient
gateway = JavaGateway(GatewayClient(port=25335))

solap4py = gateway.entry_point.getSolap4py();

def process(query):
    return solap4py.process(query);
