package core;

import py4j.GatewayServer;

public class Solap4pyEntryPoint{
	
	private Solap4py mySolap4py;


    public Solap4pyEntryPoint() {
    	mySolap4py = new Solap4py();
    }

    public Solap4py getSolap4py() {
        return mySolap4py;
    }
    
    public static void main(String[] args) {
    	GatewayServer gatewayServer = new GatewayServer(new Solap4pyEntryPoint());
        gatewayServer.start();
    }
	
	
}

