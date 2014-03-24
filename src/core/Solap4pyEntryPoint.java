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
    
    public void run(){
    	GatewayServer gatewayServer = new GatewayServer(this);
        gatewayServer.start();
    }
    
    public static void main(String[] args) {
    	Solap4pyEntryPoint s = new Solap4pyEntryPoint();
    	s.run();
    }
	
	
}

