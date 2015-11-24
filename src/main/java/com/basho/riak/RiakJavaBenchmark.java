package com.basho.riak;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;

import com.basho.riak.client.api.RiakClient;
import com.basho.riak.client.core.RiakCluster;
import com.basho.riak.client.core.RiakNode;

public class RiakJavaBenchmark 
{
	
    public static void main( String[] args )
    {
    	String[] hosts = {"127.0.0.1"};
    	RiakNode.Builder builder = new RiakNode.Builder();
    	List<RiakNode> nodes;
    	RiakClient client = null;
    	try {
    		nodes = RiakNode.Builder.buildNodes(builder, Arrays.asList(hosts));
    		RiakCluster riakCluster = new RiakCluster.Builder(nodes).build();
    		riakCluster.start();
    		client = new RiakClient(riakCluster);    		
    	} catch (UnknownHostException e) {
    		e.printStackTrace();
    	}
    }
}
