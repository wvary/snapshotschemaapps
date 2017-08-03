package com.snapshotschemaapps;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.lib.io.ConsoleOutputOperator;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;

import java.net.URI;

@ApplicationAnnotation(name="PubSubAppOnly")
public class PubSubApplicationOnly implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Context.OperatorContext context;
    String appName = context.getValue(Context.DAGContext.APPLICATION_ID);
    System.out.println("name: ");
    System.out.println(appName);

    String gatewayAddress = dag.getValue(Context.DAGContext.GATEWAY_CONNECT_ADDRESS);
    URI gatewayURI = URI.create("ws://" + gatewayAddress + "/pubsub");

    MyPubSub dataServer = dag.addOperator("server", new MyPubSub(dag, gatewayURI));

    RandomNumberGenerator randomGenerator = dag.addOperator("randomGenerator", RandomNumberGenerator.class);

    DataMap dataMap = dag.addOperator("dataMapper", DataMap.class);

    PubSubWebSocketAppDataResult dataResult = dag.addOperator("result", PubSubWebSocketAppDataResult.class);
    dataResult.setUri(gatewayURI);

    dag.addStream("randomNumber", randomGenerator.out, dataMap.input);
    dag.addStream("mapper", dataMap.outputData, dataServer.input);
    dag.addStream("results", dataServer.queryResult, dataResult.input);
  }

}
