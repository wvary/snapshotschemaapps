/**
 * Put your copyright and license info here.
 */
package com.snapshotschemaapps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.BaseOperator;

import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.appdata.snapshot.AppDataSnapshotServerMap;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;

import java.net.URI;

public class MyPubSub extends AppDataSnapshotServerMap
{
  private static final Logger LOG = LoggerFactory.getLogger(MyPubSub.class);

  private String topic = "RandomNumberQueryTopic";
  private transient PubSubWebSocketAppDataQuery queryProvider;

  public MyPubSub() {
  }

  public MyPubSub(DAG dag, URI gatewayURI) {
    this.setSnapshotSchemaJSON(SchemaUtils.jarResourceFileToString("schema.json"));

    queryProvider = new PubSubWebSocketAppDataQuery();
    queryProvider.setUri(gatewayURI);
    queryProvider.setTopic(topic);
    this.setEmbeddableQueryInfoProvider(queryProvider);
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    queryProvider.setTopic(topic);
    this.topic = topic;
  }
}