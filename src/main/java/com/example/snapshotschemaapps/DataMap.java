package com.snapshotschemaapps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Willet on 9/7/2016
 */
public class DataMap extends BaseOperator {

  private static final Logger LOG = LoggerFactory.getLogger(DataMap.class);

  public transient final DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
    @Override
    public void process(Integer value) {
        List<Map<String, Object>> dataPoints = new ArrayList<>();
        Map<String, Object> dataPoint = new HashMap<>();
        dataPoint.put("value", value);
        dataPoints.add(dataPoint);
        outputData.emit(dataPoints);
      LOG.info("***** info: value: {}", value);
      LOG.warn("***** warn: value: {}", value);
      LOG.debug("***** debug: value: {}", value);
      LOG.error("***** error: value: {}", value);
      LOG.trace("***** trace: value: {}", value);
    }
  };

  public transient final DefaultOutputPort<List<Map<String, Object>>> outputData = new DefaultOutputPort<>();
}
