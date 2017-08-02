/**
 * Put your copyright and license info here.
 */
package com.snapshotschemaapps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.Date;

/**
 * This is a simple operator that emits random number.
 */
public class RandomNumberGenerator extends BaseOperator implements InputOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(RandomNumberGenerator.class);

  private int minValue = 0;
  private int maxValue = 100;
  private int interval = 1000;
  private Date lastRun = new Date();

  public final transient DefaultOutputPort<Integer> out = new DefaultOutputPort<>();

  @Override
  public void emitTuples()
  {
    int diff = (int)(new Date().getTime() - lastRun.getTime());
    if (diff > interval) {
      int val = (int)Math.ceil((Math.random() * (maxValue - minValue) + minValue));
      out.emit(val);
      lastRun = new Date();
      LOG.info("***** info: val: {}", val);
      LOG.warn("***** warn: val: {}", val);
      LOG.debug("***** debug: val: {}", val);
      LOG.error("***** error: val: {}", val);
      LOG.trace("***** trace: val: {}", val);
    }
  }

  public int getMinValue()
  {
    return minValue;
  }

  /**
   * Sets the min value to be emitted every window.
   * @param minValue
   */
  public void setMinValue(int minValue)
  {
    this.minValue = minValue;
  }

  public int getMaxValue()
  {
    return maxValue;
  }

  /**
   * Sets the max value to be emitted every window.
   * @param maxValue
   */
  public void setMaxValue(int maxValue)
  {
    this.maxValue = maxValue;
  }

  public int getInterval()
  {
    return interval;
  }

  /**
   * Sets the interval (milliseconds) to emit random numbers.
   * @param interval
   */
  public void setInterval(int interval)
  {
    this.interval = interval;
  }
}
