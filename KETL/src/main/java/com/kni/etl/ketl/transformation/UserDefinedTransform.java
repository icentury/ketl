package com.kni.etl.ketl.transformation;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;
import java.sql.Connection;
import java.text.Format;
import java.text.ParsePosition;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Date;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import net.minidev.json.JSONObject;

import com.kni.etl.ketl.ETLOutPort;
import com.kni.etl.ketl.RecordChecker;
import com.kni.etl.ketl.exceptions.KETLError;
import com.kni.etl.ketl.exceptions.KETLException;
import com.kni.etl.ketl.exceptions.KETLQAException;
import com.kni.etl.ketl.exceptions.KETLTransformException;
import com.kni.etl.ketl.smp.AggregateTransformCore;
import com.kni.etl.ketl.smp.BatchManager;
import com.kni.etl.ketl.smp.BatchTransformCore;
import com.kni.etl.ketl.smp.ETLStats;
import com.kni.etl.ketl.smp.ETLWorker;
import com.kni.etl.ketl.smp.KETLExceptionWrapper;
import com.kni.etl.ketl.smp.TransformBatchManager;

public abstract class UserDefinedTransform implements Iterable<Object[]>, BatchTransformCore,
    KETLExceptionWrapper<KETLTransformException> {

  final public Object getResource(String name) {
    return null;
  }

  final public String getTempDir() {
    return this.configuration.getTempDir();
  }

  final public class Input {
    final private ETLOutPort port;

    public Input(ETLOutPort port) {
      this.port = port;
    }


    private boolean used = true;

    public void used(boolean used) {
      this.used = used;
    }

    public boolean used() {
      return used;
    }

    public int index() {
      return this.port.getPortIndex();
    }

    public Class<?> inputClass() {
      return this.port.getPortClass();
    }

    public String name() {
      return this.port.getName();
    }

    public String channel() {
      return this.port.getChannel();
    }
  }

  final public class Output {
    final Class<?> fieldClass;

    final String fieldName;

    public Output(String fieldName, Class<?> fieldClass) {
      super();
      this.fieldName = fieldName;
      this.fieldClass = fieldClass;
    }
  }

  private List<Output> outputs = new ArrayList<Output>();

  final protected void addOut(String name, Class<?> cls) {
    this.outputs.add(new Output(name, cls));
  }

  abstract public void transform() throws InterruptedException, KETLTransformException,
      KETLQAException;

  final class QueueIterator implements Iterator<Object[]> {

    private Deque<Object[]> res = new ArrayDeque<Object[]>();

    @Override
    public boolean hasNext() {
      try {
        while (res.isEmpty()) {

          Object[][] batch = sourceQueue.take();

          if (batch == ETLWorker.ENDOBJ) {
            return false;
          } else if (batch.length > 0)
            if (mBatchManager != null) {
              batch = mBatchManager.initializeBatch(batch, batch.length);
            }

          res.addAll(java.util.Arrays.asList(batch));
        }

        return true;
      } catch (Exception e) {
        throw new KETLError(e);
      }

    }

    @Override
    public Object[] next() {
      return res.pop();
    }

    @Override
    public void remove() {
      res.pop();
    }

  }

  private List<Input> inputs;
  private int item = 0, batchSize = 100;
  private AggregateTransformCore mAggregator = null;
  private TransformBatchManager mBatchManager;
  private Object[][] outBatch = null;
  private RecordChecker mRecordChecker;
  private BlockingQueue<Object[][]> sourceQueue;
  private ETLStats mStatsCollector;
  private BlockingQueue<Object[][]> targetQueue;
  private UDFConfiguration configuration;

  final public void close() throws InterruptedException, KETLTransformException {
    flush();
    this.targetQueue.put(ETLWorker.ENDOBJ);
    if (mAggregator != null) {
      mAggregator.aggregateBatch(null, -1);
    }
  }

  final public void emit(Object[] record) throws InterruptedException, KETLTransformException,
      KETLQAException {

    if (record.length != this.outputs.size())
      throw new KETLTransformException(
          "Emitted record array does not match the expected record number of fields. Expected field count->"
              + this.outputs.size() + ", record contains->" + record.length);
    if (outBatch == null)
      outBatch = new Object[this.batchSize][];
    outBatch[item++] = record;
    this.mRecordChecker.recordCheck(record, null);

    if (item >= outBatch.length) {
      flush();
    }
  }

  @Override
  final public void wrapException(Exception e) throws KETLTransformException {
    if (e instanceof KETLTransformException)
      throw (KETLTransformException) e;

    throw new KETLTransformException(e);

  }

  final public void emitException(Exception e, Object[] record, int recordNumber)
      throws InterruptedException, KETLTransformException, KETLQAException {
    this.mRecordChecker.recordCheck(record, e);
    try {
      this.mStatsCollector.incrementErrorCount(new KETLTransformException(e), record, recordNumber);
    } catch (KETLException e2) {
      wrapException(e2);
    }
  }



  final protected void setOptions(TransformBatchManager batchManager, RecordChecker recordChecker,
      ETLStats statsCollector, UDFConfiguration config) {
    this.mStatsCollector = statsCollector;
    this.mRecordChecker = recordChecker;
    this.mBatchManager = batchManager;
    this.mRecordChecker = recordChecker;
    this.configuration = config;
  }

  final private void flush() throws InterruptedException, KETLTransformException {
    if (item > 0) {
      // resize is flushing before full
      if (item < outBatch.length) {
        Object[][] tmp = new Object[item][];
        System.arraycopy(outBatch, 0, tmp, 0, item);
        outBatch = tmp;
      }

      if (this.mBatchManager != null) {
        this.mBatchManager.finishBatch(null, BatchManager.LASTBATCH);
      }

      if (this.mAggregator != null) {
        this.mAggregator.aggregateBatch(outBatch, outBatch.length);
      } else
        this.targetQueue.put(outBatch);

      mStatsCollector.updateThreadStats(item);
      outBatch = null;
      item = 0;
    }
  }

  final protected List<Input> getInputs() {
    return this.inputs;
  }

  final protected List<Output> getOutputs() {
    return this.outputs;
  }

  abstract public void instantiate(List<Input> inputs) throws KETLTransformException;

  @Override
  final public void iterableTransform(BlockingQueue<Object[][]> sourceQueue,
      BlockingQueue<Object[][]> targetQueue) {
    this.sourceQueue = sourceQueue;
    this.targetQueue = targetQueue;
  }

  @Override
  final public Iterator<Object[]> iterator() {
    return new QueueIterator();
  }

  public Input newInput(ETLOutPort port) {
    return new Input(port);
  }



  final protected Map<String, JSONObject> getValueMapping(String arg0)
      throws KETLTransformException {
    try {
      return this.configuration.getValueMapping(arg0);
    } catch (Exception e) {
      throw new KETLTransformException(e);
    }
  }

  final protected String getParameter(String arg0) {
    return this.configuration.getParameter(arg0);
  }

  final protected String getAttribute(String arg0) {
    return this.configuration.getAttribute(arg0);
  }

  final protected Connection getConnection(String arg0) throws KETLTransformException {
    return this.configuration.getConnection(arg0);
  }



  final protected void setSharedResource(String field, Object value) {
    this.configuration.setSharedResource(field, value);
  }

  final protected Object getSharedResource(String field) {
    return this.configuration.getSharedResource(field);
  }

  final protected void releaseConnection(Connection con) {
    this.configuration.releaseConnection(con);

  }

  final protected Object cast(Object val, Class cl, Format formatter, ParsePosition position)
      throws KETLTransformException {

    try {

      if (val == null)
        return null;

      if (val.getClass() == cl)
        return val;

      Number num = val instanceof Number ? (Number) val : null;
      String result = val.toString();

      if (cl == Float.class || cl == float.class)
        return num == null ? Float.parseFloat(result) : num.floatValue();

      if (cl == String.class)
        return result;

      if (cl == Long.class || cl == long.class)
        return num == null ? Long.parseLong(result) : num.longValue();

      if (cl == Integer.class || cl == int.class)
        return num == null ? Integer.parseInt(result) : num.intValue();

      if (cl == java.util.Date.class || cl == java.sql.Timestamp.class || cl == java.sql.Date.class) {

        if (num != null) {
          long epoch = num.longValue();
          if (epoch < 11396265000l)
            epoch = epoch * 1000;
          return new Date(epoch);
        }

        position = new ParsePosition(0);

        position.setIndex(0);
        return formatter.parseObject(result, position);
      }

      if (cl == Double.class || cl == double.class)
        return num == null ? Double.parseDouble(result) : num.doubleValue();

      if (cl == Character.class || cl == char.class)
        return new Character(result.charAt(0));

      if (cl == Boolean.class || cl == boolean.class)
        return Boolean.parseBoolean(result);

      if (cl == Byte[].class || cl == byte[].class)
        return result.getBytes();

      if (cl == BigDecimal.class)
        return new BigDecimal(result);

      Constructor<?> con;
      try {
        con = cl.getConstructor(new Class[] {String.class});
      } catch (Exception e) {
        throw new KETLTransformException("No constructor found for class " + cl.getCanonicalName()
            + " that accepts a single string");
      }
      return con.newInstance(new Object[] {result});

    } catch (Exception e) {
      throw new KETLTransformException("Failed to cast ", e);
    }
  }

  public void complete() {

  }

}
