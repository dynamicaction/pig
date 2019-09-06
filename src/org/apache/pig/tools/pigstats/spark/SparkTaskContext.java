package org.apache.pig.tools.pigstats.spark;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.pig.backend.hadoop.executionengine.TaskContext;

public class SparkTaskContext extends TaskContext<SparkCounters> {

    SparkCounters counters;
    
    public SparkTaskContext(SparkCounters counters) {
        super();
        this.counters = counters;
    }

    @Override
    public SparkCounters get() {
        return counters;
    }

    private SparkCounter<Long> getCounterImpl(Enum<?> name) {
        if (counters == null) return null;
        @SuppressWarnings("unchecked")
        SparkCounter<Long> counter = counters.getCounter(name);
        return counter;
    }
    
    private SparkCounter<Long> getCounterImpl(String group, String name) {
        if (counters == null) return null;
        @SuppressWarnings("unchecked")
        SparkCounter<Long> counter = counters.getCounter(group, name);
        return counter;
    }
    
    @Override
    public Counter getCounter(Enum<?> name) {
        return adaptCounter(getCounterImpl(name));
    }

    @Override
    public Counter getCounter(String group, String name) {
        return adaptCounter(getCounterImpl(group, name));
    }

    @Override
    public boolean incrCounter(Enum<?> name, long delta) {
        SparkCounter<Long> counter = getCounterImpl(name);
        if (counter == null) return false;
        counter.increment(delta);
        return true;
    }

    @Override
    public boolean incrCounter(String group, String name, long delta) {
        SparkCounter<Long> counter = getCounterImpl(group, name);
        if (counter == null) return false;
        counter.increment(delta);
        return true;
    }

    private Counter adaptCounter(SparkCounter<Long> counter) {
        return counter == null ? null : new Counter() {
            public void write(DataOutput out) throws IOException {
                throw new RuntimeException("Not supported");
            }
            
            @Override
            public void readFields(DataInput in) throws IOException {
                throw new RuntimeException("Not supported");
            }
            
            @Override
            public void setValue(long value) {
                throw new RuntimeException("Not supported");
            }
            
            @Override
            public void setDisplayName(String displayName) {
                counter.setDisplayName(displayName);
            }
            
            @Override
            public void increment(long incr) {
                counter.increment(incr);
            }
            
            @Override
            public long getValue() {
                return counter.getValue();
            }
            
            @Override
            public Counter getUnderlyingCounter() {
                return null;
            }
            
            @Override
            public String getName() {
                return counter.getName();
            }
            
            @Override
            public String getDisplayName() {
                return counter.getDisplayName();
            }
        };
    }
}
