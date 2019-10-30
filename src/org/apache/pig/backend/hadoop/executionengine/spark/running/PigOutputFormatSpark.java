package org.apache.pig.backend.hadoop.executionengine.spark.running;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobStatus.State;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputCommitter;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigOutputFormat;
import org.apache.pig.backend.hadoop.executionengine.physicalLayer.relationalOperators.POStore;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.UDFContext;

public class PigOutputFormatSpark extends PigOutputFormat {
    @Override
    public RecordWriter<WritableComparable, Tuple> getRecordWriter(
            TaskAttemptContext taskattemptcontext) throws IOException,
            InterruptedException {
        resetUDFContextForThreadReuse();
        return super.getRecordWriter(taskattemptcontext);
    }

    @Override
    public OutputCommitter getOutputCommitter(
            TaskAttemptContext taskattemptcontext) throws IOException,
            InterruptedException {
        resetUDFContextForThreadReuse();
        setupUdfEnvAndStores(taskattemptcontext);

        // we return an instance of PigOutputCommitterSpark (PIG-4202) to Hadoop - this instance
        // will wrap the real OutputCommitter(s) belonging to the store(s)
        return new PigOutputCommitterSpark(taskattemptcontext,
                mapStores,
                reduceStores);
    }

    public static void resetUDFContextForThreadReuse() {
        // On the Spark AM, MROutput OutputCommitters are initialized and setupJob
        // called on them in a loop in the same thread.
        // commitJob/abortJob can be called from any thread based on events received from vertices

        // On the Spark tasks, it initializes different inputs/outputs in different Initializer threads
        // by submitting them to a thread pool. Even though threadpoolsize=numInputs+numOutputs
        // a thread can be reused.

        // Since deserialized UDFContext from input and output payload contains
        // information only for that input or output reduce payload sizes, we need to
        // ensure it is deserialized everytime before use in a thread to get the right one.
        UDFContext.getUDFContext().reset();
    }

    public static class PigOutputCommitterSpark extends PigOutputCommitter {

        public PigOutputCommitterSpark(TaskAttemptContext context,
                List<POStore> mapStores, List<POStore> reduceStores)
                throws IOException {
            super(context, mapStores, reduceStores);
        }

        @Override
        public void setupJob(JobContext context) throws IOException {
            resetUDFContextForThreadReuse();
            try {
                super.setupJob(context);
            } finally {
                resetUDFContextForThreadReuse();
            }

        }

        @Override
        public void commitJob(JobContext context) throws IOException {
            resetUDFContextForThreadReuse();
            try {
                super.commitJob(context);
            } finally {
                resetUDFContextForThreadReuse();
            }
        }

        @Override
        public void abortJob(JobContext context, State state)
                throws IOException {
            resetUDFContextForThreadReuse();
            try {
                super.abortJob(context, state);
            } finally {
                resetUDFContextForThreadReuse();
            }
        }

    }
}
