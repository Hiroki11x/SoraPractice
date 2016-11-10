package dynamodbutil;

import org.apache.hadoop.mapred.*;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.Serializable;
import java.util.stream.IntStream;

import static java.util.Objects.requireNonNull;

/**
 * Created by hirokinaganuma on 16/11/10.
 */
public class DynamoDbInputFormat implements InputFormat, Serializable {

    private final String NUMBER_OF_SPLITS = "2";//適当に追加

    @Override
    public InputSplit[] getSplits(@Nonnull final JobConf job, final int numSplits) throws IOException {
        final int splits = Integer.parseInt(requireNonNull(job.get(NUMBER_OF_SPLITS), NUMBER_OF_SPLITS
                + " must be non-null"));

        return IntStream.
                range(0, splits).
                mapToObj(segmentNumber -> new DynamoDbSplit(segmentNumber, splits)).
                toArray(InputSplit[]::new);
    }

    @Override
    public RecordReader getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
        return null;
    }
}