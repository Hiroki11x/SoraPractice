package dynamodbutil;

import org.apache.hadoop.mapred.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by hirokinaganuma on 16/11/10.
 */
public class DynamoDbSplit implements InputSplit, Serializable {
    private int segmentNumber;
    private int splits;

// have setter and getter methods to read and set the variables


    public DynamoDbSplit(int segmentNumber, int splits) {
        this.segmentNumber = segmentNumber;
        this.splits = splits;
    }

    public int getSegmentNumber() {
        return segmentNumber;
    }

    public int getSplits() {
        return splits;
    }

    public void setSegmentNumber(int segmentNumber) {
        this.segmentNumber = segmentNumber;
    }

    public void setSplits(int splits) {
        this.splits = splits;
    }


    @Override
    public long getLength() throws IOException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        return new String[0];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {

    }
}