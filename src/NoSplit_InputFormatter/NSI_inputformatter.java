package NoSplit_InputFormatter;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

public class NSI_inputformatter
  extends FileInputFormat<NullWritable, Text>
{
  public boolean isSplitable(FileSystem fs, Path filename)
  {
    return false;
  }
  
  public RecordReader<NullWritable, Text> getRecordReader(InputSplit split, JobConf job_run, Reporter reporter)
    throws IOException
  {
    return new NSI_record_reader(job_run, (FileSplit)split);
  }
}
