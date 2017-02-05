package Hadoop_StudMarks_Writable;
import java.io.PrintStream;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class HSMW_partitionar
  extends Partitioner<HSMW_writable, NullWritable>
{
  public int getPartition(HSMW_writable composite_val, NullWritable null_val, int numPartitions)
  {
    System.out.println("I am inside the partitionar");
    System.out.println(composite_val.getname().hashCode() % numPartitions);
    
    return composite_val.getname().hashCode() % numPartitions;
  }
}
