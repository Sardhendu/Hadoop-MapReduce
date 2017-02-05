package Hadoop_StudMarks_Writable;

import java.io.PrintStream;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HSMW_compositecomparator
  extends WritableComparator
{
  protected HSMW_compositecomparator()
  {
    super(HSMW_writable.class, true);
  }
  
  public int compare(WritableComparable w1, WritableComparable w2)
  {
    System.out.println("Inside the compositecomparator class");
    HSMW_writable key1 = (HSMW_writable)w1;
    HSMW_writable key2 = (HSMW_writable)w2;
    
    int compare = key1.getname().compareTo(key2.getname());
    int b = 0;
    if (compare == 0) {
      if (key1.getmarks() < key2.getmarks()) {
        b = 1;
      } else if (key1.getmarks() > key2.getmarks()) {
        b = -1;
      } else {
        b = 0;
      }
    }
    return b;
  }
}