package Hadoop_StudMarks_Writable;
import java.io.PrintStream;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class HSMW_groupcomparator
  extends WritableComparator
{
  protected HSMW_groupcomparator()
  {
    super(HSMW_writable.class, true);
  }
  
  public int compare(WritableComparable w1, WritableComparable w2)
  {
    System.out.println("inside the HSMW_groupcomparator class");
    
    HSMW_writable key1 = (HSMW_writable)w1;
    HSMW_writable key2 = (HSMW_writable)w2;
    System.out.println("key_1.name is " + key1.getname() + " key_2.name is " + key2.getname());
    System.out.println("key_1.marks is " + key1.getmarks() + " key_2.marks is " + key2.getmarks());
    
    return key1.getname().compareTo(key2.getname());
  }
}
