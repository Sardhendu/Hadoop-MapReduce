package Trail_Files;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;


public class TreeMapDemo {

	   public static void main(String args[]) {
	      // Create a hash map
		   TreeMap<Double, String> tm = new TreeMap();
	      
	      // Put elements to the map
	      tm.put(new Double(3434.34),"Zara");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        } 
	      tm.put(new Double(123.22), "Mahnaz");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      tm.put(new Double(1378.00), "Ayan");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      tm.put(new Double(99.22), "Daisy" );
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      tm.put(new Double(-19.08), "Qadir");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      tm.put(new Double(-222.08), "ladir");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      tm.put(new Double(-222.08), "kamchor");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      tm.put(new Double(222.08), "motherFucker");
	      if (tm.size() > 5) {
	          System.out.println(tm.firstKey());
	              tm.remove(tm.lastKey());
	        }
	      
	      
	      // Get a set of the entries
	      Set set = tm.entrySet();
	      
	      // Get an iterator
	      Iterator i = set.iterator();
	      
	      // Display elements
	      while(i.hasNext()) {
	         Map.Entry me = (Map.Entry)i.next();
	         System.out.print(me.getKey() + ": ");
	         System.out.println(me.getValue());
	      }
	      System.out.println();
	      
	      // Deposit 1000 into Zara's account

	   }
	}