package Trail_Files;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
//import java.util.Map;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class KeyValue {
//	Map<String, List<String>> hm = new HashMap<String, List<String>>();
	private TreeMap<Integer, Set> hm = new TreeMap<Integer, Set>();
	public void hitme(){
//		List<String> values = new ArrayList<String>();
		int i =0;
		while (i<3){
			Set<String> values = new HashSet<String>();
			
			
			if (i==0){
				values.add("Value 1");
				values.add("Value 2");
				values.add("Value 3");
				values.add("Value 4");
				System.out.println(values);
				hm.put(99999, values);
			}
			else if (i==1){
				values.add("Value 10");
				System.out.println(values);
				hm.put(1234, values);
			}
			else{
				values.add("Value 100");
				values.add("Value 111");
				values.add("Value 121");
				System.out.println(values);
				hm.put(88888, values);
			}
			i += 1;
		}
		
//		
//		Set<String> values1 = null ;
////		List<String> values1 = new ArrayList<String>();
//		values1.add("Value 1");
//		values1.add("Value 1");
//		values1.add("Value 3");
//		System.out.println(values1);
//		hm.put(2344, values1);
//		
//		Set<String> values11 = null ;
////		List<String> values11 = new ArrayList<String>();
//		values11.add("Value 3");
//		values11.add("Value 4");
//		values11.add("Value 5");
//		values11.add("Value 6");
//		System.out.println(values11);
//		hm.put(3241, values11);
	}
	
	public static void main(String [] args){
		
		KeyValue obj_KeyValue = new KeyValue();
		obj_KeyValue.hitme();
		System.out.print(obj_KeyValue.hm);
//		Iterator it = obj_KeyValue.hm.entrySet().iterator();
		System.out.println();
		System.out.println();
		System.out.println();
		int pp = 0;
		for (Map.Entry<Integer, Set> entryKeyVal : obj_KeyValue.hm.entrySet()) {
		    System.out.println("key=" + entryKeyVal.getKey() + ", value=" + entryKeyVal.getValue());
		    Iterator<String> it = entryKeyVal.getValue().iterator();
		     while(it.hasNext() && pp<5){
		        System.out.println(it.next());
		        pp+=1;
		     }
		     if (pp>=5){
		    	 break;
		     }
		}   // avoids a ConcurrentModificationException
	    
//		for key,value 
		Integer Intt = 20;
		StringBuilder fnlString = new StringBuilder();
		fnlString.append("My").append(",").append(Intt.toString());
		System.out.println(fnlString);

	}
	
		
}
