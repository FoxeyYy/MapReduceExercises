import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	
	

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  Map<String, Integer> documents = new TreeMap<>();
	  	  
	  for (Text val : values) {

		  StringTokenizer itr = new StringTokenizer(val.toString(), ", ");
		  while (itr.hasMoreTokens()) {

			  String token = itr.nextToken();


			  String[] splits = token.split(":");
			  String doc = splits[0];

			  Integer numOcc = documents.get(doc);
			  if (numOcc == null) {
				  numOcc = 0;
			  }

			  documents.put(doc, numOcc + Integer.valueOf(splits[1]));
		  }

	  }
      
      StringBuilder result = new StringBuilder();
      
      Iterator<String> documentsIterator = documents.keySet().iterator();
      
      while (documentsIterator.hasNext()) {
    	  String doc = documentsIterator.next();
    	  result.append(doc).append(":").append(documents.get(doc));
    	  if (documentsIterator.hasNext()) {
    		  result.append(", ");
    	  }
      }
      
      context.write(key, new Text(result.toString()));
      
    }

}