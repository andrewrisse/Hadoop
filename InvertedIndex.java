import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//References:
//Code is modified from:
//https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
//Ran into this and referenced it to debug the errors I was having, tried not to copy it, but the 
//flow ended up being similar:
//https://github.com/imehrdadmahdavi/map-reduce-inverted-index/blob/master/InvertedIndex.java
//For StringBuilder: https://www.geeksforgeeks.org/stringbuilder-append-method-in-java-with-examples/
//For hashmap: https://www.geeksforgeeks.org/java-util-hashmap-in-java/

public class InvertedIndex {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{


    private Text word = new Text();
    private Text ID = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //Get the Document ID and store the rest of the string after it
      String docID = value.toString().substring(0, value.toString().indexOf("\t"));
      String afterID =  value.toString().substring(value.toString().indexOf("\t") + 1);     
      ID.set(docID);
      afterID =  afterID.replaceAll("[^a-zA-Z]", " ");//Remove all special characters
      afterID = afterID.toLowerCase();//Lower case

      StringTokenizer itr = new StringTokenizer(afterID, " ");//Space is only delimiter per discussion on Piazza     
      
   while (itr.hasMoreTokens()) {
      
        word.set(itr.nextToken());
        context.write(word, ID);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {

      HashMap<String, Integer> myMap = new HashMap<String, Integer>();
      for (Text v : values) {
         if(myMap.containsKey(v.toString())){
             myMap.put(v.toString(),myMap.get(v.toString())+1);//Add one more count at that keys doc ID
        }
        else{//first time this docID and key have been associated
             myMap.put(v.toString(),1);
       }
      } 
       StringBuilder docIdCounts = new StringBuilder(); //append all the docID:# strings together for the single key

       //Create a long string of docIDs followed by their count
       for(String documentID : myMap.keySet()){
          docIdCounts.append(documentID + ":" + myMap.get(documentID) + " "); 
       }

       Text finalList = new Text(docIdCounts.toString());
       context.write(key, finalList);//Write the word followed by the list of docID:#
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "inverted index");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
