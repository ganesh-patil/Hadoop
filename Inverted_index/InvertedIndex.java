import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndex {


//mapper class 
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

   // private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text fileName = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

      String inputFileName = ((FileSplit) context.getInputSplit()).getPath().getName();
     // String inputSource=context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir", null);
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
	fileName.set(inputFileName);
        context.write(word, fileName);
      }
    }
  }

//reducer class 
  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //String s  = new String();
	List<String> s = new ArrayList<String>();
	String listString = "";
      for(Text val : values ) {
	if(!s.contains(val.toString())) {
		s.add(val.toString());
    		listString += val.toString() + ",";
	}
      }

      result.set(listString);
      context.write(key, result);
    }
  }


//drivercode 
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "InvertedIndex");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
