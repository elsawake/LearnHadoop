import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.*;

public class WordCount {

    abstract class WordCountMapper extends MapReduceBase implements
            Mapper<LongWritable, Text, OutputCollector, Reporter> {
        public void map(LongWritable key, Text value,
                        OutputCollector<Text, IntWritable> output,
                        Reporter reporter) throws IOException {
            // Convert input to string with all lower case
            String[] wordArray = value.toString().toLowerCase().split("\\s+"); // Split by empty space
            for (String word:wordArray) {
                output.collect(new Text(word), new IntWritable(1));
            }
        }
    }

    abstract class WordCountReducer extends MapReduceBase implements
            Reducer<Text, Iterator, OutputCollector, Reporter> {
        public void reduce(Text key, Iterator<IntWritable> value,
                           OutputCollector<Text, IntWritable> output,
                           Reporter reporter) throws IOException {

            int sum = 0;
            while (value.hasNext()) {
                sum += value.get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("Usage: wordCount <input path> <output path>");
            System.exit(-1);
        }
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("Count the word");
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setMapperClass(WordCountMapper.class);
        conf.setReducerClass(WordCountReducer.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        JobClient.runJob(conf);
    }
}
