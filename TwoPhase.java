
// Two phase matrix multiplication in Hadoop MapReduce
// Template file for homework #1 - INF 553 - Spring 2017
// - Wensheng Wu

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

// add your import statement here if needed
// you can only import packages from java.*;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoPhase {
    
    // mapper for processing entries of matrix A
    public static class PhaseOneMapperA extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text outKey = new Text();
        private Text outVal = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            // fill in your code int matrixId = key.getFirst();
            String[] a = value.toString().split(",");
            outKey.set(a[1]);
            outVal.set("A," + a[0] + "," + a[2]);
            context.write(outKey, outVal);
        }
    }
    
    // mapper for processing entries of matrix B
    public static class PhaseOneMapperB extends Mapper<LongWritable, Text, Text, Text> {
        
        private Text outKey = new Text();
        private Text outVal = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            
            String[] b = value.toString().split(",");
            outKey.set(b[0]);
            outVal.set("B," + b[1] + "," + b[2]);
            context.write(outKey, outVal);
        }
    }
    
    public static class PhaseOneReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text outKey = new Text();
        private Text outVal = new Text();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            
            
            // fill in your code
            List<String> matA = new ArrayList<String>();
            List<String> matB = new ArrayList<String>();
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                Text next = iterator.next();
                String[] el = next.toString().split(",", 2);
                if (el[0].equalsIgnoreCase("A")) {
                    matA.add(el[1]);
                } else if (el[0].equalsIgnoreCase("B")) {
                    matB.add(el[1]);
                }
            }
            for (int i = 0; i < matA.size(); i++) {
                String[] A = matA.get(i).split(",");
                for (int j = 0; j < matB.size(); j++) {
                    String[] B = matB.get(j).split(",");
                    outKey.set(A[0] + "," + B[0]);
                    String val = Integer.toString(Integer.valueOf(A[1]) * Integer.valueOf(B[1]));
                    outVal.set(val);
                    context.write(outKey, outVal);
                }
            }
            
        }
    }
    
    public static class PhaseTwoMapper extends Mapper<Text, Text, Text, Text> {
        
        private Text outKey = new Text();
        private Text outVal = new Text();
        
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            
            outKey = key;
            outVal = value;
            context.write(outKey, outVal);
            
        }
        
    }
    
    public static class PhaseTwoReducer extends Reducer<Text, Text, Text, Text> {
        
        private Text outKey = new Text();
        private Text outVal = new Text();
        
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            
            int total = 0;
            
            
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                total += Integer.valueOf(iterator.next().toString());
            }
            outKey = key;
            outVal.set(Integer.toString(total));
            context.write(outKey, outVal);
            
        }
        
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        Job jobOne = Job.getInstance(conf, "phase one");
        
        jobOne.setJarByClass(TwoPhase.class);
        
        jobOne.setOutputKeyClass(Text.class);
        jobOne.setOutputValueClass(Text.class);
        
        jobOne.setReducerClass(PhaseOneReducer.class);
        
        MultipleInputs.addInputPath(jobOne, new Path(args[0]), TextInputFormat.class, PhaseOneMapperA.class);
        
        MultipleInputs.addInputPath(jobOne, new Path(args[1]), TextInputFormat.class, PhaseOneMapperB.class);
        
        Path tempDir = new Path("temp");
        
        FileOutputFormat.setOutputPath(jobOne, tempDir);
        jobOne.waitForCompletion(true);
        
        // job two
        Job jobTwo = Job.getInstance(conf, "phase two");
        
        jobTwo.setJarByClass(TwoPhase.class);
        
        jobTwo.setOutputKeyClass(Text.class);
        jobTwo.setOutputValueClass(Text.class);
        
        jobTwo.setMapperClass(PhaseTwoMapper.class);
        jobTwo.setReducerClass(PhaseTwoReducer.class);
        
        jobTwo.setInputFormatClass(KeyValueTextInputFormat.class);
        
        FileInputFormat.setInputPaths(jobTwo, tempDir);
        FileOutputFormat.setOutputPath(jobTwo, new Path(args[2]));
        
        jobTwo.waitForCompletion(true);
        
        FileSystem.get(conf).delete(tempDir, true);
        
    }
}
