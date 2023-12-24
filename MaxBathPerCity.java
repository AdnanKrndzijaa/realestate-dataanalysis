import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxBathPerCity {
    public static class RealEstateMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text city = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 4) {
                String cityValue = parts[4].trim();  
                String bathValue = parts[2].trim(); 

                if (!bathValue.isEmpty()) {
                    try {
                        int bathCount = Integer.parseInt(bathValue);
                        city.set(cityValue);
                        context.write(city, new IntWritable(bathCount));
                    } catch (NumberFormatException e) {
                    }
                }        
            }
        }
    }

    public static class RealEstateReducer extends Reducer<Text, IntWritable, NullWritable, Text> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int maxBathCount = Integer.MIN_VALUE;
            for (IntWritable value : values) {
                maxBathCount = Math.max(maxBathCount, value.get());
            }
            String result = "City: " + key.toString() + ", Max Bath Count: " + maxBathCount;
            context.write(NullWritable.get(), new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Bath Count Per City");
        job.setJarByClass(MaxBathPerCity.class);
        job.setMapperClass(RealEstateMapper.class);
        job.setReducerClass(RealEstateReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
