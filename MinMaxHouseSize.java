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

public class MinMaxHouseSize {
    public static class RealEstateMapper extends Mapper<LongWritable, Text, NullWritable, DoubleWritable> {
        private DoubleWritable houseSize = new DoubleWritable();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 7) {
                try {
                    double size = Double.parseDouble(parts[7].trim());
                    houseSize.set(size);
                    context.write(NullWritable.get(), houseSize);
                } catch (NumberFormatException e) {
                }
            }
        }
    }

    public static class RealEstateReducer extends Reducer<NullWritable, DoubleWritable, Text, Text> {
        private DoubleWritable maxHouseSize = new DoubleWritable(Double.MIN_VALUE);
        private DoubleWritable minHouseSize = new DoubleWritable(Double.MAX_VALUE);
        private Text maxCity = new Text();
        private Text minCity = new Text();
        @Override
        public void reduce(NullWritable key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            for (DoubleWritable value : values) {
                double size = value.get();
                if (size > maxHouseSize.get()) {
                    maxHouseSize.set(size);
                    maxCity.set("City: " + context.getConfiguration().get("maxCity")); 
                }
                if (size < minHouseSize.get()) {
                    minHouseSize.set(size);
                    minCity.set("City: " + context.getConfiguration().get("minCity"));
                }
            }
            String maxResult = "Max House Size: " + maxHouseSize.toString() + ", " + maxCity.toString();
            String minResult = "Min House Size: " + minHouseSize.toString() + ", " + minCity.toString();
        
            context.write(new Text("Max"), new Text(maxResult));
            context.write(new Text("Min"), new Text(minResult));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Min House Size");
        job.setJarByClass(MinMaxHouseSize.class);
        job.setMapperClass(RealEstateMapper.class);
        job.setReducerClass(RealEstateReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
