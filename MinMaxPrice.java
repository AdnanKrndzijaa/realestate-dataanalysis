import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TreeMap;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxMinPricePerCity {
    public static class RealEstateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable price = new DoubleWritable();
        private Text city = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 9) {
                String cityValue = parts[4].trim();
                double priceValue = Double.parseDouble(parts[9].trim());
                price.set(priceValue);
                context.write(city, price);
            }
        }
    }

    public static class RealEstateReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        private TreeMap<Double, Text> topCities = new TreeMap<>();
        private TreeMap<Double, Text> bottomCities = new TreeMap<>();
        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double maxPrice = Double.MIN_VALUE;
            double minPrice = Double.MAX_VALUE;
            for (DoubleWritable value : values) {
                double price = value.get();
                maxPrice = Math.max(maxPrice, price);
                minPrice = Math.min(minPrice, price);
            }
            String result = "Max Price: " + maxPrice + ", Min Price: " + minPrice;
            topCities.put(maxPrice, new Text(result));
            bottomCities.put(minPrice, new Text(result));
            if (topCities.size() > 1) {
                topCities.remove(topCities.firstKey());
            }
            if (bottomCities.size() > 1) {
                bottomCities.remove(bottomCities.lastKey());
            }
        }
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(NullWritable.get(), new Text("Top 1 City - Max Price:"));
            for (Text cityInfo : topCities.descendingMap().values()) {
                context.write(NullWritable.get(), cityInfo);
            }
            context.write(NullWritable.get(), new Text("Bottom 1 City - Min Price:"));
            for (Text cityInfo : bottomCities.values()) {
                context.write(NullWritable.get(), cityInfo);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Max Min Price Per City");
        job.setJarByClass(MaxMinPricePerCity.class);
        job.setMapperClass(RealEstateMapper.class);
        job.setReducerClass(RealEstateReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
