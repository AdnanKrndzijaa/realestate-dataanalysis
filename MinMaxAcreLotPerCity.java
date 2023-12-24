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

public class MinMaxAcreLotPerCity {
    public static class RealEstateMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable acreLot = new DoubleWritable();
        private Text city = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 5) {
                String cityValue = parts[4].trim();
                String acreLotStr = parts[3].trim();
                try {
                    double acreLotValue = Double.parseDouble(acreLotStr);
                    city.set(cityValue);
                    acreLot.set(acreLotValue);
                    context.write(city, acreLot);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing acre lot value for city " + cityValue + ": " + e.getMessage());
                }
            }
        }
    }

    public static class RealEstateReducer extends Reducer<Text, DoubleWritable, NullWritable, Text> {
        private TreeMap<Double, Text> topCities = new TreeMap<>();

        @Override
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
                throws IOException, InterruptedException {
            double maxAcreLot = Double.MIN_VALUE;
            double minAcreLot = Double.MAX_VALUE;
            for (DoubleWritable value : values) {
                double acreLot = value.get();
                maxAcreLot = Math.max(maxAcreLot, acreLot);
                minAcreLot = Math.min(minAcreLot, acreLot);
            }
            String result = "City: " + key.toString() + ", Max Acre Lot: " + maxAcreLot + ", Min Acre Lot: " + minAcreLot;
            topCities.put(maxAcreLot, new Text(result));
            if (topCities.size() > 10) {
                topCities.remove(topCities.firstKey());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Text cityInfo : topCities.descendingMap().values()) {
                context.write(NullWritable.get(), cityInfo);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top 10 Max Min Acre Lot Per City");
        job.setJarByClass(MinMaxAcreLotPerCity.class);
        job.setMapperClass(RealEstateMapper.class);
        job.setReducerClass(RealEstateReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("UlaznaPutanja"));
        FileOutputFormat.setOutputPath(job, new Path("IzlaznaPutanja"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
