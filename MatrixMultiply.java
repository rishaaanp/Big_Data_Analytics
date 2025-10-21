import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MatrixMultiply {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length != 4) return;

            String matrixName = parts[0];
            int i = Integer.parseInt(parts[1]);
            int j = Integer.parseInt(parts[2]);
            int val = Integer.parseInt(parts[3]);

            Configuration conf = context.getConfiguration();
            int n = Integer.parseInt(conf.get("matrix.size")); // shared dimension

            if (matrixName.equals("A")) {
                for (int k = 0; k < n; k++) {
                    context.write(new Text(i + "," + k), new Text("A," + j + "," + val));
                }
            } else if (matrixName.equals("B")) {
                for (int k = 0; k < n; k++) {
                    context.write(new Text(k + "," + j), new Text("B," + i + "," + val));
                }
            }
        }
    }

    public static class MatrixReducer extends Reducer<Text, Text, Text, IntWritable> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<Integer, Integer> aMap = new HashMap<>();
            Map<Integer, Integer> bMap = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String matrixName = parts[0];
                int index = Integer.parseInt(parts[1]);
                int value = Integer.parseInt(parts[2]);

                if (matrixName.equals("A")) {
                    aMap.put(index, value);
                } else if (matrixName.equals("B")) {
                    bMap.put(index, value);
                }
            }

            int result = 0;
            for (int k : aMap.keySet()) {
                if (bMap.containsKey(k)) {
                    result += aMap.get(k) * bMap.get(k);
                }
            }

            context.write(key, new IntWritable(result));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: MatrixMultiply <input path> <output path> <shared dimension>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("matrix.size", args[2]);

        Job job = Job.getInstance(conf, "Matrix Multiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
