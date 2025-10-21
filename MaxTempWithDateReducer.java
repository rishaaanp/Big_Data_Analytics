import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MaxTempWithDateReducer extends Reducer<Text, Text, Text, Text> {

    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int maxTemp = Integer.MIN_VALUE;
        String maxTempDate = "";

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 2) {
                String date = parts[0];
                int temp = Integer.parseInt(parts[1]);

                if (temp > maxTemp) {
                    maxTemp = temp;
                    maxTempDate = date;
                }
            }
        }

        result.set(maxTempDate + "\t" + maxTemp);
        context.write(key, result);
    }
}

