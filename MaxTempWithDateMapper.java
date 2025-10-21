import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempWithDateMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text yearKey = new Text();
    private Text dateTempValue = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (line.startsWith("Date")) return; // skip header

        String[] parts = line.split(",");  // split by comma
        if (parts.length == 2) {
            String date = parts[0];
            String tempStr = parts[1];
            String year = date.substring(0, 4);

            yearKey.set(year);
            dateTempValue.set(date + "," + tempStr);
            context.write(yearKey, dateTempValue);
        }
    }
}

