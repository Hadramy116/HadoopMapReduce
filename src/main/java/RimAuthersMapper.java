import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.file.tfile.TFile;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Scanner;

public class RimAuthersMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text author = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        Scanner line = new Scanner(value.toString());
        line.useDelimiter("->");
        author.set(line.next());
        context.write(author, one);
    }
}
