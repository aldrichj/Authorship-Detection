import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

/**
 *
 * Summation the author and calculate their parts of the denominator
 * Created by jeremy on 4/1/16.
 */
public class CalcReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values,
                       Context context)
            throws IOException, InterruptedException {
        double summedNumerator = 0.0;
        double summedAuthor = 0.0;
        boolean first = false;

        ArrayList<String> data = new ArrayList<>();

        // (author),(word,TFIDF,numerator)
        for(Text value:values){
            String[] line = value.toString().split("#");

            String word = line[0];
            String TFIDF = line[1];
            String numerator = line[2];

            summedNumerator += Double.parseDouble(numerator);
            summedAuthor += Math.pow(Double.parseDouble(TFIDF),2);


            data.add(word);
        }


        double authorSquareRoot = Math.sqrt(summedAuthor);


        for (String word:data){


            // (word,author),(Summed numerator,rooted author denominator)
            context.write(new Text(word+"#"+key),new Text("#"+summedNumerator+"#"+authorSquareRoot));

        }




    }
}
