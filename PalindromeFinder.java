import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.List;
import java.util.Arrays;
import java.util.regex.Pattern;


public final class PalindromeFinder {
    private static final Pattern SPACE = Pattern.compile("[\\p{Punct}\\s]+");

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("JavaWordCount")
              // .config("spark.master", "local")
                .getOrCreate();

        //path to file(s) you want to search for Palindromes
        String path = ".../*/*";

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        JavaRDD<String> words = lines.flatMap(s -> Arrays.asList(SPACE.split(s)).iterator());
        //test for palindromes

        JavaPairRDD<String, Integer> palindromes = words.mapToPair(s -> new Tuple2<>(s, 1)).
                filter(new Function<Tuple2<String, Integer>, Boolean>() {

                           public Boolean call(Tuple2<String, Integer> tuple) {
                               String line = tuple._1;
                               line = line.replaceAll("\\s", "").toLowerCase();
                               if (line.equals("") || line.length() == 1 || line.chars().allMatch( Character::isDigit )) {
                                   return false;
                               }
                               return line.equals(new StringBuilder(line).reverse().toString());
                           }
                       }
                );

        JavaPairRDD<String, Integer> palindrome = palindromes.reduceByKey((a, b) -> a + b);

        List<Tuple2<String, Integer>> output = palindrome.collect();

        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1());
        }
        spark.stop();
    }
}