package other;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by hirokinaganuma on 16/10/26.
 *
 * [TODO] FIXIT
 */
public class DataLoader {

    public class Info implements java.io.Serializable {

    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Movie implements java.io.Serializable{
        private final int year;
        private final String title;

        @JsonCreator
        public Movie(@JsonProperty("year") int year,
                     @JsonProperty("title") String title) {
            this.year = year;
            this.title = title;
        }

        public int getYear() {
            return year;
        }

        public String getTitle() {
            return title;
        }
    }


    //JavaでのJSONのロード
    public static class ParseJson implements FlatMapFunction<Iterator<String>, Movie> {
        public Iterable<Movie> call(Iterator<String> lines) throws Exception {
            ArrayList<Movie> movie = new ArrayList<>();
            ObjectMapper mapper = new ObjectMapper();
            while (lines.hasNext()) {
                String line = lines.next();
                try {
                    movie.add(mapper.readValue(line, Movie.class));
                } catch (Exception e) {
                    // Skip invalid input
                }
            }
            return movie;
        }
    }


    public static void main(String[] args) throws Exception {

        AmazonDynamoDBClient client = new AmazonDynamoDBClient().withEndpoint("http://localhost:8000");
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("Movies");


        String jsonFile = readFile("bin/input/dynamodb/moviedata.json", StandardCharsets.UTF_8);
        List<Movie> movieList = deserialize(jsonFile,Movie.class);


        for (int i = 0; i< movieList.size();i++) {

            int year = movieList.get(i).year;
            String title = movieList.get(i).title;

            try {
                table.putItem(new Item()
                        .withPrimaryKey("year", year, "title", title));
                System.out.println("PutItem succeeded: " + year + " " + title);

            } catch (Exception e) {
                System.err.println("Unable to add movie: " + year + " " + title);
                System.err.println(e.getMessage());
                break;
            }
        }
    }

    public static <T extends Object> List<T> deserialize(final String jsonStr, final Class<T> clazz)throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        final CollectionType jt = mapper.getTypeFactory().constructCollectionType(List.class, clazz);
        return (List<T>) mapper.readValue(jsonStr, jt);
    }


    static String readFile(String path, Charset encoding) throws IOException {
        byte[] encoded = Files.readAllBytes(Paths.get(path));
        return new String(encoded, encoding);
    }
}


