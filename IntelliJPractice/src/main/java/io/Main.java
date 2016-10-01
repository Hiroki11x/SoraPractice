package io;

/**
 * Created by hirokinaganuma on 2016/09/30.
 */
import static spark.Spark.*;

public class Main {
    public static void main(String[] args) {
        get("/hello", (req, res) -> "Hello World");
    }
}