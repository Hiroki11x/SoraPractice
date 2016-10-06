package bigdecimal;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigDecimal;
import java.util.Arrays;

/**
 * Created by hirokinaganuma on 2016/10/06.
 */
public class BDSample02 {
    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(
                "local", "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));

        JavaRDD<BigDecimal> input = sc.parallelize(Arrays.asList(new BigDecimal("2.0"),new BigDecimal("3.0")));
        input.cache();

        BigDecimal add = input.reduce((a, b) -> (a.add(b)));
        System.out.println(add);

        BigDecimal subtract = input.reduce((a, b) -> (a.subtract(b)));
        System.out.println(subtract);

        BigDecimal multiply = input.reduce((a, b) -> (a.multiply(b)));
        System.out.println(multiply);

        BigDecimal divide = input.reduce((a, b) -> (a.divide(b,10,BigDecimal.ROUND_HALF_DOWN)));
        System.out.println(divide);
    }
}
