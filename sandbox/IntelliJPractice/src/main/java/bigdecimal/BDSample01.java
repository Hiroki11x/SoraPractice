package bigdecimal;

import org.apache.spark.api.java.JavaSparkContext;

import java.math.BigDecimal;

/**
 * Created by hirokinaganuma on 2016/10/06.
 */
public class BDSample01 {

    public static void calc(){
        // BigDecimalの生成
        BigDecimal one = new BigDecimal("1.00");
        BigDecimal two = new BigDecimal("2.00");
        BigDecimal three = new BigDecimal("3.00");

        // 足し算 ( 1.0 + 2.0 )
        BigDecimal add = one.add(two);

        // 引き算 ( 1.0 - 2.0）
        BigDecimal subtract = one.subtract(two);

        // 掛け算 ( 2.0 * 3.0 )
        BigDecimal multiply = two.multiply(three);

        // 割り算 ( 1.0 / 3.0 少数第３位で四捨五入する )
        BigDecimal divide = one.divide(three, 2, BigDecimal.ROUND_HALF_UP);

        System.out.println("足し算: " + add);
        System.out.println("引き算: " + subtract);
        System.out.println("掛け算: " + multiply);
        System.out.println("割り算: " + divide);
    }



    public static void main(String[] args) throws Exception {
        JavaSparkContext sc = new JavaSparkContext(
                "local", "basicloadjson", System.getenv("SPARK_HOME"), System.getenv("JARS"));
        calc();
    }

}
