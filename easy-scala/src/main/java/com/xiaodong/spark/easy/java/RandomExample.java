package com.xiaodong.spark.easy.java;

import java.util.Random;

/**
 * Created by lixiaodong on 16/9/28.
 */
public class RandomExample {
    public static void main(String[] args) {
        System.out.println(pi(20000000));
        System.out.println(piPositive(20000000));
    }

    private static double piPositive(int slices) {
        double count = 0;
        Random random = new Random();
        for (int i=0; i<slices; i++) {
            double x = random.nextDouble();
            double y = random.nextDouble();
            if (x*x + y*y < 1) {
                count ++;
            }
        }
        return 4.0 * count/slices;
    }

    private static double pi(int slices) {
        double count = 0;
        Random random = new Random();
        for (int i=0; i<slices; i++) {
            double x = random.nextDouble() * 2.0 - 1;
            double y = random.nextDouble() * 2.0 - 1;
            if (x*x + y*y < 1) {
                count ++;
            }
        }
        return 4.0 * count/slices;
    }
}
