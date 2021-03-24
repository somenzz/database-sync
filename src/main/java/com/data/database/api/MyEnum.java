package com.data.database.api;

public class MyEnum {
    public enum ColSizeTimes{

        EQUAL(1),DOUBLE(2);
        private int times = 0;
        // 定义一个带参数的构造器，枚举类的构造器只能使用 private 修饰
        private ColSizeTimes(int times) {
            this.times = times;
        }
        public int getTimes(){
            return times;
        }
    };

}