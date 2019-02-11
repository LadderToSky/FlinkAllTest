package com.InkBamboo.Test;

/**
 * Created By InkBamboo
 * Date: 2019/1/21 9:51
 * Calm Positive
 * Think Then Ask
 */
public class wordcount{
    String colOne;
    int colTwo;
    String colThree;
    String colFour;

    /**
     * 必须要此默认构造方法，否则报错
     * class com.InkBamboo.Test.wordcount is missing a default constructor so it cannot be used as a POJO type and must be processed as GenericType        -
     */
    public wordcount(){}

    public wordcount(String colOne, int colTwo, String colThree, String colFour) {
        this.colOne = colOne;
        this.colTwo = colTwo;
        this.colThree = colThree;
        this.colFour = colFour;
    }

    public String getColOne() {
        return colOne;
    }

    public void setColOne(String colOne) {
        this.colOne = colOne;
    }

    public int getColTwo() {
        return colTwo;
    }

    public void setColTwo(int colTwo) {
        this.colTwo = colTwo;
    }

    public String getColThree() {
        return colThree;
    }

    public void setColThree(String colThree) {
        this.colThree = colThree;
    }

    public String getColFour() {
        return colFour;
    }

    public void setColFour(String colFour) {
        this.colFour = colFour;
    }

    @Override
    public String toString() {
        return "wordcount{" +
                "colOne='" + colOne + '\'' +
                ", colTwo='" + colTwo + '\'' +
                ", colThree='" + colThree + '\'' +
                ", colFour='" + colFour + '\'' +
                '}';
    }
}
