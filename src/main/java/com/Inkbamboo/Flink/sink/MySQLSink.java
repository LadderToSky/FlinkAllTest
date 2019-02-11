package com.Inkbamboo.Flink.sink;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import scala.Tuple3;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * java 实现的 针对flink处理之后输出到sink的，.addsink(SinkFunction) 的针对性实现
 *
 * scala实现，只需要定义一个function 并针对性的处理DataStream中的数据。参见 streamKafaDemo
 */
public class MySQLSink extends
        RichSinkFunction<Tuple3<Integer, String, Integer>> {

    private static final long serialVersionUID = 1L;
    private Connection connection;
    private PreparedStatement preparedStatement;
    String username ="";// Config.getString("mysql.user");
    String password = "";
    String drivername ="";// Config.getString("mysql.driver");
    String dburl = ""; //Config.getString("mysql.url");

    @Override
    public void invoke(Tuple3<Integer, String, Integer> value) throws Exception {
        Class.forName(drivername);
        connection = DriverManager.getConnection(dburl, username, password);
        String sql = "replace into orders(order_id,order_no,order_price) values(?,?,?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1, value._1());
        preparedStatement.setString(2, value._2());
        preparedStatement.setInt(3, value._3());
        preparedStatement.executeUpdate();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }

    }
}
