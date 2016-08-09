package test;

import com.udbac.hadoop.common.SDCLogConstants;
import com.udbac.hadoop.entity.AnalysedLog;
import com.udbac.hadoop.util.JdbcUtils;
import com.udbac.hadoop.util.TimeUtil;
import jodd.util.StringUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.sql.Statement;
import java.util.*;
import java.util.zip.CRC32;

/**
 * Created by root on 2016/7/25.
 */
public class TestHive {

    public static void main(String[] args) {
        try {
            Statement stmt = JdbcUtils.getConnection().createStatement();
            String tableName = "test";
            stmt.execute("drop table if exists " + tableName);
            stmt.execute("create table " + tableName + " (\n" +
                    "visitId STRING,\n" +
                    "deviceId STRING,\n" +
                    "time STRING,\n" +
                    "date DATE,\n" +
                    "cIp  STRING,\n" +
                    "csUserAgent STRING,\n" +
                    "utmSource STRING,\n" +
                    "uType STRING,\n" +
                    "wtAvv STRING,\n" +
                    "wtPos STRING,\n" +
                    "login int,\n" +
                    "menu int,\n" +
                    "user int,\n" +
                    "cart int,\n" +
                    "suc int,\n" +
                    "pay int,\n" +
                    "duration STRING,\n" +
                    "pageView STRING\n" +
                    ")" +
                    "PARTITIONED BY(YEAR INT ,MONTH INT ,DATE INT )" +
                    "ROW ROWFORMAT DELIMITED FIELDS TERMINATED BY '|' STORED AS TEXTFILE;");
            // show tables
            String sql = "show tables '" + tableName + "'";
            System.out.println("Running: " + sql);
            ResultSet res = stmt.executeQuery(sql);
            if (res.next()) {
                System.out.println(res.getString(1));
            }
            // describe table
            sql = "describe " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1) + "\t" + res.getString(2));
            }

            // load data into table
            // NOTE: filepath has to be local to the hive server
            // NOTE: /tmp/a.txt is a ctrl-A separated file with two fields per line
            String filepath = "/user/mr/output/out/part-r-00000";
            sql = "load data inpath '" + filepath + "' into table " + tableName;
            System.out.println("Running: " + sql);
            stmt.execute(sql);

            // select * query
//               sql = "select * from " + tableName +" limit 10";
//               System.out.println("Running: " + sql);
//               res = stmt.executeQuery(sql);
//               while (res.next()) {
//                   System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
//               }

            // regular hive query
            sql = "select count(1) from " + tableName;
            System.out.println("Running: " + sql);
            res = stmt.executeQuery(sql);
            while (res.next()) {
                System.out.println(res.getString(1));
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
        }
    }

}
