package com.udbac.hadoop.util;

import java.sql.*;
import java.util.Properties;

/**
 * Created by root on 2016/7/28.
 */
public final class JdbcUtils {
    private static Properties ps = new Properties();
    private static final String db_setting = "/system-config.properties";

    private JdbcUtils() {
    }

    // 注册驱动
    static {
        try {
            if (JdbcUtils.class.getResourceAsStream(db_setting) == null) {
                throw new RuntimeException("acs-db.properties is not found ");
            }
            ps.load(JdbcUtils.class.getResourceAsStream(db_setting));
            Class.forName(ps.getProperty("jdbc.driver"));
        } catch (Exception e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(ps.getProperty("jdbc.url"), ps.getProperty("jdbc.username"), "");
    }

    public static void free(ResultSet rs, Statement st, Connection conn) {
        try {
            if (rs != null)
                rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (st != null)
                    st.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (conn != null)
                        conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
