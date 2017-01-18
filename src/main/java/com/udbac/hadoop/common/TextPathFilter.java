//package com.udbac.hadoop.common;
//
//import com.udbac.hadoop.util.TimeUtil;
//import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.fs.PathFilter;
//
//import java.io.IOException;
//
///**
// * Created by root on 2016/8/17.
// */
//public class TextPathFilter extends Configured implements PathFilter {
//    @Override
//    public boolean accept(Path path) {
//        String date1 = getConf().get(SDCLogConstants.RUNNING_DATE_PARAMES);
//        FileSystem fs = null;
//        try {
//            fs = FileSystem.get(getConf());
//            FileStatus fileStatus = fs.getFileStatus(path);
//            if (fileStatus.isDirectory()) {
//                String name = path.getName();
//                if (TimeUtil.isValidateRunningDate(name)) {//文件夹名字 yyyyMMdd
//                    if (name.equals(date1)) {//目录名字是否为前一天日期 yyyyMMdd
//                        return true;
//                    }
//                }
//            }
//            if (fileStatus.isFile()) {
//                return true;
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        } finally {
//            if (fs != null) {
//                try {
//                    fs.close();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//        return false;
//    }
//}
