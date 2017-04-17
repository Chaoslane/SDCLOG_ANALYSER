package com.udbac.test;

import com.udbac.hadoop.common.LogParseException;
import org.apache.commons.lang.StringUtils;
import org.mortbay.util.MultiMap;
import org.mortbay.util.UrlEncoded;

import java.net.MalformedURLException;

/**
 * Created by root on 2017/3/31.
 */
public class SplitTest {
    public static void main(String[] args) throws LogParseException, MalformedURLException {
        String query = "https://m.baidu.com/baidu.php?sc.6zsK00KEfk_2yghcC0KJdU017ogXy3yd5bGGwwPPTPy4QK9Lj_tO6evox9G-2ULNURbaQj5FGm4nyjy5ClYpFmDZssK0fD4z-DwtA3_MoI98h9AJOcv7axfR78K-98cUBEIvn69emDvwxg7zJLIT49DWwUXF23Fv2BiCp78piWtou2LC2f.Db_arozo__vI5FKZNKsY2oCrKMZKsIVvWwKLOUOftC4Cph2SEexlZ-G94THZ3SIxHu3vI5QX1BsTEO6SgQSLOs7BmIWWo_oL4Q23N9h9mo__zU-0.U1Yk0ZDqzXBtkVXsYPePS0KY5UOP151g_PjX0A-V5HcznsKM5gI1ThI8Tv60Iybq0ZKGujYz0APGujY1rHb0UgfqnH0kPdtknjD4g1nvnj7xn101n0KopHYs0ZFY5HmYPfK-pyfqnH0zPHTkg1DkPjRdn-tYnjIxnHDYP16zg1DsPj63P-tknHRzn1-xnH0snjbLg1DkPjmvn-tknjT1nHNxnHDdnHn3g1DkPHnkPdtknHRzPjNxnHDdnH04g1DkPHc3n-tknHRsPjPxnHDYP1b30AFG5HcsP-tznj0sn-tznj01nfKVm1YYPWm4Pj6kPW7xPjmLPjnkPH64g1KxnHmzrjcsnW6zPfKkTA-b5H00TyPGujYs0ZFMIA7M5H00ULu_5fK9mWYsg100ugFM5H00TZ0qn0K8IM0qna3snj0snj0sn0KVIZ0qn0KbuAqs5H00ThCqn0KbugmqIv-1ufKhIjYz0ZKC5HRkP1nd0Aq15Hc0mMTqP0K8IjYk0ZPl5Hnzn7tknj0k0ZwdT1YvnWm3PWTkPWRLnH6vrjbvrjc10ZF-TgfqnHf4nHTvPjfYPWcznfK1pyfqmHfkPAP9PW79uAfsnhm3mfKWTvYqPDf1rjfdfRFDPRwKnWTLP0K9m1Yk0ZK85H00TydY5H00Tyd15H00XMfqn0KVmdqhThqV5HKxn7tsg100uA78IyF-gLK_my4GuZnqn7tsg1Kxn0KbIA-b5H00ugwGujYVnfK9TLKWm1Ys0ZNspy4Wm1Ys0Z7VuWYs0AuWIgfqn0KhXh6qn0Khmgfqn0KlTAkdT1Ys0A7buhk9u1Yk0APzm1YznHR3n6&qid=a414ca61add02f8a&sourceid=111&placeid=1&rank=1&shh=m.baidu.com&word=%E8%AE%BE%E8%AE%A1%E8%A3%85%E4%BF%AE%E7%BD%91&sht=1019201c&ck=1372.71.339.127.360.258.0.0.904.339.127";
        System.out.println(query.split("\\?",2)[0]);
    }

}
