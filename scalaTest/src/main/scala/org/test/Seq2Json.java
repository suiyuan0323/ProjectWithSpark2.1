package org.test;

/**
 * @author xiaomei.wang
 * @version 1.0
 * @date 2019/8/30 10:00
 */
import com.alibaba.fastjson.JSON;
import java.util.List;
public class Seq2Json<E> {

    public String seq2Josn(List<E> ll) {
        String string = JSON.toJSONString(ll);
        return string;
    }

}

