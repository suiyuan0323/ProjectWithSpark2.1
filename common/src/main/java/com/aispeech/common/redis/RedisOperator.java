package com.aispeech.common.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author xiaomei.wang
 * @version 1.0
 * @date 2019/8/12 13:35
 */
public class RedisOperator implements IRedisOperator {

    public final static Logger logger = LoggerFactory.getLogger(RedisOperator.class);

    // 从每一个cluster的nodes中找到匹配的key
    public TreeSet<String> keys(String pattern, JedisCluster jedisCluster) {
        logger.debug("Start getting keys...");
        TreeSet<String> keys = new TreeSet<>(Comparator.naturalOrder());
        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            logger.debug("Getting keys from: {}", k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys(pattern));
            } catch (Exception e) {
                logger.error("Getting keys error: {}", e);
            } finally {
                logger.debug("Connection closed.");
                connection.close();//用完一定要close这个链接！！！
            }
        }
        logger.debug("Keys gotten!");
        return keys;
    }

    public TreeSet<String> hashSlotkeys(String pattern, JedisCluster jedisCluster) {
        TreeSet<String> keys = new TreeSet<>(Comparator.naturalOrder());

        ScanParams scanParams = new ScanParams();

        Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
        for (String k : clusterNodes.keySet()) {
            logger.debug("Getting keys from: {}", k);
            JedisPool jp = clusterNodes.get(k);
            Jedis connection = jp.getResource();
            try {
                keys.addAll(connection.keys(pattern));
            } catch (Exception e) {
                logger.error("Getting keys error: {}", e);
            } finally {
                logger.debug("Connection closed.");
                connection.close();//用完一定要close这个链接！！！
            }
        }
        logger.debug("Keys gotten!");
        return keys;
    }

}