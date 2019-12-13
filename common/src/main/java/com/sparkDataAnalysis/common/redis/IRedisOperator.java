package com.sparkDataAnalysis.common.redis;

import redis.clients.jedis.JedisCluster;

import java.util.TreeSet;

/**
 * @author xiaomei.wang
 * @version 1.0
 * @date 2019/8/12 13:36
 */
public interface IRedisOperator {

    /**
     * 根据pattern 获取所有的keys
     *
     * @param pattern
     * @return
     */
    TreeSet<String> keys(String pattern, JedisCluster jedisCluster);
}