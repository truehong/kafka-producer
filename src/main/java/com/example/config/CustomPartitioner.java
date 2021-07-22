package com.example.config;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import javax.rmi.CORBA.Util;
import java.util.List;
import java.util.Map;


/**
 * ex) partition 매칭 가능
 * return 파디션 번호
 * */
public class CustomPartitioner implements Partitioner {

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // 레코드에 메시지 키를 지정하지 않은 경우
        if(keyBytes == null)
            throw new InvalidRecordException("Need message key");

        // 판교일 경우 0번으로 저장
        if(((String)key).equals("pangyo"))
            return 0;

        // 아닐 경우 해시값을 지정하여 특정 파티션에 매칭 되도록 설정
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }

    @Override
    public void close() {

    }
}
