package com.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    /**
     * partition 메서드에는 레코드를 기반으로 파티션을 정하는 로직을 포함한다.
     * 리턴값은 주어진 레코드가 들어갈 파티션 번호이다.
     *
     * @param topic
     * @param key
     * @param keyBytes
     * @param value
     * @param valueBytes
     * @param cluster
     * @return
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        /**
         * 레코드에 메시지 키를 지정하지 않은 경우 -> 비정상적인 데이터로 간주하고 InvalidRecordException 발생 처리.
         */
        if (keyBytes == null) {
            throw new InvalidRecordException("Need message key");
        }

        /**
         * 메시지 키에 따라 레코드가 전달될 파티션을 지정한다.
         * jwpark06 -> partition 0
         * wychoi01 -> partition 1
         * smlee02 -> partition 2
         */
        String keyName = (String) key;
        if (keyName.equals("jwpark06")) {
            return 0;
        } else if (keyName.equals("wychoi01")) {
            return 1;
        } else if (keyName.equals("smlee02")) {
            return 2;
        }

        /**
         * 메시지 키가 존재하지만 "jwpark06", "wychoi01", "smlee02"가 아닌 경우
         * 해시값을 지정하여 특정 파티션에 매칭되도록 한다.
         */
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}