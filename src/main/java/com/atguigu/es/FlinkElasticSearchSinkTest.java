package com.atguigu.es;


import jdk.nashorn.internal.ir.RuntimeNode;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FlinkElasticSearchSinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = streamExecutionEnvironment.socketTextStream("localhost", 9999);

        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("localhost", 9200, "http"));
        ElasticsearchSink.Builder<String> esBuilder = new ElasticsearchSink.Builder(hosts,
                new ElasticsearchSinkFunction<String>() {

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {

                        Map<String, String> jsonMap = new HashMap<>();
                        jsonMap.put("data", element);

                        IndexRequest indexRequest = Requests.indexRequest();
                        indexRequest.index("flink-index");
                        indexRequest.id("9001");
                        indexRequest.source(jsonMap);

                        indexer.add(indexRequest);

                    }


                });


        esBuilder.setBulkFlushMaxActions(1);
        source.addSink(esBuilder.build());

        streamExecutionEnvironment.execute("flink-es");
    }
}
