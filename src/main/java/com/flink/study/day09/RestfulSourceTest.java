package com.flink.study.day09;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author humingming
 * @date 2024/1/23 21:22
 */
public class RestfulSourceTest {


    public static class RestfulSource implements SourceFunction<String> {

        private String url;

        public RestfulSource(String url) {
            this.url = url;
        }


        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            URL url = new URL(this.url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                InputStream inputStream = connection.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
                String line;
//                while ((line = bufferedReader.readLine()) != null) {
//                    ctx.collect(line);
//                }
                line = bufferedReader.readLine();
                //{"results":[{"list":[{"gameName":"aaa"},{"gameName":"bbb"}]}]}
                //先声明一个objectMapper
                ObjectMapper objectMapper = new ObjectMapper();
                //将json读取成树状结构
                JsonNode jsonNode = objectMapper.readTree(line);
                // 先获取第一层的results节点，因为results的值是一个数组，所以还需要往下进行循环拆分
                JsonNode jn_1 = jsonNode.get("results");
                if (jn_1!=null && jn_1.isArray()){
                    //循环遍历第一层results数组里面的所有内容
                    for (JsonNode node1:jn_1){
                        JsonNode jn_2 = node1.get("list");
                        //取出的list节点还是一个数组的结构，所以还需要往下继续拆分
                        if (jn_2!=null && jn_2.isArray()){
                            //循环遍历第二层list数组里面所有的内容
                            for (JsonNode node2:jn_2){
                                String gameName = node2.get("gameName").asText();
                                String startTime = node2.get("startTime").asText();
                                String statusDesc = node2.get("statusDesc").asText();
                                ctx.collect(gameName+"："+startTime+"："+statusDesc);
                            }
                        }
                    }
                }


                inputStreamReader.close();
                bufferedReader.close();
            }
            // 一行代码解决
            //  ctx.collect(HttpUtil.get(this.url));

        }

        @Override
        public void cancel() {
            // 这里是定义取消数据的捕获，没有就不写
        }
    }



    public static void func() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String url = "https://cbs-i.sports.cctv.com/cache/0fe461738f548ffb6227a83776895fad?ran=1706015977071";
        DataStreamSource<String> source = env.addSource(new RestfulSource(url));
        source.print();
        env.execute();


    }


    public static void main(String[] args) throws Exception {
        func();
    }


}
