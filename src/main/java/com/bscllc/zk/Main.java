package com.bscllc.zk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ValueNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.CharBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

    /**
     * Transform the JSON string to YAML.
     * Not used.
     * @param jsonString
     * @return
     * @throws JsonProcessingException
     * @throws IOException
     */
    public static String asYaml(String jsonString) throws JsonProcessingException, IOException {
        // parse JSON
        JsonNode jsonNodeTree = new ObjectMapper().readTree(jsonString);
        // save it as YAML
        YAMLMapper ym = new YAMLMapper();

        String jsonAsYaml = ym.writeValueAsString(jsonNodeTree);
        return jsonAsYaml;
    }

    /**
     * This is used to flatten the JSON tree
     * @param currentPath The path
     * @param jsonNode The JSON node
     * @param map Map to add keys and values
     */
    public static  void addKeys(String currentPath, JsonNode jsonNode, Map<String, String> map) {
        if (jsonNode.isObject()) {
            ObjectNode objectNode = (ObjectNode) jsonNode;
            Iterator<Map.Entry<String, JsonNode>> iter = objectNode.fields();
//            String pathPrefix = currentPath.isEmpty() ? "" : currentPath + ".";
            String pathPrefix = currentPath.isEmpty() ? "" : currentPath + "/";

            while (iter.hasNext()) {
                Map.Entry<String, JsonNode> entry = iter.next();
                addKeys(pathPrefix + entry.getKey(), entry.getValue(), map);
            }
        } else if (jsonNode.isArray()) {
            ArrayNode arrayNode = (ArrayNode) jsonNode;
            for (int i = 0; i < arrayNode.size(); i++) {
                addKeys(currentPath + "[" + i + "]", arrayNode.get(i), map);
            }
        } else if (jsonNode.isValueNode()) {
            ValueNode valueNode = (ValueNode) jsonNode;
            map.put(currentPath, valueNode.asText());
        }
    }

    public static void main(String ...args) {

        try {
            FileInputStream fis = new FileInputStream(args[0]);

            FileChannel channel = fis.getChannel();

            Charset charset = Charset.forName("UTF-8");

            MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, channel.size());

            CharBuffer cb = charset.decode(buffer);
            channel.close();

            JsonNode jsonNodeTree = new ObjectMapper().readTree(cb.toString());

//            String yaml = asYaml(cb.toString());

            HashMap<String, String>map = new HashMap<>();

            // start it all off with /edwin root
            addKeys("/edwin", jsonNodeTree, map);

            channel.close();

            RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
            CuratorFramework client = CuratorFrameworkFactory.newClient("localhost:2181", retryPolicy);
            client.start();

            LOGGER.info("Connected to Zookeeper");

            for (String key : map.keySet()) {
                LOGGER.info(String.format("Key: '%s'. Value: '%s'", key, map.get(key)));

                // insert into Zookeeper tree
                client.create()
                        .creatingParentsIfNeeded()
                        .forPath(key, map.get(key).getBytes());
            }

            TimeUnit.SECONDS.sleep(30);

            LOGGER.info("Closing connection to Zookeeper");
            client.close();
        }
        catch(Exception exp) {
            exp.printStackTrace();

            LOGGER.error("Failure somewhere", exp);
        }
    }
}
