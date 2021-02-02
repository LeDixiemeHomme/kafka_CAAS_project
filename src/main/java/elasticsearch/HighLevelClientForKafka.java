package elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class HighLevelClientForKafka {
    RestHighLevelClient restHighLevelClient;

    public HighLevelClientForKafka(int elasticPort, String elasticHost){
        this.restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(elasticHost, elasticPort, "http")));
    }

    public static RestHighLevelClient restHighLevelClient(int elasticPort, String elasticHost) {
        return new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(elasticHost, elasticPort, "http")));
    }

    public void pushKafkaMessageToElasticIndex(String index, int id, String message) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        {
            builder.field("message", message);
        }
        builder.endObject();

        IndexRequest indexRequest = new IndexRequest(index).id(String.valueOf(id)).source(builder);

        IndexResponse indexResponse = this.restHighLevelClient
                .index(indexRequest, RequestOptions.DEFAULT);

        System.out.println(indexResponse);
    }

    public void createGetRequest(String index, int id) throws IOException {
        GetRequest getRequest = new GetRequest(
                index,
                String.valueOf(id));
        GetResponse getResponse = this.restHighLevelClient.get(getRequest, RequestOptions.DEFAULT);
        System.out.println(getResponse.toString());
    }

    public void createDeleteRequest(String index, int id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(
                index,
                String.valueOf(id));
        DeleteResponse deleteResponse = this.restHighLevelClient.delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteResponse.toString());
    }
}
