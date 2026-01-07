package cn.thinkingdata.analytics.request;

import cn.thinkingdata.analytics.exception.IllegalDataException;
import cn.thinkingdata.analytics.util.TDLogger;
import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class TDDebugRequest extends TDBaseRequest {
    private Boolean writeData = true;
    private String deviceId;
    public TDDebugRequest(URI server_uri, String appId, Integer timeout) {
        super(server_uri, appId, timeout);
    }

    public TDDebugRequest(URI server_uri, String appId, boolean writeData, String deviceId) {
        super(server_uri, appId);
        this.writeData = writeData;
        this.deviceId = deviceId;
    }

    public TDDebugRequest(URI server_uri, String appId) {
        super(server_uri, appId);
    }

    @Override
    void sendRequest(HttpPost httpPost) {
        try (CloseableHttpResponse response = getHttpClient().execute(httpPost)) {
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200) {
                throw new IllegalDataException("Cannot post message to " + this.getServerUri() + ", status code:" + statusCode);
            }
            String result = EntityUtils.toString(response.getEntity(), "UTF-8");
            TDLogger.println("Response="+result);
            JSONObject resultJson = JSONObject.parseObject(result);
            checkingRetCode(resultJson);
            return;
        } catch (Exception e) {
            TDLogger.println(e.getLocalizedMessage());
        } finally {
            httpPost.releaseConnection();
        }
    }

    @Override
    HttpEntity getHttpEntity(String data) {
        List<NameValuePair> nameValuePairs = new ArrayList<>();
        nameValuePairs.add(new BasicNameValuePair("source", "server"));
        nameValuePairs.add(new BasicNameValuePair("appid", getAppId()));
        nameValuePairs.add(new BasicNameValuePair("data", data));
        if (!this.writeData) {
            nameValuePairs.add(new BasicNameValuePair("dryRun", String.valueOf(1)));
        }
        if (this.deviceId != null && this.deviceId.length() > 0) {
            nameValuePairs.add(new BasicNameValuePair("deviceId", this.deviceId));
        }
        return new UrlEncodedFormEntity(nameValuePairs, StandardCharsets.UTF_8);
    }

    @Override
    void checkingRetCode(JSONObject resultJson) {
        if (resultJson.getInteger("errorLevel") != 0) {
            throw new IllegalDataException(resultJson.toJSONString());
        }
    }
}
