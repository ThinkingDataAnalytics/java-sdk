package cn.thinkingdata.analytics.request;

import cn.thinkingdata.analytics.exception.IllegalDataException;
import cn.thinkingdata.analytics.exception.NeedRetryException;
import cn.thinkingdata.analytics.util.TDLogger;
import com.alibaba.fastjson2.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.util.EntityUtils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;
public class TDBatchRequest extends TDBaseRequest {
    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }
    private String compress = "gzip";

    public TDBatchRequest(URI server_uri, String appId, Integer timeout) {
        super(server_uri, appId, timeout);
    }
    public TDBatchRequest(URI server_uri, String appId) {
        super(server_uri, appId);
    }

    @Override
    void sendRequest(HttpPost httpPost) {
        httpPost.addHeader("compress", compress);
        for (int i = 0; ; ) {
            try (CloseableHttpResponse response = getHttpClient().execute(httpPost)) {
                int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != 200) {
                    throw new NeedRetryException("Cannot post message to " + getServerUri() + ", status code:" + statusCode);
                }
                String result = EntityUtils.toString(response.getEntity(), "UTF-8");
                JSONObject resultJson = JSONObject.parseObject(result);
                checkingRetCode(resultJson);
                return;
            } catch (IOException | NeedRetryException e) {
                TDLogger.println(e.getLocalizedMessage());
                if (i++ == 2) {
                    throw new NeedRetryException("Cannot post message to " + getServerUri(), e);
                }
            } finally {
                httpPost.releaseConnection();
            }
        }
    }

    @Override
    HttpEntity getHttpEntity(String data) {
        try {
            byte[] dataCompressed = new byte[0];
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            if ("gzip".equalsIgnoreCase(this.compress)) {
                dataCompressed = gzipCompress(dataBytes);
            } else if ("none".equalsIgnoreCase(this.compress)) {
                dataCompressed = dataBytes;
            }
            return new ByteArrayEntity(dataCompressed);
        } catch (IOException e) {
            throw new NeedRetryException("compress failed", e);
        }
    }

    @Override
    void checkingRetCode(JSONObject resultJson) {
        int retCode = resultJson.getInteger("code");
        TDLogger.println("Response="+resultJson.toString());
        if (retCode != 0) {
            if (retCode == -1) {
                throw new IllegalDataException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid data format");
            } else if (retCode == -2) {
                throw new IllegalDataException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "APP ID doesn't exist");
            } else if (retCode == -3) {
                throw new IllegalDataException(resultJson.containsKey("msg") ? resultJson.getString("msg") : "invalid ip transmission");
            } else {
                throw new IllegalDataException("Unexpected response return code: " + retCode);
            }
        }
    }

    private static byte[] gzipCompress(byte[] srcBytes) throws IOException {
        GZIPOutputStream gzipOut = null;
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            gzipOut = new GZIPOutputStream(out);
            gzipOut.write(srcBytes);
            gzipOut.close();
            return out.toByteArray();
        } finally {
            if (gzipOut != null) {
                gzipOut.close();
            }
        }
    }
}
