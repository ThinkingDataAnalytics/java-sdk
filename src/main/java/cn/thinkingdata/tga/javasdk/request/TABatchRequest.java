package cn.thinkingdata.tga.javasdk.request;

import cn.thinkingdata.tga.javasdk.exception.IllegalDataException;
import cn.thinkingdata.tga.javasdk.exception.NeedRetryException;
import cn.thinkingdata.tga.javasdk.util.TALogger;
import com.alibaba.fastjson.JSONObject;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import org.anarres.lzo.LzoAlgorithm;
import org.anarres.lzo.LzoCompressor;
import org.anarres.lzo.LzoLibrary;
import org.anarres.lzo.LzoOutputStream;
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
public class TABatchRequest extends TABaseRequest {

    public String getCompress() {
        return compress;
    }

    public void setCompress(String compress) {
        this.compress = compress;
    }
    private String compress = "gzip";

    public TABatchRequest(URI server_uri, String appId, Integer timeout) {
        super(server_uri, appId, timeout);
    }
    public TABatchRequest(URI server_uri, String appId) {
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
                TALogger.print(e.getLocalizedMessage());
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
            byte[] dataCompressed;
            byte[] dataBytes = data.getBytes(StandardCharsets.UTF_8);
            if ("lzo".equalsIgnoreCase(this.compress)) {
                dataCompressed = lzoCompress(dataBytes);
            } else if ("lz4".equalsIgnoreCase(this.compress)) {
                dataCompressed = lz4Compress(dataBytes);
            } else if ("none".equalsIgnoreCase(this.compress)) {
                dataCompressed = dataBytes;
            } else {
                dataCompressed = gzipCompress(dataBytes);
            }
            return new ByteArrayEntity(dataCompressed);
        } catch (IOException e) {
            throw new NeedRetryException("压缩数据失败！", e);
        }
    }

    @Override
    void checkingRetCode(JSONObject resultJson) {
        int retCode = resultJson.getInteger("code");
        TALogger.print("Response="+resultJson.toString());
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

    private static byte[] lz4Compress(byte[] srcBytes) throws IOException {
        LZ4Factory factory = LZ4Factory.fastestInstance();
        ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
        LZ4Compressor compressor = factory.fastCompressor();
        LZ4BlockOutputStream compressedOutput = new LZ4BlockOutputStream(
                byteOutput, 2048, compressor);
        compressedOutput.write(srcBytes);
        compressedOutput.close();
        return byteOutput.toByteArray();
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
    private static byte[] lzoCompress(byte[] srcBytes) throws IOException {
        LzoCompressor compressor = LzoLibrary.getInstance().newCompressor(
                LzoAlgorithm.LZO1X, null);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        LzoOutputStream cs = new LzoOutputStream(os, compressor);
        cs.write(srcBytes);
        cs.close();
        return os.toByteArray();
    }

}
