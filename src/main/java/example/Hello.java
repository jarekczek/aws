package example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

public class Hello {

  public static final String BUCKET_NAME = "jarekczekbucket";

  public Object handleRequest(Object event, Context context) throws Exception {
    String objectKey = null;
    if (event instanceof Map) {
      System.out.println("mapa");
      Map map = (Map) event;
      for (Object key: map.keySet()) {
        System.out.println("[" + key + "] = " + map.get(key));
        if (key.equals("key"))
          objectKey = map.get(key).toString();
      }
    }
    Object notificationKey = extractNotificationKey(event);
    if (notificationKey != null)
      objectKey = notificationKey.toString();
    System.out.println("objectKey: " + objectKey);
    AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
    S3Object file = s3.getObject(BUCKET_NAME, objectKey);
    InputStream rawStream = file.getObjectContent();
    try (InputStream bufferedStream = new BufferedInputStream(rawStream)) {
      String result = DigestUtils.md5Hex(bufferedStream);
      System.out.println(result);
      return result;
    }
  }
  
  private static Object extractNotificationKey(Object event) {
    List records = null;
    Map record = null;
    Map s3 = null;
    Map s3Object = null;
    Object objectKey = null;

    if (event instanceof Map) {
      Map eventMap = (Map) event;
      if (eventMap.containsKey("Records") && eventMap.get("Records") instanceof List)
        records = (List) eventMap.get("Records");
    }
    
    if (records != null) {
      System.out.println("records: " + records);
      if (records.get(0) instanceof Map)
        record = (Map)records.get(0);
    }
    
    if (record != null) {
      System.out.println("record: " + record);
      if (record.containsKey("s3") && record.get("s3") instanceof Map) {
        s3 = (Map)record.get("s3");
      }
    }
    
    if (s3 != null) {
      System.out.println("s3: " + s3);
      if (s3.containsKey("object") && s3.get("object") instanceof Map) {
        s3Object = (Map)s3.get("object");
      }
    }
    
    if (s3Object != null) {
      System.out.println("s3Object: " + s3Object);
      objectKey = s3Object.getOrDefault("key", null);
    }
    
    return objectKey;
  }

  public static void main1(String[] args) throws Exception {
    AmazonS3 client = AmazonS3ClientBuilder.defaultClient();
    String objectKey = "instal/microsoft/vidcap_clone_vb_cap_src_20110630.zip";
    System.out.println(client.doesObjectExist(BUCKET_NAME, objectKey));
    ObjectMetadata meta = client.getObjectMetadata(BUCKET_NAME, objectKey);
    System.out.println(meta.getContentLength());
    System.out.println(meta.getStorageClass());
    System.out.println(meta.getOngoingRestore());
    System.out.println(meta.getRestoreExpirationTime());
    //S3Object file = client.getObject(BUCKET_NAME, objectKey);
    //System.out.println(file.getObjectMetadata().getContentLength());
    String objectKey2 = "instal/microsoft/vidcap_clone_vb_vcap_exe_20110630.zip";
    String objectKey3 = "backup/tiktalik/go.bat";
    S3Object file = client.getObject(BUCKET_NAME, objectKey3);
    System.out.println(Utils.md5hash(file.getObjectContent()));
  }

  public static void main(String[] args) throws Exception {
    InputStream str = new FileInputStream("C:\\backup\\toshiba_c660\\verified\\drajwery.zip");
    long t0 = System.currentTimeMillis();
    System.out.println(DigestUtils.md5Hex(new BufferedInputStream(str)));
    //System.out.println(Utils.md5hash(str));
    System.out.println(System.currentTimeMillis() - t0);
    str.close();
    if (true) {
      System.exit(0);
    }

    String jsonPath = "J:\\lang\\java\\aws\\src\\main\\resources\\notificationEvent.json";
    byte[] bytes = Files.readAllBytes(new File(jsonPath).toPath());
    Map data = new ObjectMapper().readValue(bytes, Map.class);
    System.out.println(extractNotificationKey(data));
  }

}
