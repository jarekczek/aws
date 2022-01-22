package example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListPartsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PartListing;
import com.amazonaws.services.s3.model.PartSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import stream.LimitedBandwidthInputStream;
import stream.PerfInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class Upload {
  String localPath;
  String remotePath;
  String bucketName = "jarekczekbucket";
  int partSize = 32 * 1024 * 1024;
  Logger log = LoggerFactory.getLogger(Upload.class);
  AmazonS3 s3;

  public Upload(String localPath, String remotePath) {
    this.localPath = localPath;
    this.remotePath = remotePath;
    if (this.remotePath.equals("/")) {
      // Otherwise aws creates directory with name "/".
      this.remotePath = "";
    }
    if (this.remotePath.endsWith("/") || this.remotePath.isEmpty()) {
      File localFile = new File(localPath);
      this.remotePath += localFile.getName();
    }
  }

  public static void main(String[] args) throws Exception {
    String localPath = args[0];
    String remotePath = args.length >= 2 ? args[1] : "";
    new Upload(localPath, remotePath).go();
  }

  private void go() throws Exception {
    File file = new File(localPath);
    log.info("uploading " + localPath + " as " + remotePath +
      ", size: " + file.length() +
      ", parts: " + String.format("%.02f", file.length() / 1.0 / partSize));
    s3 = AmazonS3ClientBuilder.defaultClient();
    UploadState uploadState = readOngoingUploadState();
    if (uploadState == null)
      uploadState = initUpload();
    while (true) {
      int partNr = uploadState.etags.size() + 1;
      PartETag etag = uploadPart(uploadState.uploadId, partNr);
      if (etag == null)
        break;
      uploadState.etags.add(etag);
    }
    s3.completeMultipartUpload(new CompleteMultipartUploadRequest(
      bucketName, remotePath, uploadState.uploadId, uploadState.etags));
    log.info("upload completed");
  }

  private UploadState initUpload() throws Exception {
    File file = new File(localPath);
    String fileMd5sum = Utils.md5hash(file);
    log.info("local file md5sum: " + fileMd5sum);
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, remotePath);
    initRequest.setStorageClass(StorageClass.DeepArchive);
    ObjectMetadata metadata = new ObjectMetadata();
    // Amazon doesn't accept setting these fields, so we duplicate it in user metadata.
    metadata.setContentMD5(fileMd5sum);
    metadata.setLastModified(Date.from(Instant.ofEpochMilli(file.lastModified())));
    Map<String, String> userMetaData = new HashMap<>();
    userMetaData.put("md5", fileMd5sum);
    userMetaData.put("lastModifiedTime", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(file.lastModified()));
    metadata.setUserMetadata(userMetaData);
    initRequest.setObjectMetadata(metadata);
    InitiateMultipartUploadResult initResult = s3.initiateMultipartUpload(initRequest);
    return new UploadState(initResult.getUploadId());
  }

  private UploadState readOngoingUploadState() {
    ListMultipartUploadsRequest req = new ListMultipartUploadsRequest(bucketName);
    req.setPrefix(remotePath);
    MultipartUploadListing result = s3.listMultipartUploads(req);
    for (MultipartUpload multipartUpload : result.getMultipartUploads()) {
      log.info("found ongoing upload for " + multipartUpload.getKey());
      UploadState uploadState = new UploadState(multipartUpload.getUploadId());
      PartListing partListing = s3.listParts(new ListPartsRequest(bucketName, multipartUpload.getKey(), multipartUpload.getUploadId()));
      for (PartSummary part : partListing.getParts()) {
        uploadState.etags.add(new PartETag(part.getPartNumber(), part.getETag()));
      }
      return uploadState;
    }
    return null;
  }

  private PartETag uploadPart(String uploadId, int partNr) throws Exception {
    log.info("uploading part " + partNr);
    UploadPartRequest uploadPartRequest = new UploadPartRequest();
    uploadPartRequest.setBucketName(bucketName);
    uploadPartRequest.setUploadId(uploadId);
    uploadPartRequest.setKey(remotePath);
    uploadPartRequest.setPartNumber(partNr);
    byte[] partBytes = getPartBytes(partNr);
    if (partBytes.length == 0) {
      log.info("part is empty");
      return null;
    }
    int speedLimitKBs = Integer.parseInt(System.getProperty("jarek.max_bandwidth", "0"));
    uploadPartRequest.setInputStream(new LimitedBandwidthInputStream(
      partBytes, speedLimitKBs, streamCallback()));
    uploadPartRequest.setPartSize(partBytes.length);
    String hash = md5sumBase64(partBytes);
    uploadPartRequest.setMd5Digest(hash);
    UploadPartResult partResult = s3.uploadPart(uploadPartRequest);
    System.out.println();
    return partResult.getPartETag();
  }

  private Consumer<PerfInfo> streamCallback() {
    return (pi -> {
      if (pi.bytesRead % (1024*1024) == 0) {
        if (System.getProperty("jarek.max_bandwidth") == null) {
          System.out.print(pi.toString() + ", ");
        } else {
          System.out.print(".");
        }
      }
    });
  }

  private String md5sum(byte[] partBytes) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(partBytes);
    return Utils.toHexString(md.digest());
  }

  private String md5sumBase64(byte[] partBytes) throws NoSuchAlgorithmException {
    MessageDigest md = MessageDigest.getInstance("MD5");
    md.update(partBytes);
    return Base64.getEncoder().encodeToString(md.digest());
  }

  private byte[] getPartBytes(int partNr) throws IOException {
    byte[] buf = new byte[partSize];
    try (FileInputStream str = new FileInputStream(localPath)) {
      int bytesRead;
      str.skip(partSize * (partNr - 1));
      bytesRead = str.read(buf, 0, partSize);
      if (bytesRead <= 0)
        return new byte[0];
      if (bytesRead == partSize)
        return buf;
      else
        return Arrays.copyOfRange(buf, 0, bytesRead);
    }
  }
}
