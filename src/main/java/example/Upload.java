package example;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

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
    List<PartETag> etags = new ArrayList<>();
    log.info("uploading " + localPath + " as " + remotePath +
      ", size: " + file.length() +
      ", parts: " + String.format("%.02f", file.length() / 1.0 / partSize));
    s3 = AmazonS3ClientBuilder.defaultClient();
    InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, remotePath);
    initRequest.setStorageClass(StorageClass.DeepArchive);
    InitiateMultipartUploadResult initResult = s3.initiateMultipartUpload(initRequest);
    log.info("uploadId: " + initResult.getUploadId());
    try {
      for (int partNr = 1; true; partNr++) {
        PartETag etag = uploadPart(initResult.getUploadId(), partNr);
        if (etag == null)
          break;
        etags.add(etag);
      }
      s3.completeMultipartUpload(new CompleteMultipartUploadRequest(
        bucketName, remotePath, initResult.getUploadId(), etags));
      log.info("upload completed");
    } catch (Exception e) {
      log.error("catched upload error", e);
      s3.abortMultipartUpload(
        new AbortMultipartUploadRequest(bucketName, remotePath, initResult.getUploadId()));
      log.info("upload aborted");
    }
    log.info("local file md5sum: " + Utils.md5hash(new File(localPath)));
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
    uploadPartRequest.setInputStream(new ByteArrayInputStream(partBytes));
    uploadPartRequest.setPartSize(partBytes.length);
    String hash = md5sumBase64(partBytes);
    uploadPartRequest.setMd5Digest(hash);
    log.info("md5 base64: " + hash + ", hex: " + md5sum(partBytes));
    UploadPartResult partResult = s3.uploadPart(uploadPartRequest);
    log.info("got etag: " + partResult.getPartETag().getETag());
    return partResult.getPartETag();
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
    File file = new File(localPath);
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
