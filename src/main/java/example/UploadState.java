package example;

import com.amazonaws.services.s3.model.PartETag;

import java.util.ArrayList;
import java.util.List;

public class UploadState {
  public String uploadId;
  List<PartETag> etags = new ArrayList<>();

  public UploadState(String uploadId) {
    this.uploadId = uploadId;
  }
}
