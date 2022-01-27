package example;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {
  public static String toHexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder(2 * bytes.length);
    for (byte b : bytes) {
      sb.append(String.format("%02X", b));
    }
    return sb.toString();
  }

  public static String md5hash(File file) throws Exception {
    try (InputStream str = new FileInputStream(file)) {
      return md5hash(str);
    }
  }

  public static String md5hash(InputStream rawInputStream) throws NoSuchAlgorithmException, IOException {
    try (BufferedInputStream inputStream = new BufferedInputStream(rawInputStream)) {
      MessageDigest md = MessageDigest.getInstance("MD5");
      byte[] buf = new byte[1000000];
      while (true) {
        int c = inputStream.read(buf);
        if (c < 0)
          break;
        md.update(buf, 0, c);
      }
      byte[] digest = md.digest();
      return Utils.toHexString(digest);
    }
  }
}
