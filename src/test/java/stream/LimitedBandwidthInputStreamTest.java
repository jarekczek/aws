package stream;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public class LimitedBandwidthInputStreamTest {
  @Test
  public void test() throws IOException {
    InputStream stream1 = new TestPerfStream(2048, 200);
    int speedLimit = Integer.parseInt(System.getProperty("jarek.max_bandwidth", "150"));
    InputStream stream2 = new LimitedBandwidthInputStream(stream1, speedLimit, streamCallback());
    while (stream2.read() >= 0) {
      // Do nothing.
    }
  }

  private Consumer<PerfInfo> streamCallback() {
    return (pi -> {
      if ((pi.bytesRead) % (1024*1024) == 0) {
        System.out.println(pi.toString());
      }
    });
  }

}