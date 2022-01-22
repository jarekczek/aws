package stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

public class TestPerfStream extends InputStream {
  private static Logger log = LoggerFactory.getLogger(TestPerfStream.class);

  private int speedKps;
  private int lengthK;

  private long bytesRead;
  private long startTime;

  public TestPerfStream(int lengthK, int speedKps) {
    this.speedKps = speedKps;
    this.lengthK = lengthK;
  }

  @Override
  public int read() {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }
    bytesRead++;

    //This is not very accurate, just an approximation.
    //Simulating a stream that is not immediate.
    if (bytesRead % (speedKps * 20 / 10) == 0) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (bytesRead / 1024 > lengthK) {
      return -1;
    }

    if (bytesRead % (1024 * 1024) == 0) {
      log.debug("speed [kps]: {} ({} / {})", actualSpeedKps(),
        bytesRead / 1024, elapsed() / 1000);
    }

    return 0;
  }

  private double actualSpeedKps() {
    return (bytesRead / 1024d) / (elapsed() / 1000d);
  }

  private long elapsed() {
    return System.currentTimeMillis() - startTime;
  }

  private double elapsedDouble() {
    return (System.currentTimeMillis() - startTime) * 1.0d;
  }

}
