package stream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.function.Consumer;

public class LimitedBandwidthInputStream extends InputStream {
  private final InputStream inputStream;
  private final int requestedSpeedKps;

  private long totalRead = 0;
  private Consumer<PerfInfo> callback;

  private long startTime;

  public LimitedBandwidthInputStream(byte[] partBytes, int requestedSpeedKps) {
    this(new ByteArrayInputStream(partBytes), requestedSpeedKps);
  }

  public LimitedBandwidthInputStream(byte[] partBytes, int requestedSpeedKps, Consumer<PerfInfo> callback) {
    this(new ByteArrayInputStream(partBytes), requestedSpeedKps, callback);
  }

  public LimitedBandwidthInputStream(InputStream wrappedStream, int requestedSpeedKps) {
    this(wrappedStream, requestedSpeedKps, null);
  }

  public LimitedBandwidthInputStream(InputStream wrappedStream, int requestedSpeedKps, Consumer<PerfInfo> callback) {
    this.inputStream = wrappedStream;
    this.requestedSpeedKps = requestedSpeedKps;
    this.callback = callback;
  }

  @Override
  public int read() throws IOException {
    if (startTime == 0) {
      startTime = System.currentTimeMillis();
    }

    this.totalRead++;

    double actualSpeedKps = (this.totalRead / 1024d) / (this.elapsed() / 1000d);
    if (this.requestedSpeedKps != 0 && actualSpeedKps > this.requestedSpeedKps) {
      sleep(1);
    }

    if (callback != null) {
      callback.accept(createPerfInfo());
    }
    return inputStream.read();
  }

  private long elapsed() {
    return System.currentTimeMillis() - startTime;
  }

  private void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private PerfInfo createPerfInfo() {
    PerfInfo pi = new PerfInfo();
    pi.bytesRead = this.totalRead;
    pi.elapsedMillis = System.currentTimeMillis() - startTime;
    pi.speedKbs = (pi.bytesRead / 1024d) / (pi.elapsedMillis / 1000d);
    return pi;
  }

  @Override
  public void close() throws IOException {
    super.close();
    this.inputStream.close();
  }
}
