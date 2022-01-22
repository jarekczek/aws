package stream;

public class PerfInfo {
  public long bytesRead;
  public long elapsedMillis;
  public double speedKBps;

  public String toString() {
    return "speed [kBps]: " + speedKBps +
      " (" + (bytesRead / 1024) + " / " + (elapsedMillis / 1000) + ")";
  }

}
