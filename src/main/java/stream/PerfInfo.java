package stream;

public class PerfInfo {
  public long bytesRead;
  public long elapsedMillis;
  public double speedKbs;

  public String toString() {
    return "speed [kps]: " + speedKbs +
      " (" + (bytesRead / 1024) + " / " + (elapsedMillis / 1000) + ")";
  }

}
