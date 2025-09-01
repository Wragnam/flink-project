package za.co.trackmatic.flink;

public class Common {

    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (Exception ignored) {
        }
    }
}
