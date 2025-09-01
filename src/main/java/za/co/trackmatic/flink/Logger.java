package za.co.trackmatic.flink;

import java.net.*;

public class Logger {

    public static void log(String msg, Integer port) {
        if(port == null){
            port = 5555;
        }

        try (DatagramSocket sock = new DatagramSocket()){
            msg = msg + "\n";
            byte[] b = msg.getBytes();
            int len = b.length;
            if (len > 1024) {
                len = 1024;
            }
            DatagramPacket packet = new DatagramPacket(b, len, InetAddress.getByName("localhost"), port);
            sock.send(packet);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
