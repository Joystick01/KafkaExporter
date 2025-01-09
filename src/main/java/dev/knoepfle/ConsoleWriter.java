package dev.knoepfle;

public class ConsoleWriter implements Writer {

    String topic;

    public ConsoleWriter(String topic) {
        this.topic = topic;
    }

    @Override
    public void write(String message) {
        System.out.println("[" + topic + "] " + message);
    }
}
