package edu.buffalo.cse.cse486586.simpledht;

import java.io.Serializable;
import java.util.Map;

public class Message implements Serializable {

    private static final long serialVersionUID = 7863261235394607847L;

    String key;
    String value;
    String type;
    String myPort;
    String successor;
    String predecessor;
    String query;
    Map<String,String> cursorList;

    public Message() {

    }

    public Message(String myPort,String type, String successor, String predecessor) {
        this.type = type;
        this.myPort = myPort;
        this.successor = successor;
        this.predecessor = predecessor;
    }

    public Message(String type, String key, String value) {
        this.type = type;
        this.key = key;
        this.value = value;
    }

    public Message(String myPort, String type, String key, String value, String query, Map<String,String> cursorList) {
        this.type = type;
        this.key = key;
        this.value = value;
        this.myPort = myPort;
        this.query = query;
        this.cursorList = cursorList;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getMyPort() {
        return myPort;
    }

    public void setMyPort(String myPort) {
        this.myPort = myPort;
    }

    public String getSuccessor() {
        return successor;
    }

    public void setSuccessor(String successor) {
        this.successor = successor;
    }

    public String getPredecessor() {
        return predecessor;
    }

    public void setPredecessor(String predecessor) {
        this.predecessor = predecessor;
    }

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public Map<String, String> getCursorList() {
        return cursorList;
    }

    public void setCursorList(Map<String, String> cursorList) {
        this.cursorList = cursorList;
    }
}
