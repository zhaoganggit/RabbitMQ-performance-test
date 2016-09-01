package com.bin.test.rabbitmqtest.ack;


import java.io.Serializable;


public class TestMessage implements Serializable {

    public TestMessage(){

    }

    public TestMessage(String content){
        this.content = content;
    }

    protected String content;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
