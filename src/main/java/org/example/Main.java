package org.example;

import ckafka.data.SwapData;
import lac.cnclib.net.NodeConnection;
import lac.cnclib.sddl.message.ApplicationMessage;
import lac.cnclib.sddl.message.Message;
import main.java.ckafka.mobile.CKMobileNode;
import main.java.ckafka.mobile.tasks.SendLocationTask;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

public class Main extends CKMobileNode {
    public static void main(String[] args) {
        Scanner keyboard = new Scanner(System.in);
        Main m = new Main();
        //System.out.println("Mensagem:");
        m.sendUnicastMessage(keyboard);
    }

    @Override
    public void connected(NodeConnection nodeConnection) {
        try {
            System.out.println("Connected");
            final SendLocationTask sendlocationtask = new SendLocationTask(this);
            this.scheduledFutureLocationTask = this.threadPool.scheduleWithFixedDelay(
                    sendlocationtask, 5000, 60000, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            System.out.println("Error scheduling SendLocationTask");
        }
    }

    private void sendUnicastMessage(Scanner keyboard) {
        System.out.println("Mensagem unicast. Entre com o UUID do NM: ");
        String uuid = keyboard.nextLine();
        System.out.print("Entre com a mensagem: ");
        String messageText = keyboard.nextLine();
        System.out.println(String.format("Mensagem de %s para %s.", messageText, uuid));
        // Create and send the message
        SwapData privateData = new SwapData();
        privateData.setMessage(messageText.getBytes(StandardCharsets.UTF_8));
        privateData.setTopic("PrivateMessageTopic");
        privateData.setRecipient(uuid);
        ApplicationMessage message = createDefaultApplicationMessage();
        message.setContentObject(privateData);
        sendMessageToGateway(message);
    }


    @Override
    public void newMessageReceived(NodeConnection nodeConnection, Message message) {
        try {
            SwapData swp = fromMessageToSwapData(message);
            if(swp.getTopic().equals("Ping")) {
                message.setSenderID(this.mnID);
                sendMessageToGateway(message);
            } else {
                String str = new String(swp.getMessage(), StandardCharsets.UTF_8);
                logger.info("Message: " + str);
            }
        } catch (Exception e) {
            logger.error("Error reading new message received");
        }

    }

    @Override
    public void disconnected(NodeConnection nodeConnection) {

    }

    @Override
    public void unsentMessages(NodeConnection nodeConnection, List<Message> list) {

    }

    @Override
    public void internalException(NodeConnection nodeConnection, Exception e) {

    }
}