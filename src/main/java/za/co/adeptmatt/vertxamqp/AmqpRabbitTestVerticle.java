package za.co.adeptmatt.vertxamqp;

import io.vertx.amqp.AmqpClient;
import io.vertx.amqp.AmqpClientOptions;
import io.vertx.amqp.AmqpConnection;
import io.vertx.amqp.AmqpMessage;
import io.vertx.amqp.AmqpReceiver;
import io.vertx.core.AbstractVerticle;
import java.util.UUID;

/**
 *
 * @author matt-home
 */
public class AmqpRabbitTestVerticle extends AbstractVerticle {

    @Override
    public void start() throws Exception {
        AmqpClientOptions options = new AmqpClientOptions()
                .setHost("YOUR_RABBIT_MQ_SERVER")
                .setPort(5672);

        AmqpClient client = AmqpClient.create(vertx, options);

        client.connect(result -> {
            if (result.failed()) {
                System.out.println("Unable to connect to the broker");
                System.out.println(result.cause());
            } else {
                System.out.println("Connection succeeded");
                AmqpConnection connection = result.result();
                connection.exceptionHandler(exception -> exception.printStackTrace());
                createSendAndRec(connection);
            }
        });
    }

    private void createSendAndRec(AmqpConnection connection) {
        connection.createAnonymousSender(responseSender -> {
            if (responseSender.failed()) {
                System.out.println("Failed to create anon sender");
                System.out.println(responseSender.cause());
            } else {
                System.out.println("Created anon sender");
            }
            // You got an anonymous sender, used to send the reply
            // Now register the main receiver:
            connection.createReceiver("my-queue", done -> {
                if (done.failed()) {
                    System.out.println("Unable to create receiver");
                } else {
                    System.out.println("Created receiver");
                    AmqpReceiver receiver = done.result();
                    receiver.handler(msg -> {
                        // You got the message, let's reply.

                        System.out.println("Got request: " + msg.bodyAsString());

                        responseSender.result()
                                .send(AmqpMessage.create()
                                        .address(msg.replyTo())
                                        .correlationId(msg.id()) // send the message id as correlation id
                                        .withBody("Hello, " + msg.bodyAsString())
                                        .build());

                    });

                }
            });

        }).exceptionHandler(exception
                -> exception.printStackTrace()
        );

        connection.createDynamicReceiver(replyReceiver -> {
            System.out.println("Created dynamic receiver");
            // We got a receiver, the address is provided by the broker
            String replyToAddress = replyReceiver.result().address();

            // Attach the handler receiving the reply
            replyReceiver.result().handler(msg -> {
                System.out.println("Got the reply! " + msg.bodyAsString());
            });

            // Create a sender and send the message:
            connection.createSender("my-queue", sender -> {

                System.out.println("Sending message");

                String messageId = UUID.randomUUID().toString();

                //vertx.setPeriodic(10000, handler -> {
                sender.result().send(AmqpMessage.create()
                        .replyTo(replyToAddress)
                        .id(messageId)
                        .withBody("Matt").build());
            });
        }).exceptionHandler(exception
                -> exception.printStackTrace()
        );
    }
}
