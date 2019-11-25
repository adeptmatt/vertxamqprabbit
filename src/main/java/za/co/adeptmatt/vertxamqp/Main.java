package za.co.adeptmatt.vertxamqp;

import io.vertx.core.Vertx;

/**
 *
 * @author matt-home
 */
public class Main {
    public static void main(String...args){
        Vertx vertx = Vertx.vertx();
        
        vertx.deployVerticle(new AmqpRabbitTestVerticle());
    }
}
