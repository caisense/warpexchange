package com.itranswarp.exchange.push;

import jakarta.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.itranswarp.exchange.redis.RedisCache;
import com.itranswarp.exchange.support.LoggerSupport;

import io.vertx.core.Vertx;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.Request;
import io.vertx.redis.client.Response;
import io.vertx.redis.client.ResponseType;
import io.vertx.redis.client.impl.types.BulkType;

@Component
public class PushService extends LoggerSupport {

    @Value("${server.port}")
    private int serverPort;

    @Value("${exchange.config.hmac-key}")
    String hmacKey;

    @Value("${spring.redis.standalone.host:localhost}")
    private String redisHost;

    @Value("${spring.redis.standalone.port:6379}")
    private int redisPort;

    @Value("${spring.redis.standalone.password:}")
    private String redisPassword;

    @Value("${spring.redis.standalone.database:0}")
    private int redisDatabase = 0;

    private Vertx vertx;

    @PostConstruct
    public void startVertx() {
        logger.info("start vertx...");
        // 启动Vert.x:
        this.vertx = Vertx.vertx();
        // 创建一个Vert.x Verticle组件:
        var push = new PushVerticle(this.hmacKey, this.serverPort);
        vertx.deployVerticle(push);
        // 连接到Redis:
        String url = "redis://" + (this.redisPassword.isEmpty() ? "" : ":" + this.redisPassword + "@") + this.redisHost
                + ":" + this.redisPort + "/" + this.redisDatabase;

        logger.info("create redis client: {}", url);
        Redis redis = Redis.createClient(vertx, url);

        redis.connect().onSuccess(conn -> {
            logger.info("connect to redis ok.");
            // 连接成功事件处理:
            conn.handler(response -> {
                // 收到Redis的PUSH:
                if (response.type() == ResponseType.PUSH) {
                    int size = response.size();
                    // todo size==3?
                    if (size == 3) {
                        Response type = response.get(2);
                        // 收到PUBLISH通知:
                        if (type instanceof BulkType) {
                            String msg = type.toString();
                            if (logger.isDebugEnabled()) {
                                logger.debug("receive push message: {}", msg);
                            }
                            push.broadcast(msg);
                        }
                    }
                }
            });
            logger.info("try subscribe...");
            // 订阅Redis的Topic:
            conn.send(Request.cmd(Command.SUBSCRIBE).arg(RedisCache.Topic.NOTIFICATION)).onSuccess(resp -> {
                logger.info("subscribe ok.");
            }).onFailure(err -> {
                logger.error("subscribe failed.", err);
                System.exit(1);
            });
        }).onFailure(err -> {
            logger.error("connect to redis failed.", err);
            System.exit(1);
        });
    }

    void exit(int exitCode) {
        this.vertx.close();
        System.exit(exitCode);
    }
}
