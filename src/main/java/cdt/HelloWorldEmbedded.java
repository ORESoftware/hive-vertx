package cdt;

import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.ResultSet;
import org.apache.hive.jdbc.HiveDriver;
import io.vertx.core.Vertx;
// import org.apache.hive.jdbc.HiveDriver;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.web.Router;

// http://vertx.io/blog/my-first-vert-x-3-application/
// https://github.com/vert-x3/vertx-maven-starter


// DEV: jdbc:hive2://hddev1-edge-lb01:20000/
// STAGE: jdbc:hive2://hdstg1-edge-lb01:20000/
// PROD: jdbc:hive2://hdprd1-edge-lb01:20000


/*

The Vert.x CLI, the Maven plugin and the Gradle plugin all have
redeploy/reload options:

http://vertx.io/docs/vertx-core/java/#_live_redeploy
https://github.com/fabric8io/vertx-maven-plugin
https://github.com/jponge/vertx-gradle-plugin

They rebuild the app completely. Doesn't take too long usually, a few
seconds (no complex dependency tree to build!)

But it's not partial reloading like JRebel.

For future questions please use the Vert.x forum (see vertx.io/community)


https://github.com/vert-x3/vertx-examples/blob/master/web-examples/src/main/java/io/vertx/example/web/rest/SimpleREST.java
https://github.com/vert-x3/vertx-examples/blob/master/jdbc-examples/src/main/java/io/vertx/example/jdbc/transaction/JDBCExample.java

*/

///////////////////////////////////////////////////////////////////////////////////

public class HelloWorldEmbedded {

  public static void main(String[] args) {

    Vertx vertx = Vertx.vertx();

    JsonObject config = new JsonObject()
        .put("url", "jdbc:hive2://hdprd1-edge-lb01:20000")
        .put("driver_class", "org.hsqldb.jdbcDriver")
        .put("user", "hdpai")
        .put("password", "Dat@_Infusi0n_1")
        .put("max_pool_size", 30);

    SQLClient client = JDBCClient.createShared(vertx, config);



    // Create a router object.
    Router router = Router.router(vertx);

    // Bind "/" to our hello message.
    router.route("/").handler(routingContext -> {
      HttpServerResponse response = routingContext.response();

      client.getConnection(res -> {
        if (res.succeeded()) {

          SQLConnection connection = res.result();

          System.out.println("connection succeeded.");
          connection.query("SHOW DATABASES", resp -> {
            if (resp.succeeded()) {

              ResultSet rs = resp.result();
              // Do something with results
              System.out.println(rs);

              response
                  .putHeader("content-type", "application/json")
                  .end(rs.toJson().toString());
            }
            else {
              System.out.println("Failed.");
              response
                  .putHeader("content-type", "application/json")
                  .end(new JsonObject().put("error","failed to show databases").toString());
            }
          });

        }
        else {
          System.out.println("connection refused.");
          response
              .putHeader("content-type", "application/json")
              .end(new JsonObject().put("error","sql client connection refused").toString());
        }
      });

    });

//    router.route("/assets/*").handler(StaticHandler.create("assets"));
//
//    router.get("/api/whiskies").handler(this::getAll);
//    router.route("/api/whiskies*").handler(BodyHandler.create());
//    router.post("/api/whiskies").handler(this::addOne);
//    router.get("/api/whiskies/:id").handler(this::getOne);
//    router.put("/api/whiskies/:id").handler(this::updateOne);
//    router.delete("/api/whiskies/:id").handler(this::deleteOne);

//    vertx.createHttpServer()
//        .requestHandler(req -> req.response()
//            .end("Hello World!")).listen(8080);

    vertx.createHttpServer()
        .requestHandler(router::accept).listen(8080);



  }

}