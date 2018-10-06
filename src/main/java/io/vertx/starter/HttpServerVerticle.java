package io.vertx.starter;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class HttpServerVerticle extends AbstractVerticle {

  public static final String CONFIG_HTTP_SERVER_PORT = "http.server.port";
  public static final String CONFIG_WIKIDB_QUEUE = "wikidb.queue";

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpServerVerticle.class);
  private static final String EMPTY_PAGE_MARKDOWN = "# A new page \n" +
    "\n" +
    "Feel-free to write in Markdown!\n";

  private String wikiDbQueue;
  private final FreeMarkerTemplateEngine engine = FreeMarkerTemplateEngine.create();

  @Override
  public void start(Future<Void> startFuture) throws Exception {

    this.wikiDbQueue = config().getString(CONFIG_WIKIDB_QUEUE, "wikidb.queue");

    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeleteHandler);

    int portNumber = config().getInteger(CONFIG_HTTP_SERVER_PORT, 8080);
    server
      .requestHandler(router::accept)
      .listen(portNumber, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port " + portNumber);
          startFuture.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          startFuture.fail(ar.cause());
        }
      });
  }

  private void pageDeleteHandler(RoutingContext context) {
    String id = context.request().getParam("id");
    JsonObject request = new JsonObject().put("id", id);
    DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "delete-page");

    vertx.eventBus().send(wikiDbQueue, request, deliveryOptions, reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/");
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext context) {
    LOGGER.debug("pageCreateHandler started with {}", context.request());
    String pageName = context.request().getParam("name");
    String location = "/wiki/" + pageName;
    if (pageName == null || pageName.isEmpty()) {
      location = "/";
    }
    context.response().setStatusCode(303);
    context.response().putHeader("Location", location);
    context.response().end();
  }

  private void pageUpdateHandler(RoutingContext context) {
    LOGGER.debug("pageUpdateHandler started with {}", context.request());
    String title = context.request().getParam("title");
    JsonObject request = new JsonObject()
      .put("id", context.request().getParam("id"))
      .put("title", title)
      .put("markdown", context.request().getParam("markdown"));

    DeliveryOptions deliveryOptions = new DeliveryOptions();
    if ("yes".equals(context.request().getParam("newPage"))) {
      deliveryOptions.addHeader("action", "create-page");
    } else {
      deliveryOptions.addHeader("action", "save-page");
    }
    LOGGER.debug("pageUpdateHandler send request with headers {}.",
      deliveryOptions.getHeaders()
    );

    vertx.eventBus().send(wikiDbQueue, request, deliveryOptions, reply -> {
      if (reply.succeeded()) {
        context.response().setStatusCode(303);
        context.response().putHeader("Location", "/wiki/" + title);
        context.response().end();
      } else {
        context.fail(reply.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext context) {
    LOGGER.debug("pageRenderingHandler started with {}", context.request());
    String page = context.request().getParam("page");
    JsonObject request = new JsonObject().put("page", page);

    DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "get-page");
    vertx.eventBus().send(wikiDbQueue, request, deliveryOptions, reply -> {
      if (reply.succeeded()) {
        JsonObject body =(JsonObject) reply.result().body();
        boolean foundFlag = body.getBoolean("found");
        LOGGER.debug("pageRenderingHandler found flag={}.", foundFlag);
        String rawContent = body.getString("rawContent", EMPTY_PAGE_MARKDOWN);
        LOGGER.debug("pageRenderingHandler rawContent={}.", rawContent);
        context.put("title", page);
        LOGGER.debug("pageRenderingHandler title={}.", page);
        context.put("id", body.getInteger("id", -1));
        LOGGER.debug("pageRenderingHandler id={}.", body.getInteger("id", -1));
        context.put("newPage", foundFlag ? "no" : "yes");
        LOGGER.debug("pageRenderingHandler title={}.", foundFlag ? "no" : "yes");
        context.put("rawContent", rawContent);
        context.put("content", Processor.process(rawContent));
        LOGGER.debug("pageRenderingHandler content={}.", rawContent);
        context.put("timestamp", new Date().toString());
        LOGGER.debug("pageRenderingHandler timestamp={}.", new Date().toString());

        engine.render(context, "templates", "/page.ftl", ar -> {
          if (ar.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(ar.result());
          } else {
            context.fail(ar.cause());
          }
        });
      } else {
        context.fail(reply.cause());
      }
    });

  }

  private void indexHandler(RoutingContext context) {
    DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "all-pages");
    vertx.eventBus().send(this.wikiDbQueue, new JsonObject(), deliveryOptions, reply -> {
      if (reply.succeeded()) {
        JsonObject body = (JsonObject) reply.result().body();
        context.put("title", "Wiki home");
        context.put("pages", body.getJsonArray("pages").getList());
        engine.render(context, "templates", "/index.ftl", asyncResult -> {
          if (asyncResult.succeeded()) {
            context.response().putHeader("Content-Type", "text/html");
            context.response().end(asyncResult.result());

          } else {
            context.fail(asyncResult.cause());
          }
        });

      } else {
        context.fail(reply.cause());
      }
    });
  }
}
