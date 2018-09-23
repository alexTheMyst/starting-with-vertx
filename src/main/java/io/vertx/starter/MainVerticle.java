package io.vertx.starter;

import com.github.rjeschke.txtmark.Processor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.templ.FreeMarkerTemplateEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

public class MainVerticle extends AbstractVerticle {

  private static final String SQL_CREATE_PAGES_TABLE = "create table if not exists Pages (Id integer identity primary key, Name varchar(255) unique, Content clob)";
  private static final String SQL_GET_PAGE = "select Id, Content from Pages where Name = ?";
  private static final String SQL_CREATE_PAGE = "insert into Pages values (NULL, ?, ?)";
  private static final String SQL_SAVE_PAGE = "update Pages set Content = ? where Id = ?";
  private static final String SQL_ALL_PAGES = "select Name from Pages";
  private static final String SQL_DELETE_PAGE = "delete from Pages where Id = ?";
  private static final String EMPTY_PAGE_MARKDOWN = "# A new page\n \n Feel-free to write in Markdown!\n";

  private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class);

  private final FreeMarkerTemplateEngine templateEngine = FreeMarkerTemplateEngine.create();

  private JDBCClient dbClient;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    Future<Void> steps = prepareDatabase().compose(v -> startHttpServer());
    steps.setHandler(startFuture.completer());
  }

  private Future<Void> prepareDatabase() {
    Future<Void> future = Future.future();

    this.dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", "jdbc:hsqldb:file:db/wiki")
      .put("driver_class", "org.hsqldb.jdbcDriver")
      .put("max_pool_size", 30));

    this.dbClient.getConnection(asyncResult -> {
      if (asyncResult.failed()) {
        LOGGER.error("DB connection failed.", asyncResult.cause());
        future.fail(asyncResult.cause());
      } else {
        LOGGER.debug("Db connection established.");
        SQLConnection connection = asyncResult.result();
        connection.execute(SQL_CREATE_PAGES_TABLE, createAsyncResult -> {
          connection.close();
          if (createAsyncResult.failed()) {
            LOGGER.error("Create table statement failed.", createAsyncResult.cause());
            future.fail(createAsyncResult.cause());
          } else {
            LOGGER.debug("Pages table created.");
            future.complete();
          }
        });
      }
    });

    return future;
  }

  private Future<Void> startHttpServer() {
    Future<Void> future = Future.future();
    HttpServer server = vertx.createHttpServer();

    Router router = Router.router(vertx);
    router.get("/").handler(this::indexHandler);
    router.get("/wiki/:page").handler(this::pageRenderingHandler);
    router.post().handler(BodyHandler.create());
    router.post("/save").handler(this::pageUpdateHandler);
    router.post("/create").handler(this::pageCreateHandler);
    router.post("/delete").handler(this::pageDeleteHandler);

    server
      .requestHandler(router::accept)
      .listen(8080, ar -> {
        if (ar.succeeded()) {
          LOGGER.info("HTTP server running on port 8080");
          future.complete();
        } else {
          LOGGER.error("Could not start a HTTP server", ar.cause());
          future.fail(ar.cause());
        }
      });
    return future;
  }

  private void pageDeleteHandler(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    LOGGER.debug("PageDeleteHandler set id={}", id);
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.updateWithParams(SQL_DELETE_PAGE, new JsonArray().add(id), res -> {
          connection.close();
          if (res.succeeded()) {
            routingContext.response().setStatusCode(303);
            routingContext.response().putHeader("Location", "/");
            routingContext.response().end();
            LOGGER.debug("PageDeleteHandler finished.");
          } else {
            routingContext.fail(res.cause());
          }
        });
      } else {
        routingContext.fail(car.cause());
      }
    });
  }

  private void pageCreateHandler(RoutingContext routingContext) {
    String pageName = routingContext.request().getParam("name");
    String location = "/wiki/" + pageName;
    LOGGER.debug("PageCreateHandler set pageName={}.", pageName);
    if (pageName == null || pageName.isEmpty()) {
      location = "/";
    }
    routingContext.response().setStatusCode(303);
    routingContext.response().putHeader("Location", location);
    routingContext.response().end();
    LOGGER.debug("PageCreateHandler finished.");
  }

  private void pageUpdateHandler(RoutingContext routingContext) {
    String id = routingContext.request().getParam("id");
    String title = routingContext.request().getParam("title");
    String markdown = routingContext.request().getParam("markdown");
    boolean newPage = "yes".equals(routingContext.request().getParam("newPage"));
    LOGGER.debug("PageUpdateHandler started with id={} title={} markdown={} newPage={}",
      id, title, markdown, newPage);
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        String sql = newPage ? SQL_CREATE_PAGE : SQL_SAVE_PAGE;
        JsonArray params = new JsonArray();   // <3>
        if (newPage) {
          params.add(title).add(markdown);
        } else {
          params.add(markdown).add(id);
        }
        LOGGER.debug("PageUpdateHandler params={}.", params);
        connection.updateWithParams(sql, params, res -> {   // <4>
          connection.close();
          if (res.succeeded()) {
            routingContext.response().setStatusCode(303);    // <5>
            routingContext.response().putHeader("Location", "/wiki/" + title);
            routingContext.response().end();
            LOGGER.debug("PageUpdateHandler finished.");
          } else {
            routingContext.fail(res.cause());
          }
        });
      } else {
        routingContext.fail(car.cause());
      }
    });
  }

  private void pageRenderingHandler(RoutingContext routingContext) {
    String page = routingContext.request().getParam("page");
    LOGGER.debug("PageRenderingHandler started for page={}.", page);

    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        connection.queryWithParams(SQL_GET_PAGE, new JsonArray().add(page), fetch -> {
          connection.close();
          if (fetch.succeeded()) {
            JsonArray row = fetch.result().getResults()
              .stream()
              .findFirst()
              .orElseGet(() -> new JsonArray().add(-1).add(EMPTY_PAGE_MARKDOWN));
            LOGGER.debug("PageRenderingHandler get row={}.", row);
            Integer id = row.getInteger(0);
            String rawContent = row.getString(1);

            routingContext.put("title", page);
            routingContext.put("id", id);
            routingContext.put("newPage", fetch.result().getResults().size() == 0 ? "yes" : "no");
            routingContext.put("rawContent", rawContent);
            routingContext.put("content", Processor.process(rawContent));
            routingContext.put("timestamp", new Date().toString());

            templateEngine.render(routingContext, "templates", "/page.ftl", ar -> {
              if (ar.succeeded()) {
                routingContext.response().putHeader("Content-Type", "text/html");
                routingContext.response().end(ar.result());
                LOGGER.debug("PageRenderingHandler finished.");
              } else {
                routingContext.fail(ar.cause());
              }
            });
          } else {
            routingContext.fail(fetch.cause());
          }
        });

      } else {
        routingContext.fail(car.cause());
      }
    });
  }

  private void indexHandler(RoutingContext context) {
    dbClient.getConnection(car -> {
      if (car.succeeded()) {
        SQLConnection connection = car.result();
        LOGGER.debug("IndexHandler got sql connection.");
        connection.query(SQL_ALL_PAGES, res -> {
          connection.close();
          if (res.succeeded()) {
            List<String> pages = res.result()
              .getResults()
              .stream()
              .map(json -> json.getString(0)).sorted()
              .collect(Collectors.toList());
            LOGGER.debug("IndexHandler got a list from DB.");
            context.put("title", "Wiki home");
            context.put("pages", pages);
            templateEngine.render(context, "templates", "/index.ftl", ar -> {
              if (ar.succeeded()) {
                context.response().putHeader("Content-Type", "text/html");
                context.response().end(ar.result());
                LOGGER.debug("IndexHandler template engine rendered response page.");
              } else {
                context.fail(ar.cause());
              }
            });
          } else {
            context.fail(res.cause());
          }
        });
      } else {
        context.fail(car.cause());
      }
    });
  }
}
