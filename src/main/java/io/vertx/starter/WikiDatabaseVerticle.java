package io.vertx.starter;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class WikiDatabaseVerticle extends AbstractVerticle {

  public static final String CONFIG_WIKI_JDBC_URL = "wiki.jdbc.url";
  public static final String CONFIG_WIKI_JDBC_DRIVER_CLASS = "wiki.jdbc.driver.class";
  public static final String CONFIG_WIKI_MAX_POOL_SIZE = "wiki.jdbc.max.pool.size";
  public static final String CONFIG_WIKI_SQL_QUERIES_RESOURCE_FILE = "wiki.sql.queries.resource.file";
  public static final String CONFIG_WIKI_QUEUE = "wikidb.queue";

  private static final Logger LOGGER = LoggerFactory.getLogger(WikiDatabaseVerticle.class);

  private final Map<SqlQuery, String> sqlQueries = new HashMap<>();

  private enum SqlQuery {
    CREATE_PAGES_TABLE,
    ALL_PAGES,
    GET_PAGE,
    CREATE_PAGE,
    SAVE_PAGE,
    DELETE_PAGE
  }

  private JDBCClient dbClient;

  @Override
  public void start(Future<Void> startFuture) throws Exception {
    loadSqlQueries();

    dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", config().getString(CONFIG_WIKI_JDBC_URL, "jdbc:hsqldb:file:db/wiki"))
      .put("driver_class", config().getString(CONFIG_WIKI_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_WIKI_MAX_POOL_SIZE, 30)));

    dbClient.getConnection(asyncResult -> {
      if (asyncResult.failed()) {
        LOGGER.error("DB connection failed.", asyncResult.cause());
        startFuture.fail(asyncResult.cause());
      } else {
        LOGGER.debug("Db connection established.");
        SQLConnection connection = asyncResult.result();
        connection.execute(sqlQueries.get(SqlQuery.CREATE_PAGES_TABLE), createAsyncResult -> {
          connection.close();
          if (createAsyncResult.failed()) {
            LOGGER.error("Create table statement failed.", createAsyncResult.cause());
            startFuture.fail(createAsyncResult.cause());
          } else {
            LOGGER.debug("Pages table created.");
            vertx.eventBus().consumer(config().getString(CONFIG_WIKI_QUEUE, "wikidb.queue"), this::onMessage);
            LOGGER.debug("Queue consumer created {}.", config().getString(CONFIG_WIKI_QUEUE, "wikidb.queue"));
            startFuture.complete();
          }
        });
      }
    });
  }

  public enum ErrorCodes {
    NO_ACTION_SPECIFIED,
    BAD_ACTION,
    DB_ERROR
  }

  private void onMessage(Message<JsonObject> message) {
    LOGGER.debug("onMessage got message with headers {} and body {}."
    ,message.headers()
    ,message.body());
    if (!message.headers().contains("action")) {
      LOGGER.error("No action headers specified for message with headers {} and body {}",
        message.headers(), message.body());
      message.fail(ErrorCodes.NO_ACTION_SPECIFIED.ordinal(), "No action header specified.");
      return;
    }

    String action = message.headers().get("action");

    switch (action) {
      case "all-pages":
        fetchAllPAges(message);
        break;
      case "get-page":
        fetchPage(message);
        break;
      case "create-page":
        createPage(message);
        break;
      case "save-page":
        savePage(message);
        break;
      case "delete-page":
        deletePage(message);
        break;
      default:
        message.fail(ErrorCodes.BAD_ACTION.ordinal(), "Bad action " + action);
    }
  }

  private void deletePage(Message<JsonObject> message) {
    JsonArray data = new JsonArray().add(message.body().getString("id"));

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_PAGE), data, result -> {
      if (result.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, result.cause());
      }
    });
  }

  private void savePage(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray data = new JsonArray()
      .add(request.getString("markdown"))
      .add(request.getString("id"));

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_PAGE), data, result -> {
      if (result.succeeded()) {
        message.reply("ok");
      } else {
        reportQueryError(message, result.cause());
      }
    } );
  }

  private void createPage(Message<JsonObject> message) {
    JsonObject request = message.body();
    JsonArray data = new JsonArray()
      .add(request.getString("title"))
      .add(request.getString("markdown"));
    LOGGER.debug("createPage creating page with {}.", data);

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_PAGE), data, result -> {
        if (result.succeeded()) {
          message.reply("ok");
        } else {
          reportQueryError(message, result.cause());
        }
      }
    );
  }

  private void fetchPage(Message<JsonObject> message) {
    LOGGER.debug("fetchPage got a message with headers {} adn a body {}.",
      message.headers(), message.body());
    String requestedPage = message.body().getString("page");
    LOGGER.debug("fetchPage requestedPage = {}", requestedPage);
    JsonArray params = new JsonArray().add(requestedPage);

    dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_PAGE), params, fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();
        ResultSet resultSet = fetch.result();
        if (resultSet.getNumRows() == 0) {
          response.put("found", false);
        } else {
          response.put("found", true);
          JsonArray row = resultSet.getResults().get(0);
          response.put("id", row.getInteger(0));
          response.put("rawContent", row.getString(1));
        }
        message.reply(response);
      } else {
        reportQueryError(message, fetch.cause());
      }
    });
  }

  private void fetchAllPAges(Message<JsonObject> message) {
    dbClient.query(sqlQueries.get(SqlQuery.ALL_PAGES), result -> {
      if (result.succeeded()) {
        List<String> pages = result.result()
          .getResults()
          .stream()
          .map(json -> json.getString(0))
          .sorted()
          .collect(Collectors.toList());
        message.reply(new JsonObject().put("pages", new JsonArray(pages)));
      } else {
        reportQueryError(message, result.cause());
      }
    });
  }

  private void reportQueryError(Message<JsonObject> message, Throwable cause) {
    LOGGER.error("Database error.", cause);
    message.fail(ErrorCodes.DB_ERROR.ordinal(), cause.getMessage());
  }

  private void loadSqlQueries() throws IOException {
    String queriesFile = config().getString(CONFIG_WIKI_SQL_QUERIES_RESOURCE_FILE);
    InputStream queryInputStream;
    if (queriesFile != null) {
      queryInputStream = new FileInputStream(queriesFile);
    } else {
      queryInputStream = getClass().getResourceAsStream("/db-queries.properties");
    }

    Properties props = new Properties();
    props.load(queryInputStream);
    queryInputStream.close();

    sqlQueries.put(SqlQuery.CREATE_PAGES_TABLE, props.getProperty("create-pages-table"));
    sqlQueries.put(SqlQuery.GET_PAGE, props.getProperty("get-page"));
    sqlQueries.put(SqlQuery.CREATE_PAGE, props.getProperty("create-page"));
    sqlQueries.put(SqlQuery.SAVE_PAGE, props.getProperty("save-page"));
    sqlQueries.put(SqlQuery.ALL_PAGES, props.getProperty("all-pages"));
    sqlQueries.put(SqlQuery.DELETE_PAGE, props.getProperty("delete-page"));

  }
}
