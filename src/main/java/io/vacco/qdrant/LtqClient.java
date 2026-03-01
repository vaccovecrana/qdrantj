package io.vacco.qdrant;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.*;
import java.time.Duration;
import java.util.*;

public class LtqClient {

  public interface JsonIn {
    <T> T fromJson(Reader r, Type t);
  }

  public interface JsonOut {
    String toJson(Object o);
  }

  private final HttpClient httpClient;
  private final String baseUrl;
  private final JsonIn jIn;
  private final JsonOut jOut;

  public LtqClient(String qdrantUrl, JsonIn jIn, JsonOut jOut, Duration timeout) {
    this.baseUrl = qdrantUrl.endsWith("/") ? qdrantUrl.substring(0, qdrantUrl.length() - 1) : qdrantUrl;
    this.jIn = jIn;
    this.jOut = jOut;
    var htb = HttpClient.newBuilder();
    if (timeout != null) {
      htb.connectTimeout(timeout);
    }
    this.httpClient = htb.build();
  }

  public String qdSanitizeCollectionName(String name) {
    // Qdrant collection names must be valid identifiers
    return name.replaceAll("[^a-zA-Z0-9_-]", "_");
  }

  public void qdCreateCollection(String name, LtqCreateCollectionRequest req) {
    try {
      var httpReq = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + name))
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString(jOut.toJson(req)))
        .timeout(Duration.ofSeconds(30))
        .build();
      var resp = httpClient.send(httpReq, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() != 200 && resp.statusCode() != 201) {
        throw new IllegalStateException("Failed to create collection: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create collection", e);
    }
  }

  public LtqCollectionResponse qdGetCollection(String name) {
    try {
      var req = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + name))
        .GET()
        .timeout(Duration.ofSeconds(10))
        .build();
      var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        return jIn.fromJson(new StringReader(resp.body()), LtqCollectionResponse.class);
      } else {
        throw new IllegalStateException("Failed to get collection: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public Map<String, Object> qdHealthCheck() {
    try {
      var req = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/"))
        .GET()
        .timeout(Duration.ofSeconds(5))
        .build();
      var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        var body = resp.body();
        if (body != null && !body.trim().isEmpty()) {
          return jIn.fromJson(new StringReader(body), Map.class);
        } else {
          throw new IllegalStateException("Empty response body");
        }
      } else {
        throw new IllegalStateException("Health check failed: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Health check failed", e);
    }
  }

  public LtqCollectionResponse qdListCollections() {
    try {
      var req = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections"))
        .GET()
        .timeout(Duration.ofSeconds(10))
        .build();
      var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        return jIn.fromJson(new StringReader(resp.body()), LtqCollectionResponse.class);
      } else {
        throw new IllegalStateException("Failed to get collections: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to list collections", e);
    }
  }

  public LtqCollectionResponse qdScrollPoints(String collectionName, LtqScrollRequest req) {
    try {
      var httpReq = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + collectionName + "/points/scroll"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jOut.toJson(req)))
        .timeout(Duration.ofSeconds(30))
        .build();
      var resp = httpClient.send(httpReq, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        return jIn.fromJson(new StringReader(resp.body()), LtqCollectionResponse.class);
      } else {
        throw new IllegalStateException("Failed to scroll points: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to scroll points", e);
    }
  }

  public void qdDeleteCollection(String name) {
    try {
      var req = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + name))
        .DELETE()
        .timeout(Duration.ofSeconds(30))
        .build();
      var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() != 200) {
        throw new IllegalStateException("Failed to delete collection: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete collection", e);
    }
  }

  public void qdDeletePoints(String collectionName, LtqDeleteRequest req) {
    try {
      var httpReq = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + collectionName + "/points/delete"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jOut.toJson(req)))
        .timeout(Duration.ofSeconds(30))
        .build();
      var resp = httpClient.send(httpReq, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() != 200) {
        throw new IllegalStateException("Failed to delete points: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to delete points", e);
    }
  }

  public LtqSearchResponse qdSearchPoints(String collectionName, LtqSearchRequest req) {
    try {
      var httpReq = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + collectionName + "/points/search"))
        .header("Content-Type", "application/json")
        .POST(HttpRequest.BodyPublishers.ofString(jOut.toJson(req)))
        .timeout(Duration.ofSeconds(30))
        .build();
      var resp = httpClient.send(httpReq, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 200) {
        return jIn.fromJson(new StringReader(resp.body()), LtqSearchResponse.class);
      } else {
        throw new IllegalStateException("Failed to search points: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to search points", e);
    }
  }

  public void qdAddDocuments(String collectionName, List<LtqDocument> documents) {
    if (documents == null || documents.isEmpty()) {
      return; // Nothing to add
    }
    try {
      var points = new ArrayList<LtqPoint>();

      for (var doc : documents) {
        var point = new LtqPoint();
        point.id = doc.id;
        point.vector = doc.embedding;
        point.payload = doc.metadata;
        points.add(point);
      }

      var upsertReq = new LtqUpsertRequest();
      upsertReq.points = points;

      var req = HttpRequest.newBuilder()
        .uri(URI.create(baseUrl + "/collections/" + collectionName + "/points"))
        .header("Content-Type", "application/json")
        .PUT(HttpRequest.BodyPublishers.ofString(jOut.toJson(upsertReq)))
        .timeout(Duration.ofSeconds(60))
        .build();

      var resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() != 200) {
        throw new IOException("Failed to add documents: " + resp.statusCode() + " " + resp.body());
      }
    } catch (Exception e) {
      throw new IllegalStateException(e);
    }
  }

  public LtqFilter qdBuildFilter(Map<String, Object> filters) {
    var filter = new LtqFilter();
    var must = new ArrayList<LtqFilterCondition>();

    for (var entry : filters.entrySet()) {
      var condition = new LtqFilterCondition();
      condition.key = entry.getKey();
      var match = new LtqMatch();
      var value = entry.getValue();
      if (value instanceof List) {
        var anyList = new ArrayList<String>();
        for (var item : (List<?>) value) {
          if (item instanceof String) {
            anyList.add((String) item);
          }
        }
        match.any = anyList;
      } else {
        match.value = value.toString();
      }
      condition.match = match;
      must.add(condition);
    }

    filter.must = must;
    return filter;
  }

  public LtqVectorConfig qdCreateVectorConfig(int embeddingDimensions) {
    var config = new LtqVectorConfig();
    config.size = embeddingDimensions;
    config.distance = "Cosine";
    return config;
  }

}
