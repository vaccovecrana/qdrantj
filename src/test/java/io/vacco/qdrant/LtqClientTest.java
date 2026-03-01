package io.vacco.qdrant;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import j8spec.annotation.DefinedOrder;
import j8spec.junit.J8SpecRunner;
import org.junit.runner.RunWith;

import java.awt.*;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static j8spec.J8Spec.*;
import static org.junit.Assert.*;

@DefinedOrder
@RunWith(J8SpecRunner.class)
public class LtqClientTest {

  public static final String qdrantUrl = "http://localhost:6333";

  private static final Gson gson = new GsonBuilder().create();
  private static LtqClient client;

  static {
    if (GraphicsEnvironment.isHeadless()) {
      System.out.println("CI/CD build. Nothing to do.");
    } else {
      client = new LtqClient(qdrantUrl, gson::fromJson, gson::toJson, Duration.ofSeconds(30));

      describe("LtqClient", () -> {
        it("qdSanitizeCollectionName sanitizes collection names", () -> {
          assertEquals("test_collection", client.qdSanitizeCollectionName("test collection"));
          assertEquals("test_collection_123", client.qdSanitizeCollectionName("test collection 123"));
          assertEquals("test_collection", client.qdSanitizeCollectionName("test@collection"));
        });

        it("qdCreateVectorConfig creates vector config", () -> {
          var config = client.qdCreateVectorConfig(384);
          assertEquals(384, config.size);
          assertEquals("Cosine", config.distance);
        });

        it("qdBuildFilter builds filters from map", () -> {
          var filters = Map.of("category", "books", "tags", List.of("fiction", "drama"));
          var filter = client.qdBuildFilter(filters);
          assertNotNull(filter.must);
          assertEquals(2, filter.must.size());
          assertEquals(2, filter.must.size());
          var keys = filter.must.stream().map(c -> c.key).collect(Collectors.toList());
          assertTrue(keys.contains("category"));
          assertTrue(keys.contains("tags"));
        });

        it("qdHealthCheck verifies server connectivity", () -> {
          var health = client.qdHealthCheck();
          assertNotNull(health);
          System.out.println("Qdrant server is running. Health: " + health);
        });
      });

      describe("Collection operations", () -> {
        var collectionName = "test_collection_" + System.currentTimeMillis();

        it("qdCreateCollection creates a new collection", () -> {
          var vectorConfig = client.qdCreateVectorConfig(128);
          var createReq = new LtqCreateCollectionRequest();
          createReq.vectors = vectorConfig;
          client.qdCreateCollection(collectionName, createReq);
        });

        it("qdGetCollection retrieves collection info", () -> {
          var collection = client.qdGetCollection(collectionName);
          assertNotNull(collection);
          assertNotNull(collection.result);
        });

        it("qdListCollections lists all collections", () -> {
          var collections = client.qdListCollections();
          assertNotNull(collections);
          assertTrue(collections.result.collections.stream().anyMatch(c -> c.name.equals(collectionName)));
        });

        describe("Point operations", () -> {
          var doc1Id = java.util.UUID.randomUUID().toString();
          var doc2Id = java.util.UUID.randomUUID().toString();

          it("qdAddDocuments adds documents to collection", () -> {
            var vector1 = new float[128];
            var vector2 = new float[128];
            for (int i = 0; i < 128; i++) {
              vector1[i] = 0.1f;
              vector2[i] = 0.2f;
            }
            var doc1 = new LtqDocument().set(doc1Id, "This is a test document", vector1, Map.of("category", "test"));
            var doc2 = new LtqDocument().set(doc2Id, "Another test document", vector2, Map.of("category", "test"));
            client.qdAddDocuments(collectionName, List.of(doc1, doc2));
          });

          it("qdSearchPoints searches for similar points", () -> {
            var searchVector = new float[128];
            for (int i = 0; i < 128; i++) {
              searchVector[i] = 0.1f;
            }
            var searchReq = new LtqSearchRequest();
            searchReq.vector = searchVector;
            searchReq.limit = 10;
            var response = client.qdSearchPoints(collectionName, searchReq);
            assertNotNull(response);
            assertNotNull(response.result);
          });

          it("qdScrollPoints scrolls through points", () -> {
            var scrollReq = new LtqScrollRequest();
            scrollReq.limit = 10;
            var response = client.qdScrollPoints(collectionName, scrollReq);
            assertNotNull(response);
            assertNotNull(response.result);
          });

          it("qdDeletePoints deletes points", () -> {
            var deleteReq = new LtqDeleteRequest();
            deleteReq.filter = client.qdBuildFilter(Map.of("category", List.of("test")));
            client.qdDeletePoints(collectionName, deleteReq);
          });
        });

        it("qdDeleteCollection deletes the collection", () -> {
          client.qdDeleteCollection(collectionName);
          try {
            client.qdGetCollection(collectionName);
            fail("Collection should have been deleted");
          } catch (IllegalStateException e) {
            // Expected
          }
        });
      });
    }

  }

}
