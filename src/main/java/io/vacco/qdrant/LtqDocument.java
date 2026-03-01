package io.vacco.qdrant;

import java.util.Map;

public class LtqDocument {

  public String id;
  public String content;
  public float[] embedding;
  public Map<String, Object> metadata;

  public LtqDocument set(String id, String content, float[] embedding, Map<String, Object> metadata) {
    this.id = id;
    this.content = content;
    this.embedding = embedding;
    this.metadata = metadata;
    return this;
  }

}
