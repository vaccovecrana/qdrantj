package io.vacco.qdrant;

import java.util.Map;

public class LtqPoint {
  public String id;
  public float[] vector;
  public Map<String, Object> payload;
  public double score; // For search results
}

