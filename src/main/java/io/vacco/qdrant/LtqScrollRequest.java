package io.vacco.qdrant;

public class LtqScrollRequest {
  public int limit;
  public boolean with_payload;
  public boolean with_vector;
  public Object offset; // Can be null, integer, or string (point ID) for pagination
  public LtqFilter filter; // Optional filter for scrolling
}

