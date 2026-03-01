package io.vacco.qdrant;

public class LtqSearchRequest {
  public float[] vector;
  public int limit;
  public boolean with_payload;
  public boolean with_vector;
  public LtqFilter filter;
}

