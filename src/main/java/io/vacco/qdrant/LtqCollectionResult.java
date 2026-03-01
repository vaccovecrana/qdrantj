package io.vacco.qdrant;

import java.util.List;

public class LtqCollectionResult {
  public LtqCollectionMetadata metadata;
  public int points_count;
  public List<LtqCollectionInfo> collections;
  public List<LtqPoint> points;
  public List<LtqPoint> result; // For scroll API - can be points array directly
  public Object next_page_offset; // For scroll API pagination - can be null, integer, or string
}

