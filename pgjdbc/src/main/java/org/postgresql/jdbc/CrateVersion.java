/*
 * Copyright (c) 2016, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.jdbc;

public class CrateVersion implements Comparable<String> {

  private final String version;

  public CrateVersion(String version) {
    this.version = version;
  }

  @Override
  public int compareTo(String o) {
    String[] v1 = version.split("\\.");
    String[] v2 = o.split("\\.");
    int i = 0;
    while (i < v1.length && i < v2.length && v1[i].equals(v2[i])) {
      i++;
    }
    if (i < v1.length && i < v2.length) {
      int diff = Integer.valueOf(v1[i]).compareTo(Integer.valueOf(v2[i]));
      return Integer.signum(diff);
    }
    return Integer.signum(v1.length - v2.length);
  }

  public boolean before(String version) {
    return this.compareTo(version) < 0;
  }

  public boolean after(String version) {
    return this.compareTo(version) > 0;
  }
}
