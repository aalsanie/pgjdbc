/*
 * Copyright (c) 2026, PostgreSQL Global Development Group
 * See the LICENSE file in the project root for more information.
 */

package org.postgresql.test.jdbc2;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.postgresql.PGConnection;
import org.postgresql.PGProperty;
import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;
import org.postgresql.test.util.CountingSocketFactory;
import org.postgresql.util.PSQLState;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

class BlobStreamTest extends BaseTest4 {
  private static final byte[] CONTENTS = {10, 20, 30, 40, 50};
  private final CountingSocketFactory.Counters counters = CountingSocketFactory.register();

  @Override
  protected void updateProperties(Properties props) {
    super.updateProperties(props);
    PGProperty.AUTOSAVE.set(props, "never");
    PGProperty.SOCKET_FACTORY.set(props, CountingSocketFactory.class.getName());
    PGProperty.SOCKET_FACTORY_ARG.set(props, counters.key());
  }

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    con.setAutoCommit(false);
  }

  @Override
  protected void tearDown() throws SQLException {
    try {
      if (con != null) {
        con.rollback();
      }
    } finally {
      try {
        super.tearDown();
      } finally {
        CountingSocketFactory.unregister(counters);
      }
    }
  }

  @Test
  void openingStreamUsesOneRoundtrip() throws Exception {
    Blob blob = createBlob(CONTENTS);
    try {
      // Move the parent descriptor before measuring stream opening.
      assertArrayEquals(new byte[]{30}, blob.getBytes(3, 1));
      long before = counters.roundtrips.get();
      try (InputStream stream = blob.getBinaryStream()) {
        assertEquals(1, counters.roundtrips.get() - before,
            "Opening a full Blob stream should only open its independent handle");
        assertContents(stream, CONTENTS);
      }
    } finally {
      blob.free();
    }
  }

  @Test
  void streamsStartAtZeroAfterParentReads() throws Exception {
    Blob blob = createBlob(CONTENTS);
    try {
      assertArrayEquals(new byte[]{40, 50}, blob.getBytes(4, 2));
      try (InputStream first = blob.getBinaryStream();
           InputStream second = blob.getBinaryStream()) {
        assertEquals(10, first.read());
        assertArrayEquals(new byte[]{20}, blob.getBytes(2, 1));
        assertContents(second, CONTENTS);
        assertContents(first, Arrays.copyOfRange(CONTENTS, 1, CONTENTS.length));
      }
      try (InputStream reopened = blob.getBinaryStream()) {
        assertContents(reopened, CONTENTS);
      }
    } finally {
      blob.free();
    }
  }

  @Test
  void emptyBlobReportsEndOfStream() throws Exception {
    Blob blob = createBlob(new byte[0]);
    try (InputStream stream = blob.getBinaryStream()) {
      assertEquals(-1, stream.read());
    } finally {
      blob.free();
    }
  }

  @Test
  void freeingBlobClosesItsStreamAndRejectsNewStreams() throws Exception {
    Blob blob = createBlob(CONTENTS);
    try (InputStream stream = blob.getBinaryStream()) {
      blob.free();
      SQLException error = assertThrows(SQLException.class, blob::getBinaryStream);
      assertEquals(PSQLState.OBJECT_NOT_IN_STATE.getState(), error.getSQLState());
      assertThrows(IOException.class, stream::read);
    } finally {
      blob.free();
    }
  }

  private Blob createBlob(byte[] contents) throws SQLException {
    LargeObjectManager manager = con.unwrap(PGConnection.class).getLargeObjectAPI();
    long oid = manager.createLO();
    try (LargeObject object = manager.open(oid)) {
      object.write(contents);
    }
    try (PreparedStatement statement = con.prepareStatement("SELECT ?::oid")) {
      statement.setLong(1, oid);
      try (ResultSet result = statement.executeQuery()) {
        assertTrue(result.next());
        return result.getBlob(1);
      }
    }
  }

  private static void assertContents(InputStream stream, byte[] expected) throws IOException {
    for (byte value : expected) {
      assertEquals(value & 0xff, stream.read());
    }
    assertEquals(-1, stream.read());
  }
}
