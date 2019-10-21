/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.pdf;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.mock.common.MockEmitter;
import io.cdap.cdap.etl.mock.common.MockPipelineConfigurer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.net.URL;

/**
 * Tests {@link PDFExtractor}.
 */
public class PDFExtractorTest {
  private static final Schema INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
  private static final Schema INVALID_INPUT = Schema.recordOf("input",
                                                      Schema.Field.of("a",
                                                                      Schema.arrayOf(Schema.of(Schema.Type.STRING))));

  // These are some arbitrary PDF files obtained from data.gov for testing
  private static String[] pdfFiles = new String[] {
    "2015_TIGER_GDB_Record_Layouts.pdf",
    "fedreqs.pdf",
    "2012_TIGERLine_Shapefiles_File_Name_Definitions.pdf",
    "PLSS-CadNSDI-Data-Set-Availability.pdf",
    "62641.pdf",
    "crc2013.pdf",
    "pdf-sample.pdf"
  };

  @Test
  public void testReadingPDFFiles() throws Exception {
    PDFExtractorConfig config = new PDFExtractorConfig("body", true);
    Transform<StructuredRecord, StructuredRecord> transform = new PDFExtractor(config);
    transform.initialize(null);
    ClassLoader classLoader = getClass().getClassLoader();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    for (String fileName : pdfFiles) {
      URL gzippedFile = classLoader.getResource(fileName);
      Path source = new Path(gzippedFile.getPath());
      FileSystem fileSystem = source.getFileSystem(new Configuration());
      byte[] pdfFileData = new byte[(int) fileSystem.getFileStatus(source).getLen()];
      try (BufferedInputStream input = new BufferedInputStream(fileSystem.open(source))) {
        input.read(pdfFileData);
      }
      transform.transform(StructuredRecord.builder(INPUT).set("body", pdfFileData).build(), emitter);
    }
    Assert.assertEquals(pdfFiles.length, emitter.getEmitted().size());
    Assert.assertEquals(12, emitter.getEmitted().get(0).getSchema().getFields().size());
    for (int i = 0; i < pdfFiles.length; i++) {
      Assert.assertNotNull(emitter.getEmitted().get(i).get("page_count"));
      Assert.assertNotNull(emitter.getEmitted().get(i).get("raw_pdf_data"));
      Assert.assertNotNull(emitter.getEmitted().get(i).get("modification_date"));
      Assert.assertNotNull(emitter.getEmitted().get(i).get("creation_date"));
    }
  }


  @Test(expected = IllegalArgumentException.class)
  public void testFieldNotInInputSchema() throws Exception {
    PDFExtractorConfig config = new PDFExtractorConfig("body", true);
    Transform<StructuredRecord, StructuredRecord> transform = new PDFExtractor(config);
    transform.configurePipeline(new MockPipelineConfigurer(INVALID_INPUT));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testInputTypeNotBytesSchema() throws Exception {
    PDFExtractorConfig config = new PDFExtractorConfig("a", true);
    Transform<StructuredRecord, StructuredRecord> transform = new PDFExtractor(config);
    transform.configurePipeline(new MockPipelineConfigurer(INVALID_INPUT));
  }

  @Test(expected = InvalidPasswordException.class)
  public void testPasswordProtectedFiles() throws Exception {
    PDFExtractorConfig config = new PDFExtractorConfig("body", false);
    Transform<StructuredRecord, StructuredRecord> transform = new PDFExtractor(config);
    transform.initialize(null);
    ClassLoader classLoader = getClass().getClassLoader();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    URL gzippedFile = classLoader.getResource("pdf-sample-encrypted.pdf");
    Path source = new Path(gzippedFile.getPath());
    FileSystem fileSystem = source.getFileSystem(new Configuration());
    byte[] pdfFileData = new byte[(int) fileSystem.getFileStatus(source).getLen()];
    try (BufferedInputStream input = new BufferedInputStream(fileSystem.open(source))) {
      input.read(pdfFileData);
    }
    transform.transform(StructuredRecord.builder(INPUT).set("body", pdfFileData).build(), emitter);
  }

  @Test
  public void testIgnoreErrors() throws Exception {
    PDFExtractorConfig config = new PDFExtractorConfig("body", true);
    Transform<StructuredRecord, StructuredRecord> transform = new PDFExtractor(config);
    transform.initialize(null);
    ClassLoader classLoader = getClass().getClassLoader();
    MockEmitter<StructuredRecord> emitter = new MockEmitter<>();

    URL gzippedFile = classLoader.getResource("pdf-sample-encrypted.pdf");
    Path source = new Path(gzippedFile.getPath());
    FileSystem fileSystem = source.getFileSystem(new Configuration());
    byte[] pdfFileData = new byte[(int) fileSystem.getFileStatus(source).getLen()];
    try (BufferedInputStream input = new BufferedInputStream(fileSystem.open(source))) {
      input.read(pdfFileData);
    }
    transform.transform(StructuredRecord.builder(INPUT).set("body", pdfFileData).build(), emitter);
    Assert.assertEquals(0, emitter.getEmitted().size());
  }
}
