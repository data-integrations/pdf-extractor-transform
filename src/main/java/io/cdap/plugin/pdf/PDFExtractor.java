/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

import com.google.common.annotations.VisibleForTesting;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDDocumentInformation;
import org.apache.pdfbox.pdmodel.encryption.InvalidPasswordException;
import org.apache.pdfbox.text.PDFTextStripper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extracts content and metadata from a PDF using the Apache PDFBox library.
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("PDFExtractor")
@Description("Extracts content and metadata from a PDF using the Apache PDFBox library.")
public final class PDFExtractor extends Transform<StructuredRecord, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(PDFExtractor.class);

  private final PDFExtractorConfig config;
  private static final Schema outputSchema =
    Schema.recordOf("output",
                    Schema.Field.of("raw_pdf_data", Schema.nullableOf(Schema.of(Schema.Type.BYTES))),
                    Schema.Field.of("text", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("page_count", Schema.nullableOf(Schema.of(Schema.Type.INT))),
                    Schema.Field.of("title", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("author", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("subject", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("keywords", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("creator", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("producer", Schema.nullableOf(Schema.of(Schema.Type.STRING))),
                    Schema.Field.of("creation_date", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("modification_date", Schema.nullableOf(Schema.of(Schema.Type.LONG))),
                    Schema.Field.of("trapped", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
  private PDFTextStripper strip;

  @VisibleForTesting
  public PDFExtractor(PDFExtractorConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    super.configurePipeline(pipelineConfigurer);

    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();

    pipelineConfigurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    super.initialize(context);
    strip = new PDFTextStripper();
  }

  @Override
  public void transform(StructuredRecord in, Emitter<StructuredRecord> emitter) throws Exception {
    if (in.get(config.getSourceFieldName()) != null) {
      PDDocument inputDoc = null;
      try {
        inputDoc = PDDocument.load((byte[]) in.get(config.getSourceFieldName()));
        PDDocumentInformation info = inputDoc.getDocumentInformation();
        String pdfString = strip.getText(inputDoc);
        emitter.emit(
          StructuredRecord.builder(outputSchema)
            .set("raw_pdf_data", in.get(config.getSourceFieldName()))
            .set("text", pdfString)
            .set("page_count", inputDoc.getNumberOfPages())
            .set("title", info.getTitle())
            .set("author", info.getAuthor())
            .set("subject", info.getSubject())
            .set("keywords", info.getKeywords())
            .set("creator", info.getCreator())
            .set("producer", info.getProducer())
            .set("creation_date", info.getCreationDate().getTimeInMillis())
            .set("modification_date", info.getModificationDate().getTimeInMillis())
            .set("trapped", info.getTrapped())
            .build());
      } catch (InvalidPasswordException pe) {
        if (!config.getContinueOnError()) {
          throw pe;
        } else {
          LOG.warn("Caught Invalid Password Exception. Continuing since continueOnError is true. Exception: {}", pe);
        }
      } catch (IOException io) {
        if (!config.getContinueOnError()) {
          throw io;
        } else {
          LOG.warn("Caught IOException. Continuing since continueOnError is true. Exception: {}", io);
        }
      } catch (Exception e) {
        if (!config.getContinueOnError()) {
          throw e;
        } else {
          LOG.warn("Caught {}. Continuing since continueOnError is true. Exception: {}",
                   e.getClass().getCanonicalName(),
                   e);
        }
      } finally {
        if (inputDoc != null) {
          inputDoc.close();
        }
      }
    } else {
      LOG.warn("No data found in source field.");
      if (!config.getContinueOnError()) {
        throw new RuntimeException("No data found in source field of incoming record.");
      }
    }
  }
}
