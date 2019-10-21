/*
 * Copyright © 2019 Cask Data, Inc.
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

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

/**
 * Config for {@link PDFExtractor}
 */
public class PDFExtractorConfig extends PluginConfig {
  public static final String PROPERTY_SOURCE_FIELD_NAME = "sourceFieldName";

  @Name("sourceFieldName")
  @Description("Specifies the input field containing the binary pdf data.")
  private final String sourceFieldName;

  @Macro
  @Description("Set to true if this plugin should ignore errors.")
  private Boolean continueOnError;

  public PDFExtractorConfig(String sourceFieldName, Boolean continueOnError) {
    this.sourceFieldName = sourceFieldName;
    this.continueOnError = continueOnError;
  }

  private PDFExtractorConfig(Builder builder) {
    this.sourceFieldName = builder.sourceFieldName;
    this.continueOnError = builder.continueOnError;
  }

  public String getSourceFieldName() {
    return sourceFieldName;
  }

  public Boolean getContinueOnError() {
    return continueOnError;
  }

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    if (inputSchema == null) {
      failureCollector.addFailure("Input schema should not be empty.", null);
      return;
    }

    Schema.Field contentField = inputSchema.getField(sourceFieldName);
    if (contentField == null) {
      failureCollector.addFailure(String.format("Field '%s' must be present in the input schema.", sourceFieldName),
                                  null).withConfigProperty(PROPERTY_SOURCE_FIELD_NAME)
        .withInputSchemaField(PROPERTY_SOURCE_FIELD_NAME);
    } else {
      Schema contentFieldSchema = contentField.getSchema();

      if (contentFieldSchema.isNullable()) {
        contentFieldSchema = contentFieldSchema.getNonNullable();
      }

      if (contentFieldSchema.getLogicalType() != null || contentFieldSchema.getType() != Schema.Type.BYTES) {
        failureCollector.addFailure(String.format("Field '%s' is of unexpected type '%s'.",
                                                  contentField.getName(), contentFieldSchema.getDisplayName()),
                                    "Supported type is bytes.")
          .withConfigProperty(PROPERTY_SOURCE_FIELD_NAME)
          .withInputSchemaField(PROPERTY_SOURCE_FIELD_NAME);
      }
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(PDFExtractorConfig copy) {
    return new Builder()
      .setSourceFieldName(copy.getSourceFieldName())
      .setContinueOnError(copy.getContinueOnError());
  }

  /**
   * Builder for {@link PDFExtractorConfig}
   */
  public static final class Builder {
    private String sourceFieldName;
    private Boolean continueOnError;

    public Builder setSourceFieldName(String sourceFieldName) {
      this.sourceFieldName = sourceFieldName;
      return this;
    }

    public Builder setContinueOnError(Boolean continueOnError) {
      this.continueOnError = continueOnError;
      return this;
    }

    private Builder() {
    }

    public PDFExtractorConfig build() {
      return new PDFExtractorConfig(this);
    }
  }
}
