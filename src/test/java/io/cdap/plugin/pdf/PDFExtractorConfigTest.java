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

import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.validation.CauseAttributes;
import io.cdap.cdap.etl.api.validation.ValidationFailure;
import io.cdap.cdap.etl.mock.validation.MockFailureCollector;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

public class PDFExtractorConfigTest {
  private static final String MOCK_STAGE = "mockStage";
  private static final Schema VALID_SCHEMA =
    Schema.recordOf("schema",
                    Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.BYTES))));

  private static final PDFExtractorConfig VALID_CONFIG =
    new PDFExtractorConfig("body", false);

  @Test
  public void testValidConfig() {
    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, VALID_SCHEMA);
    Assert.assertTrue(failureCollector.getValidationFailures().isEmpty());
  }

  @Test
  public void testContentFieldDoesNotExists() {
    Schema schema = Schema.recordOf("schema",
                                    Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    VALID_CONFIG.validate(failureCollector, schema);
    assertValidationFailed(failureCollector, PDFExtractorConfig.PROPERTY_SOURCE_FIELD_NAME);
  }

  @Test
  public void testContentFieldOfWrongType() {
    PDFExtractorConfig config = PDFExtractorConfig.builder(VALID_CONFIG)
      .setSourceFieldName("nonExisting")
      .build();

    MockFailureCollector failureCollector = new MockFailureCollector(MOCK_STAGE);
    config.validate(failureCollector, VALID_SCHEMA);
    assertValidationFailed(failureCollector, PDFExtractorConfig.PROPERTY_SOURCE_FIELD_NAME);
  }

  private static void assertValidationFailed(MockFailureCollector failureCollector, String paramName) {
    List<ValidationFailure> failureList = failureCollector.getValidationFailures();

    Assert.assertEquals(1, failureList.size());
    ValidationFailure failure = failureList.get(0);
    List<ValidationFailure.Cause> causeList = getCauses(failure, CauseAttributes.STAGE_CONFIG);
    Assert.assertEquals(1, causeList.size());
    ValidationFailure.Cause cause = causeList.get(0);
    Assert.assertEquals(paramName, cause.getAttribute(CauseAttributes.STAGE_CONFIG));
  }

  @Nonnull
  private static List<ValidationFailure.Cause> getCauses(ValidationFailure failure, String stacktrace) {
    return failure.getCauses()
      .stream()
      .filter(cause -> cause.getAttribute(stacktrace) != null)
      .collect(Collectors.toList());
  }
}
