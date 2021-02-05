// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cloud.spanner.pgadapter.metadata;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.List;

import static com.google.cloud.spanner.pgadapter.metadata.JSONUtils.getJSONArray;
import static com.google.cloud.spanner.pgadapter.metadata.JSONUtils.getJSONString;

/**
 * DynamicCommandMetadata is a simple POJO for extracting commands which are user-definable and
 * generated at run-time from a user-defined JSON. This class concerns with the population of those
 * JSON objects onto more accessible formats for easier internal handling.
 */
public class QueryRewritesMetadata {

  private static final String REWRITES_KEY = "rewrites";
  private static final String INPUT_KEY = "input_pattern";
  private static final String OUTPUT_KEY = "output_pattern";

  private final String inputPattern;
  private final String outputPattern;

  private QueryRewritesMetadata(JSONObject commandJSON) {
    this.inputPattern = getJSONString(commandJSON, INPUT_KEY);
    this.outputPattern = getJSONString(commandJSON, OUTPUT_KEY);
  }

  /**
   * Takes a JSON object and returns a list of metadata objects holding the desired information.
   *
   * @param jsonObject Input JSON object in the format {"rewrites": [{"input_pattern": "",
   * "output_pattern": ""}
   * @return A list of constructed metadata objects in the format understood by QueryRewritesMetadata
   */
  public static List<QueryRewritesMetadata> fromJSON(JSONObject jsonObject) {
    List<QueryRewritesMetadata> resultList = new ArrayList<>();
    for (Object currentJSONObject : getJSONArray(jsonObject, REWRITES_KEY)) {
      resultList.add(new QueryRewritesMetadata((JSONObject) currentJSONObject));
    }
    return resultList;
  }

  public String getInputPattern() {
    return this.inputPattern;
  }

  public String getOutputPattern() {
    return this.outputPattern;
  }

}
